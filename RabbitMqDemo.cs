using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MensageriaDemo;

public static class RabbitMqDemo
{
    private const string Exchange = "orders";
    private const string RoutingKey = "order.placed";
    private const string Process = "orders.process";
    private const string Audit = "orders.audit";
    private const string Dead = "orders.dead";

    private static async Task DeclareTopologyAsync(IChannel channel, CancellationToken ct)
    {
        await channel.ExchangeDeclareAsync(
            Exchange, ExchangeType.Direct, durable: true, cancellationToken: ct);
        await channel.ExchangeDeclareAsync(
            "orders.dlx", ExchangeType.Fanout, durable: true, cancellationToken: ct);

        // Fila principal: quorum queue, com dead-letter para mensagens rejeitadas
        var args = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum",
            ["x-dead-letter-exchange"] = "orders.dlx",
        };
        await channel.QueueDeclareAsync(
            Process, durable: true, exclusive: false, autoDelete: false, args, cancellationToken: ct);
        await channel.QueueDeclareAsync(
            Audit, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
        await channel.QueueDeclareAsync(
            Dead, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);

        // Duas filas ligadas à mesma chave: cada uma recebe uma cópia da mensagem
        await channel.QueueBindAsync(Process, Exchange, RoutingKey, cancellationToken: ct);
        await channel.QueueBindAsync(Audit, Exchange, RoutingKey, cancellationToken: ct);
        await channel.QueueBindAsync(Dead, "orders.dlx", "", cancellationToken: ct);

        foreach (var queue in new[] { Process, Audit, Dead })
            await channel.QueuePurgeAsync(queue, ct); // demo repetível
    }

    private static async Task<uint> Count(IChannel channel, string queue, CancellationToken ct) =>
        (await channel.QueueDeclarePassiveAsync(queue, ct)).MessageCount;

    public static async Task RunAsync(CancellationToken ct)
    {
        var factory = new ConnectionFactory { HostName = "localhost" }; // guest/guest
        await using var connection = await factory.CreateConnectionAsync(ct);

        // Confirmações do broker: o publish só retorna depois que o broker aceitou a mensagem
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);
        await using var channel = await connection.CreateChannelAsync(options, ct);

        await DeclareTopologyAsync(channel, ct);

        var orders = Wire.Sample();
        var handled = 0;
        var done = new TaskCompletionSource();

        // Prefetch 1: cada worker recebe uma mensagem por vez (distribuição justa)
        await channel.BasicQosAsync(0, prefetchCount: 1, global: false, ct);

        async Task StartWorker(string name)
        {
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, ea) =>
            {
                try
                {
                    var order = Wire.Deserialize(ea.Body.Span);
                    if (order.Total <= 0)
                        throw new InvalidOperationException("Total inválido");

                    Console.WriteLine($"[{name}] processou {order.CustomerId} R$ {order.Total}");
                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{name}] falhou ({ex.Message}) -> dead-letter");
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                }
                finally
                {
                    if (Interlocked.Increment(ref handled) == orders.Count)
                        done.TrySetResult();
                }
            };

            // autoAck: false -> a mensagem só sai da fila após o BasicAck
            await channel.BasicConsumeAsync(Process, autoAck: false, consumer, ct);
        }

        await StartWorker("worker-1");
        await StartWorker("worker-2");

        foreach (var order in orders)
        {
            var props = new BasicProperties
            {
                Persistent = true,
                MessageId = order.OrderId.ToString(),
            };
            await channel.BasicPublishAsync(
                Exchange, RoutingKey, mandatory: true, props, Wire.Serialize(order), ct);
        }

        await done.Task.WaitAsync(ct);
        await Task.Delay(500, ct); // deixa o broker atualizar os contadores

        Console.WriteLine($"orders.process (após consumo): {await Count(channel, Process, ct)} msgs");
        Console.WriteLine($"orders.audit (cópia sem consumidor): {await Count(channel, Audit, ct)} msgs");
        Console.WriteLine($"orders.dead (dead-letter): {await Count(channel, Dead, ct)} msgs");
    }
}
