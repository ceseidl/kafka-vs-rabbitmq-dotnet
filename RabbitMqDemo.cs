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

        // EN: Main queue: quorum queue, with dead-letter for rejected messages.
        // PT: Fila principal: quorum queue, com dead-letter para mensagens rejeitadas.
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

        // EN: Two queues bound to the same key: each one receives a copy of the message.
        // PT: Duas filas ligadas à mesma chave: cada uma recebe uma cópia da mensagem.
        await channel.QueueBindAsync(Process, Exchange, RoutingKey, cancellationToken: ct);
        await channel.QueueBindAsync(Audit, Exchange, RoutingKey, cancellationToken: ct);
        await channel.QueueBindAsync(Dead, "orders.dlx", "", cancellationToken: ct);

        foreach (var queue in new[] { Process, Audit, Dead })
            // EN: Repeatable demo.
            // PT: Demo repetível.
            await channel.QueuePurgeAsync(queue, ct);
    }

    private static async Task<uint> Count(IChannel channel, string queue, CancellationToken ct) =>
        (await channel.QueueDeclarePassiveAsync(queue, ct)).MessageCount;

    public static async Task RunAsync(CancellationToken ct)
    {
        // EN: Default credentials: guest/guest.
        // PT: Credenciais padrão: guest/guest.
        var factory = new ConnectionFactory { HostName = "localhost" };
        await using var connection = await factory.CreateConnectionAsync(ct);

        // EN: Publisher confirms: publish only returns after the broker has accepted the message.
        // PT: Confirmações do broker: o publish só retorna depois que o broker aceitou a mensagem.
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);
        await using var channel = await connection.CreateChannelAsync(options, ct);

        await DeclareTopologyAsync(channel, ct);

        var orders = Wire.Sample();
        var handled = 0;
        var done = new TaskCompletionSource();

        // EN: Prefetch 1: each worker gets one message at a time (fair dispatch).
        // PT: Prefetch 1: cada worker recebe uma mensagem por vez (distribuição justa).
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
                        throw new InvalidOperationException("Invalid total / Total inválido");

                    Console.WriteLine($"[{name}] processed / processou {order.CustomerId} R$ {order.Total}");
                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{name}] failed / falhou ({ex.Message}) -> dead-letter");
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                }
                finally
                {
                    if (Interlocked.Increment(ref handled) == orders.Count)
                        done.TrySetResult();
                }
            };

            // EN: autoAck: false -> the message only leaves the queue after BasicAck.
            // PT: autoAck: false -> a mensagem só sai da fila após o BasicAck.
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
        // EN: Lets the broker update its counters.
        // PT: Deixa o broker atualizar os contadores.
        await Task.Delay(500, ct);

        Console.WriteLine($"orders.process (after consumption / após consumo): {await Count(channel, Process, ct)} msgs");
        Console.WriteLine($"orders.audit (copy without consumer / cópia sem consumidor): {await Count(channel, Audit, ct)} msgs");
        Console.WriteLine($"orders.dead (dead-letter): {await Count(channel, Dead, ct)} msgs");
    }
}
