using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MensageriaDemo;

public static partial class RabbitMqDemo
{
    private const string Exchange = "orders";
    private const string RoutingKey = "order.placed";
    private const string Process = "orders.process";
    private const string Audit = "orders.audit";
    private const string Dead = "orders.dead";

    private static async Task DeclareTopologyAsync(
        IChannel channel, CancellationToken ct)
    {
        await channel.ExchangeDeclareAsync(
            Exchange, ExchangeType.Direct,
            durable: true, cancellationToken: ct);
        await channel.ExchangeDeclareAsync(
            "orders.dlx", ExchangeType.Fanout,
            durable: true, cancellationToken: ct);

        // EN: Main queue: quorum queue, with dead-letter for
        // rejected messages.
        // PT: Fila principal: quorum queue, com dead-letter
        // para mensagens rejeitadas.
        var args = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum",
            ["x-dead-letter-exchange"] = "orders.dlx",
        };
        await channel.QueueDeclareAsync(
            Process, durable: true, exclusive: false,
            autoDelete: false, args, cancellationToken: ct);
        await channel.QueueDeclareAsync(
            Audit, durable: true, exclusive: false,
            autoDelete: false, cancellationToken: ct);
        await channel.QueueDeclareAsync(
            Dead, durable: true, exclusive: false,
            autoDelete: false, cancellationToken: ct);

        // EN: Two queues bound to the same key: each one
        // receives a copy of the message.
        // PT: Duas filas ligadas à mesma chave: cada uma
        // recebe uma cópia da mensagem.
        await channel.QueueBindAsync(
            Process, Exchange, RoutingKey, cancellationToken: ct);
        await channel.QueueBindAsync(
            Audit, Exchange, RoutingKey, cancellationToken: ct);
        await channel.QueueBindAsync(
            Dead, "orders.dlx", "", cancellationToken: ct);

        foreach (var queue in new[] { Process, Audit, Dead })
            // EN: Repeatable demo.
            // PT: Demo repetível.
            await channel.QueuePurgeAsync(queue, ct);
    }

    private static async Task<uint> Count(
        IChannel channel, string queue, CancellationToken ct) =>
        (await channel.QueueDeclarePassiveAsync(queue, ct))
            .MessageCount;
}
