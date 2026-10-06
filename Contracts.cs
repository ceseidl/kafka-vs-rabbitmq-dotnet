using System.Text.Json;

namespace MensageriaDemo;

public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total);

public static class Wire
{
    public static byte[] Serialize(OrderPlaced order) =>
        JsonSerializer.SerializeToUtf8Bytes(order);

    public static OrderPlaced Deserialize(ReadOnlySpan<byte> body) =>
        JsonSerializer.Deserialize<OrderPlaced>(body)
        ?? throw new InvalidOperationException("Empty message / Mensagem vazia");

    // EN: Sample orders: two customers, plus one invalid order (Total <= 0).
    // PT: Pedidos de exemplo: dois clientes, e um pedido inválido (Total <= 0).
    public static IReadOnlyList<OrderPlaced> Sample() =>
    [
        new(Guid.NewGuid(), "cli-1", 100m),
        new(Guid.NewGuid(), "cli-2", 250m),
        new(Guid.NewGuid(), "cli-1", 0m),
        new(Guid.NewGuid(), "cli-2", 80m),
        new(Guid.NewGuid(), "cli-1", 40m),
        new(Guid.NewGuid(), "cli-2", 15m),
    ];
}
