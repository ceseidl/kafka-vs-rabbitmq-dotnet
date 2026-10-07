using Confluent.Kafka;

namespace MensageriaDemo;

public static partial class KafkaDemo
{
    private static async Task ProduceAsync(
        string topic,
        IReadOnlyList<OrderPlaced> orders,
        CancellationToken ct)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = Bootstrap,
            // EN: Waits for the in-sync replicas.
            // PT: Espera as réplicas em sincronia.
            Acks = Acks.All,
            // EN: Retries without duplicating.
            // PT: Retry sem duplicar.
            EnableIdempotence = true,
        };
        using var producer =
            new ProducerBuilder<string, byte[]>(config).Build();

        foreach (var order in orders)
        {
            // EN: The key defines the partition: same customer
            // -> same partition -> order preserved.
            // PT: A chave define a partição: mesmo cliente ->
            // mesma partição -> ordem preservada.
            var message = new Message<string, byte[]>
            {
                Key = order.CustomerId,
                Value = Wire.Serialize(order),
            };
            var result =
                await producer.ProduceAsync(topic, message, ct);

            Console.WriteLine(
                $"produced / produziu {order.CustomerId} -> "
                + "partition / partição "
                + $"{result.Partition.Value}, "
                + $"offset {result.Offset.Value}");
        }
    }
}
