using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace MensageriaDemo;

public static class KafkaDemo
{
    private const string Bootstrap = "localhost:9092";

    public static async Task RunAsync(CancellationToken ct)
    {
        // EN: Fresh topic on every run: the demo does not depend on old offsets.
        // PT: Tópico novo a cada execução: a demo não depende de offsets antigos.
        var topic = $"orders-{Guid.NewGuid():N}"[..14];
        await CreateTopicAsync(topic);

        var orders = Wire.Sample();
        await ProduceAsync(topic, orders, ct);

        // EN: Two groups read the SAME events, each at its own pace.
        // PT: Dois grupos leem os MESMOS eventos, cada um no seu ritmo.
        Consume("billing", topic, orders.Count, ct);
        Consume("analytics", topic, orders.Count, ct);
    }

    private static async Task CreateTopicAsync(string topic)
    {
        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = Bootstrap }).Build();

        await admin.CreateTopicsAsync(
        [
            new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }
        ]);
    }

    private static async Task ProduceAsync(
        string topic, IReadOnlyList<OrderPlaced> orders, CancellationToken ct)
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
        using var producer = new ProducerBuilder<string, byte[]>(config).Build();

        foreach (var order in orders)
        {
            // EN: The key defines the partition: same customer -> same partition -> order preserved.
            // PT: A chave define a partição: mesmo cliente -> mesma partição -> ordem preservada.
            var message = new Message<string, byte[]>
            {
                Key = order.CustomerId,
                Value = Wire.Serialize(order),
            };
            var result = await producer.ProduceAsync(topic, message, ct);

            Console.WriteLine($"produced / produziu {order.CustomerId} -> partition / partição {result.Partition.Value}, " +
                              $"offset {result.Offset.Value}");
        }
    }

    private static void Consume(string group, string topic, int expected, CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = Bootstrap,
            GroupId = group,
            // EN: A new group reads from the beginning.
            // PT: Grupo novo lê desde o início.
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // EN: Commit only after processing.
            // PT: Commit só após processar.
            EnableAutoCommit = false,
        };
        using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(topic);

        var seen = 0;
        while (seen < expected && !ct.IsCancellationRequested)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(10));
            if (result is null) break;

            var order = Wire.Deserialize(result.Message.Value);
            Console.WriteLine($"[{group}] {order.CustomerId} R$ {order.Total} " +
                              $"(partition / partição {result.Partition.Value}, offset {result.Offset.Value})");

            // EN: At-least-once: confirm after the side effect.
            // PT: At-least-once: confirma depois do efeito.
            consumer.Commit(result);
            seen++;
        }

        Console.WriteLine($"[{group}] read {seen} of {expected} events / leu {seen} de {expected} eventos");
        consumer.Close();
    }
}
