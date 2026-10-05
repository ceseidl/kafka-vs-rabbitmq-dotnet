using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace MensageriaDemo;

public static class KafkaDemo
{
    private const string Bootstrap = "localhost:9092";

    public static async Task RunAsync(CancellationToken ct)
    {
        // Tópico novo a cada execução: a demo não depende de offsets antigos
        var topic = $"orders-{Guid.NewGuid():N}"[..14];
        await CreateTopicAsync(topic);

        var orders = Wire.Sample();
        await ProduceAsync(topic, orders, ct);

        // Dois grupos leem os MESMOS eventos, cada um no seu ritmo
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
            Acks = Acks.All,           // espera as réplicas em sincronia
            EnableIdempotence = true,  // retry sem duplicar
        };
        using var producer = new ProducerBuilder<string, byte[]>(config).Build();

        foreach (var order in orders)
        {
            // A chave define a partição: mesmo cliente -> mesma partição -> ordem preservada
            var message = new Message<string, byte[]>
            {
                Key = order.CustomerId,
                Value = Wire.Serialize(order),
            };
            var result = await producer.ProduceAsync(topic, message, ct);

            Console.WriteLine($"produziu {order.CustomerId} -> partição {result.Partition.Value}, " +
                              $"offset {result.Offset.Value}");
        }
    }

    private static void Consume(string group, string topic, int expected, CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = Bootstrap,
            GroupId = group,
            AutoOffsetReset = AutoOffsetReset.Earliest, // grupo novo lê desde o início
            EnableAutoCommit = false,                   // commit só após processar
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
                              $"(partição {result.Partition.Value}, offset {result.Offset.Value})");

            consumer.Commit(result); // at-least-once: confirma depois do efeito
            seen++;
        }

        Console.WriteLine($"[{group}] leu {seen} de {expected} eventos");
        consumer.Close();
    }
}
