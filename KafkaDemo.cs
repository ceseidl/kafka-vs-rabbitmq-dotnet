using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace MensageriaDemo;

public static partial class KafkaDemo
{
    private const string Bootstrap = "localhost:9092";

    public static async Task RunAsync(CancellationToken ct)
    {
        // EN: Fresh topic on every run: no dependence on old
        // offsets.
        // PT: Tópico novo a cada execução: sem depender de
        // offsets antigos.
        var topic = $"orders-{Guid.NewGuid():N}"[..14];
        await CreateTopicAsync(topic);

        var orders = Wire.Sample();
        await ProduceAsync(topic, orders, ct);

        // EN: Two groups read the SAME events, each at its own
        // pace.
        // PT: Dois grupos leem os MESMOS eventos, cada um no seu
        // ritmo.
        Consume("billing", topic, orders.Count, ct);
        Consume("analytics", topic, orders.Count, ct);
    }

    private static async Task CreateTopicAsync(string topic)
    {
        using var admin = new AdminClientBuilder(
            new AdminClientConfig
            {
                BootstrapServers = Bootstrap
            }).Build();

        await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 3,
                ReplicationFactor = 1
            }
        ]);
    }
}
