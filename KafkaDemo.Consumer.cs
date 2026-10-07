using Confluent.Kafka;

namespace MensageriaDemo;

public static partial class KafkaDemo
{
    private static void Consume(
        string group, string topic, int expected,
        CancellationToken ct)
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
        using var consumer =
            new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(topic);

        // EN: The first poll on a fresh broker can wait for the
        // group join and the offsets topic, so keep polling
        // until a deadline instead of giving up on the first
        // empty poll.
        // PT: O primeiro poll em um broker novo pode esperar o
        // group join e o tópico de offsets; por isso continua
        // tentando até um prazo, em vez de desistir no primeiro
        // poll vazio.
        var deadline = DateTime.UtcNow.AddSeconds(25);
        var seen = 0;
        while (seen < expected
               && DateTime.UtcNow < deadline
               && !ct.IsCancellationRequested)
        {
            var result =
                consumer.Consume(TimeSpan.FromSeconds(2));
            if (result is null) continue;

            var order = Wire.Deserialize(result.Message.Value);
            Console.WriteLine(
                $"[{group}] {order.CustomerId} R$ {order.Total} "
                + "(partition / partição "
                + $"{result.Partition.Value}, "
                + $"offset {result.Offset.Value})");

            // EN: At-least-once: confirm after the side effect.
            // PT: At-least-once: confirma depois do efeito.
            consumer.Commit(result);
            seen++;
        }

        Console.WriteLine(
            $"[{group}] read {seen} of {expected} events / "
            + $"leu {seen} de {expected} eventos");
        consumer.Close();
    }
}
