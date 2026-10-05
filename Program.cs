using MensageriaDemo;

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

switch (args.FirstOrDefault())
{
    case "rabbit": await RabbitMqDemo.RunAsync(cts.Token); break;
    case "kafka":  await KafkaDemo.RunAsync(cts.Token);    break;
    default: Console.WriteLine("uso: dotnet run -- rabbit|kafka"); break;
}
