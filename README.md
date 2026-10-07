English | [Português](README.pt-BR.md)

[![CI](https://github.com/ceseidl/kafka-vs-rabbitmq-dotnet/actions/workflows/ci.yml/badge.svg)](https://github.com/ceseidl/kafka-vs-rabbitmq-dotnet/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

# Kafka vs RabbitMQ in .NET

> **Quick start**

```bash
docker compose up -d
dotnet run -- rabbit
dotnet run -- kafka
```

Needs the .NET 10 SDK and Docker. Details in [Running](#running).

Two small, runnable demos that put **RabbitMQ** (`RabbitMQ.Client` 7.2.2) and **Apache Kafka** (`Confluent.Kafka` 2.15.1) side by side on .NET 10. Both move the same `OrderPlaced` event (order id, customer id, total), so the differences in model and guarantees are easy to compare.

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Docker](https://www.docker.com/) (to run the brokers)

## Running

Start the brokers (RabbitMQ 4 with the management UI, and Kafka 4.0 in KRaft mode, no ZooKeeper):

```bash
docker compose up -d
```

- RabbitMQ: AMQP on `localhost:5672`, management UI on <http://localhost:15672> (`guest` / `guest`)
- Kafka: `localhost:9092`

The ports are bound to the loopback (`127.0.0.1` and `[::1]`) only, so the brokers (RabbitMQ uses `guest` / `guest`) are not reachable from other machines. `docker compose up -d --wait` also waits for the health checks.

Then run one of the demos (each one has a 60-second timeout):

```bash
dotnet run -- rabbit
dotnet run -- kafka
```

Running without an argument prints the usage line.

## Example output

RabbitMQ (`dotnet run -- rabbit`):

```
[worker-1] processed / processou cli-1 R$ 100
[worker-2] processed / processou cli-2 R$ 250
[worker-1] failed / falhou (Invalid total / Total inválido)
[worker-2] processed / processou cli-2 R$ 80
[worker-1] processed / processou cli-1 R$ 40
[worker-2] processed / processou cli-2 R$ 15
orders.process (after consumption / após consumo): 0 msgs
orders.audit (copy, no consumer / cópia sem consumidor): 6 msgs
orders.dead (dead-letter): 1 msgs
```

Kafka (`dotnet run -- kafka`):

```
produced / produziu cli-1 -> partition / partição 2, offset 0
produced / produziu cli-2 -> partition / partição 1, offset 0
produced / produziu cli-1 -> partition / partição 2, offset 1
produced / produziu cli-2 -> partition / partição 1, offset 1
produced / produziu cli-1 -> partition / partição 2, offset 2
produced / produziu cli-2 -> partition / partição 1, offset 2
[billing] cli-1 R$ 100 (partition / partição 2, offset 0)
[billing] cli-1 R$ 0 (partition / partição 2, offset 1)
[billing] cli-1 R$ 40 (partition / partição 2, offset 2)
[billing] cli-2 R$ 250 (partition / partição 1, offset 0)
[billing] cli-2 R$ 80 (partition / partição 1, offset 1)
[billing] cli-2 R$ 15 (partition / partição 1, offset 2)
[billing] read 6 of 6 events / leu 6 de 6 eventos
[analytics] cli-1 R$ 100 (partition / partição 2, offset 0)
[analytics] cli-1 R$ 0 (partition / partição 2, offset 1)
[analytics] cli-1 R$ 40 (partition / partição 2, offset 2)
[analytics] cli-2 R$ 250 (partition / partição 1, offset 0)
[analytics] cli-2 R$ 80 (partition / partição 1, offset 1)
[analytics] cli-2 R$ 15 (partition / partição 1, offset 2)
[analytics] read 6 of 6 events / leu 6 de 6 eventos
```

Which worker handles which order, and which partition each customer lands on, may differ between runs. The Kafka producer may also log an `rdkafka` idempotence warning while the broker is still starting.

## What each demo shows

Both demos publish six sample orders for two customers (`cli-1`, `cli-2`). One of them is invalid (total of `0`).

### RabbitMQ (`RabbitMqDemo.cs`, `RabbitMqDemo.Topology.cs`)

- A durable **direct exchange** (`orders`) routing the key `order.placed`.
- A **quorum queue** (`orders.process`) for the main work.
- A **dead-letter exchange** (`orders.dlx`, fanout) feeding `orders.dead`: the invalid order is rejected with `requeue: false` and lands there.
- **Publisher confirms** enabled, and persistent messages with a `MessageId`.
- **Manual ack** (`autoAck: false`): a message only leaves the queue after `BasicAck`.
- **Prefetch 1**, with two workers (`worker-1`, `worker-2`) sharing the queue.
- **Fan-out through two queues**: `orders.process` and `orders.audit` are bound with the same key, so each receives its own copy. Nobody consumes `orders.audit`, and at the end the demo prints the message count of each queue.

### Kafka (`KafkaDemo*.cs`)

- A topic created on every run (fresh name, so old offsets never matter) with **3 partitions**.
- The **customer id as message key**: the same customer always goes to the same partition, so its order is preserved. The producer prints the partition and offset of each message.
- A producer with **`acks=all` and idempotence** enabled (retries without duplicates).
- **Two consumer groups** (`billing` and `analytics`) reading the same events, each at its own pace.
- **Manual commit** after processing (`EnableAutoCommit = false`), which gives **at-least-once** delivery.

## Project structure

```
.
├── MensageriaDemo.csproj   # .NET 10 console app (RabbitMQ.Client, Confluent.Kafka)
├── MensageriaDemo.slnx     # solution file
├── docker-compose.yml      # RabbitMQ and Kafka (KRaft)
├── Program.cs              # entry point: dotnet run -- rabbit|kafka
├── Contracts.cs            # OrderPlaced record, JSON serialization, sample orders
├── RabbitMqDemo.Topology.cs # RabbitMQ: exchanges, queues, bindings
├── RabbitMqDemo.cs         # RabbitMQ: publish and consume
├── KafkaDemo.cs            # Kafka: entry point and topic creation
├── KafkaDemo.Producer.cs   # Kafka: producer with key
└── KafkaDemo.Consumer.cs   # Kafka: consumer groups
```

## License

[MIT](LICENSE). Author: Carlos Eduardo Seidl.
