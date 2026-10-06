[English](README.md) | Português

# Kafka vs RabbitMQ em .NET

Duas demos pequenas e executáveis que colocam **RabbitMQ** (`RabbitMQ.Client` 7.2.2) e **Apache Kafka** (`Confluent.Kafka` 2.15.1) lado a lado em .NET 10. As duas movem o mesmo evento `OrderPlaced` (id do pedido, id do cliente, total), o que facilita comparar as diferenças de modelo e de garantias.

## Pré-requisitos

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Docker](https://www.docker.com/) (para rodar os brokers)

## Como rodar

Suba os brokers (RabbitMQ 4 com o painel de administração e Kafka 4.0 em modo KRaft, sem ZooKeeper):

```bash
docker compose up -d
```

- RabbitMQ: AMQP em `localhost:5672`, painel em <http://localhost:15672> (`guest` / `guest`)
- Kafka: `localhost:9092`

Depois rode uma das demos (cada uma tem timeout de 60 segundos):

```bash
dotnet run -- rabbit
dotnet run -- kafka
```

Rodar sem argumento imprime a linha de uso.

## O que cada demo mostra

As duas demos publicam seis pedidos de exemplo para dois clientes (`cli-1`, `cli-2`). Um deles é inválido (total `0`).

### RabbitMQ (`RabbitMqDemo.cs`)

- Uma **exchange direct** durável (`orders`) roteando a chave `order.placed`.
- Uma **quorum queue** (`orders.process`) para o trabalho principal.
- Uma **dead-letter exchange** (`orders.dlx`, fanout) alimentando `orders.dead`: o pedido inválido é rejeitado com `requeue: false` e vai parar lá.
- **Publisher confirms** habilitados, e mensagens persistentes com `MessageId`.
- **Ack manual** (`autoAck: false`): a mensagem só sai da fila após o `BasicAck`.
- **Prefetch 1**, com dois workers (`worker-1`, `worker-2`) dividindo a fila.
- **Fan-out por duas filas**: `orders.process` e `orders.audit` são ligadas com a mesma chave, então cada uma recebe a sua cópia. Ninguém consome `orders.audit`, e no fim a demo imprime a contagem de mensagens de cada fila.

### Kafka (`KafkaDemo.cs`)

- Um tópico criado a cada execução (nome novo, então offsets antigos nunca importam) com **3 partições**.
- O **id do cliente como chave** da mensagem: o mesmo cliente sempre vai para a mesma partição, então a ordem dele é preservada. O produtor imprime a partição e o offset de cada mensagem.
- Um produtor com **`acks=all` e idempotência** habilitados (retry sem duplicar).
- **Dois consumer groups** (`billing` e `analytics`) lendo os mesmos eventos, cada um no seu ritmo.
- **Commit manual** após o processamento (`EnableAutoCommit = false`), o que dá entrega **at-least-once**.

## Estrutura do projeto

```
.
├── MensageriaDemo.csproj   # app console .NET 10 (RabbitMQ.Client, Confluent.Kafka)
├── MensageriaDemo.slnx     # arquivo de solução
├── docker-compose.yml      # RabbitMQ e Kafka (KRaft)
├── Program.cs              # ponto de entrada: dotnet run -- rabbit|kafka
├── Contracts.cs            # record OrderPlaced, serialização JSON, pedidos de exemplo
├── RabbitMqDemo.cs         # demo do RabbitMQ
└── KafkaDemo.cs            # demo do Kafka
```
