# Kafka vs RabbitMQ em .NET

Código do artigo **Kafka vs RabbitMQ em .NET: Mensageria na Prática**. O mesmo conjunto de seis pedidos (um deles inválido) é publicado nos dois brokers, para mostrar o que acontece depois.

- **RabbitMQ**: exchange `direct`, quorum queue, dead-letter, ack manual. O pedido inválido vai para `orders.dead`.
- **Kafka**: tópico com 3 partições, chave por cliente, dois grupos de consumo (`billing` e `analytics`) lendo os mesmos eventos, commit manual.

## Requisitos

- .NET 10 SDK
- Docker (ou instalação nativa, veja abaixo)

## Rodando

```bash
docker compose up -d
dotnet run -- rabbit
dotnet run -- kafka
docker compose down -v
```

O painel do RabbitMQ fica em http://localhost:15672 (guest/guest).

## Sem Docker

O código só espera um RabbitMQ em `localhost:5672` (guest/guest) e um Kafka em `localhost:9092`.

- **RabbitMQ**: instale o Erlang/OTP e o RabbitMQ e rode `rabbitmq-plugins enable rabbitmq_management`.
- **Kafka (KRaft)**: instale o Java 17+, baixe o `.tgz` em https://kafka.apache.org/downloads e, na pasta extraída (no Windows, use o WSL 2):

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

- **Serviço gerenciado**: troque `HostName` (RabbitMQ, em `RabbitMqDemo.cs`) e `Bootstrap` (Kafka, em `KafkaDemo.cs`) e acrescente TLS e autenticação conforme o provedor.

Os demos limpam as filas do RabbitMQ a cada execução e criam um tópico novo no Kafka.
