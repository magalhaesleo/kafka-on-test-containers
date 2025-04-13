# Kafka on Test Containers

This is a test project using **xUnit**, **Testcontainers for .NET**, **Apache Kafka**, and **Confluent Schema Registry** to demonstrate the publication of messages in **Avro format** to a Kafka topic.

## Features

- ✅ Integration tests with **Kafka in Docker** via Testcontainers
- ✅ Message publishing using **Avro serialization**
- ✅ **Schema Registry** integration
- ✅ End-to-end test verifying message publication to a Kafka topic
- ✅ Built with **.NET** and **xUnit**

## Technologies Used

- [.NET](https://dotnet.microsoft.com/)
- [xUnit](https://xunit.net/)
- [Testcontainers for .NET](https://github.com/testcontainers/testcontainers-dotnet)
- [Apache Kafka](https://kafka.apache.org/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet)
- [Chr.Avro](https://github.com/ch-robinson/dotnet-avro)

## Getting Started

### Prerequisites

- Docker
- .NET 8 or later
- Internet connection to pull Kafka and Schema Registry containers

### How It Works

1. Testcontainers starts Kafka and Schema Registry containers.
2. A test produces a message to a Kafka topic using Avro.
3. The schema is registered automatically in the Schema Registry.

### Run the Tests

```bash
dotnet test
