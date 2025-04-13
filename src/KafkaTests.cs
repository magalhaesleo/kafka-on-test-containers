using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Testcontainers.Builders;
using Testcontainers.Kafka;

namespace kafka_test_containers;

public class KafkaTests
{
    [Fact]
    public async Task Test1()
    {
        // Arrange
        await using var kafkaNetwork = new NetworkBuilder().Build();
        await kafkaNetwork.CreateAsync();
        await using var kafkaContainer = new KafkaBuilder()
            .WithNetwork(kafkaNetwork)
            .WithNetworkAliases("kafka")
            .Build();
        await using var schemaRegistryContainer = new ContainerBuilder()
            .DependsOn(kafkaContainer)
            .WithImage("confluentinc/cp-schema-registry:7.5.2")
            .WithNetwork(kafkaNetwork)
            .WithExposedPort(8081)
            .WithPortBinding(8081, true)
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                $"PLAINTEXT://kafka:{KafkaBuilder.BrokerPort}")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server started, listening for requests..."))
            .Build();
        await Task.WhenAll(kafkaContainer.StartAsync(), schemaRegistryContainer.StartAsync());
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafkaContainer.GetBootstrapAddress(),
        };

        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = $"http://{schemaRegistryContainer.Hostname}:{schemaRegistryContainer.GetMappedPublicPort(8081)}"
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var producer = new ProducerBuilder<string, GenericRecord>(producerConfig)
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
            .Build();

        // Act
        const string json = """
            {
                    "type": "record",
                    "name": "User",
                    "fields": [
                        { "name": "name", "type": "string" },
                        { "name": "favorite_number",  "type": "long" },
                        { "name": "favorite_color", "type": "string" }
                    ]
                }
            """;
        var s = (RecordSchema)Avro.Schema.Parse(json);
        const long favoriteNumber = 10;
        var record = new GenericRecord(s);
        record.Add("name", "Leonardo");
        record.Add("favorite_number", favoriteNumber);
        record.Add("favorite_color", "red");
        var produceResult = await producer.ProduceAsync("topic", new Message<string, GenericRecord> { Value = record });

        // Assert
        Assert.Equal(PersistenceStatus.Persisted, produceResult.Status);
    }
}