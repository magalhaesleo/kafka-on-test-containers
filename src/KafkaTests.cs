using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Testcontainers.Builders;
using Testcontainers.Kafka;

namespace kafka_test_containers;

public class KafkaTests(KafkaFixture kafkaFixture) : IClassFixture<KafkaFixture>
{
    [Fact]
    public async Task Given_message_should_publish_successfully()
    {
        // Arrange
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafkaFixture.KafkaContainer.GetBootstrapAddress(),
        };

        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = kafkaFixture.GetSchemaRegistryUrl()
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