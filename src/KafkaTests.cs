using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Testcontainers.Builders;
using Testcontainers.Kafka;

namespace kafka_test_containers;

public sealed class KafkaTests : IClassFixture<KafkaFixture>, IDisposable
{
    private readonly CachedSchemaRegistryClient _schemaRegistryClient;
    private readonly IProducer<string, GenericRecord> _producer;
    public KafkaTests(KafkaFixture kafkaFixture)
    {
        _schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = kafkaFixture.GetSchemaRegistryUrl()
        });
        _producer = new ProducerBuilder<string, GenericRecord>(new ProducerConfig
            {
                BootstrapServers = kafkaFixture.KafkaContainer.GetBootstrapAddress(),
            })
            .SetValueSerializer(new AvroSerializer<GenericRecord>(_schemaRegistryClient))
            .Build();
    }
    
    [Fact]
    public async Task Given_message_should_publish_successfully()
    {
        // Arrange


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
        var produceResult = await _producer.ProduceAsync("topic", new Message<string, GenericRecord> { Value = record });

        // Assert
        Assert.Equal(PersistenceStatus.Persisted, produceResult.Status);
    }

    public void Dispose()
    {
        _producer.Dispose();
        _schemaRegistryClient.Dispose();
    }
}