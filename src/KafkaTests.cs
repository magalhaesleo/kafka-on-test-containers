using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace kafka_test_containers;

public sealed class KafkaTests : IClassFixture<KafkaFixture>, IDisposable
{
    private readonly CachedSchemaRegistryClient _schemaRegistryClient;
    private readonly IProducer<string, Person> _producer;
    public KafkaTests(KafkaFixture kafkaFixture)
    {
        _schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = kafkaFixture.GetSchemaRegistryUrl()
        });
        _producer = new ProducerBuilder<string, Person>(new ProducerConfig
            {
                BootstrapServers = kafkaFixture.KafkaContainer.GetBootstrapAddress(),
                LingerMs = 0
            })
            .SetValueSerializer(new AvroSerializer<Person>(_schemaRegistryClient))
            .Build();
    }
    
    [Fact]
    public async Task Given_message_should_publish_successfully()
    {
        // Arrange
        const string topic = "person";
        var person = new Person("Leonardo", 10, "red");
        var message = new Message<string, Person> { Value = person };

        // Act
        var produceResult = await _producer.ProduceAsync(topic, message);

        // Assert
        Assert.Equal(PersistenceStatus.Persisted, produceResult.Status);
    }

    public void Dispose()
    {
        _producer.Dispose();
        _schemaRegistryClient.Dispose();
    }
}