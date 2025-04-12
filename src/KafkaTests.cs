using Confluent.Kafka;

namespace kafka_test_containers;

public class KafkaTests(KafkaFixture kafkaFixture) : IClassFixture<KafkaFixture>
{
    [Fact]
    public async Task Test1()
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafkaFixture.Container.GetBootstrapAddress(),
        };
        
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        var produceResult = await producer.ProduceAsync("topic", new Message<Null, string> { Value = Guid.NewGuid().ToString() });
        Assert.Equal(PersistenceStatus.Persisted, produceResult.Status);
    }
}