using KafkaProducer;

namespace KafkaProducerTests;

public sealed class KafkaTests(KafkaFixture kafkaFixture) : IClassFixture<KafkaFixture>, IDisposable
{
    private readonly Producer _producer = new(kafkaFixture.KafkaContainer.GetBootstrapAddress(), kafkaFixture.GetSchemaRegistryUrl());

    [Fact]
    public void Given_message_should_publish_successfully()
    {
        // Arrange
        const string topic = "persons";
        var person = new Person("Leonardo", 10, "red");

        // Act
        _producer.Produce(person);

        // Assert
        _producer.Flush();
        var persons = kafkaFixture.GetAllMessages<string, Person>(topic).ToList();
        Assert.Equivalent(Assert.Single(persons), person);
    }

    public void Dispose()
    {
        _producer.Dispose();
    }
}