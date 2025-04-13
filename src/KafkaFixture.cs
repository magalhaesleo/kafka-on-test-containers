using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Testcontainers.Kafka;
using Testcontainers.Xunit;
using Xunit.Abstractions;

namespace kafka_test_containers;

public class KafkaFixture(IMessageSink messageSink) : IAsyncLifetime
{
    public KafkaContainer KafkaContainer { get; } = BuildKafka();
    public IContainer SchemaRegistry { get; } = BuildSchemaRegistry();
    private static KafkaContainer BuildKafka() => new KafkaBuilder().Build();
    private static IContainer BuildSchemaRegistry()
    {
        return new ContainerBuilder()
            .WithImage("confluentinc/cp-schema-registry:7.5.2")
            .WithExposedPort(8081)
            .WithPortBinding(8081, true)
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", $"PLAINTEXT://kafka:{KafkaBuilder.KafkaPort}")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server started, listening for requests..."))
            .Build();
    }

    public async Task InitializeAsync()
    {
        await KafkaContainer.StartAsync();
        await SchemaRegistry.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await KafkaContainer.DisposeAsync();
        await SchemaRegistry.DisposeAsync();
    }
}
