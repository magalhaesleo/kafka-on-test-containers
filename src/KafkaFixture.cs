using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.Kafka;

namespace kafka_test_containers;

public class KafkaFixture : IAsyncLifetime
{
    private readonly INetwork _network;
    public const int SchemaRegistryPort = 8081;
    public KafkaContainer KafkaContainer { get; }
    public IContainer SchemaRegistry { get; }
    public KafkaFixture()
    {
        _network = new NetworkBuilder().Build();
        const string kafkaBroker = "kafka";
        KafkaContainer = new KafkaBuilder().WithNetwork(_network).WithNetworkAliases(kafkaBroker).Build();
        SchemaRegistry = new ContainerBuilder()
            .DependsOn(KafkaContainer)
            .WithImage("confluentinc/cp-schema-registry:7.9.0")
            .WithNetwork(_network)
            .WithPortBinding(SchemaRegistryPort, true)
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", $"http://0.0.0.0:{SchemaRegistryPort}")
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", $"PLAINTEXT://{kafkaBroker}:{KafkaBuilder.BrokerPort}")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server started, listening for requests..."))
            .Build();
    }

    public string GetSchemaRegistryUrl() =>
        $"http://{SchemaRegistry.Hostname}:{SchemaRegistry.GetMappedPublicPort(SchemaRegistryPort)}";
    
    public async Task InitializeAsync()
    {
        await _network.CreateAsync();
        await KafkaContainer.StartAsync();
        await SchemaRegistry.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await SchemaRegistry.DisposeAsync();
        await KafkaContainer.DisposeAsync();
        await _network.DisposeAsync();
    }
}
