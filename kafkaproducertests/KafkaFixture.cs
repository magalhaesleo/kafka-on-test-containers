using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.Kafka;

namespace KafkaProducerTests;

public class KafkaFixture : IAsyncLifetime
{
    private readonly INetwork _network;
    public const ushort SchemaRegistryPort = 8081;
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
    
    public List<PartitionMetadata> GetTopicPartitions(string topic)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
            { BootstrapServers = KafkaContainer.GetBootstrapAddress() }).Build(); 
        
        var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20)); 
        var topicMetadata = meta.Topics.Single(t => t.Topic == topic);
        return topicMetadata.Partitions;
    }
    
    public IEnumerable<TValue> GetAllMessages<TKey, TValue>(string topic) where TValue : ISpecificRecord
    {
        using var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = GetSchemaRegistryUrl() });
        using var consumer = new ConsumerBuilder<TKey, TValue>(new ConsumerConfig
            {
                BootstrapServers = KafkaContainer.GetBootstrapAddress(),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = "tests"
            })
            .SetValueDeserializer(new AvroDeserializer<TValue>(schemaRegistry).AsSyncOverAsync())
            .Build();

        var partitions = GetTopicPartitions(topic);
        consumer.Assign(partitions.Select(partitionMetadata => new TopicPartitionOffset(topic, partitionMetadata.PartitionId, Offset.Beginning)));

        while (true)
        {
            var timeout = TimeSpan.FromSeconds(1);
            using var cts = new CancellationTokenSource(timeout);
            TValue value;
            try
            {
                var consumeResult = consumer.Consume(cts.Token);
                value = consumeResult.Message.Value;
            }
            catch (OperationCanceledException e)
            {
                yield break;
            }
            
            yield return value;
        }
    }
}
