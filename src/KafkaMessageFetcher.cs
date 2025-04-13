using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaProducerTests;

public static class KafkaMessageFetcher
{
    public static List<PartitionMetadata> GetTopicPartitions(this KafkaFixture fixture, string topic)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
            { BootstrapServers = fixture.KafkaContainer.GetBootstrapAddress() }).Build(); 
        
        var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20)); 
        var topicMetadata = meta.Topics.Single(t => t.Topic == topic);
        return topicMetadata.Partitions;
    }
    
    public static IEnumerable<TValue> GetAllMessages<TKey, TValue>(this KafkaFixture fixture, string topic) where TValue : ISpecificRecord
    {
        using var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = fixture.GetSchemaRegistryUrl() });
        using var consumer = new ConsumerBuilder<TKey, TValue>(new ConsumerConfig
            {
                BootstrapServers = fixture.KafkaContainer.GetBootstrapAddress(),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = "tests"
            })
            .SetValueDeserializer(new AvroDeserializer<TValue>(schemaRegistry).AsSyncOverAsync())
            .Build();

        var partitions = fixture.GetTopicPartitions(topic);
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