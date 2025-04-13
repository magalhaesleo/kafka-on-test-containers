using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaProducer;

public sealed class Producer : IDisposable
{
    private readonly CachedSchemaRegistryClient _schemaRegistryClient;
    private readonly IProducer<string, Person> _producer;

    public Producer(string bootstrapServers, string schemaRegistryUrl)
    {
        _schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = schemaRegistryUrl
        });
        _producer = new ProducerBuilder<string, Person>(new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                LingerMs = 100
            })
            .SetValueSerializer(new AvroSerializer<Person>(_schemaRegistryClient).AsSyncOverAsync())
            .Build();
    }
    
    public void Produce(Person person)
    {
        const string topic = "persons";
        
        var message = new Message<string, Person>
        {
            Value = person
        };
        _producer.Produce(topic, message);
    }
    
    public void Flush() => _producer.Flush();

    public void Dispose()
    {
        _producer.Dispose();
        _schemaRegistryClient.Dispose();
    }
}