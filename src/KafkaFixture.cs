using Testcontainers.Kafka;
using Testcontainers.Xunit;
using Xunit.Abstractions;

namespace kafka_test_containers;

public class KafkaFixture(IMessageSink messageSink) : ContainerFixture<KafkaBuilder, KafkaContainer>(messageSink)
{
}