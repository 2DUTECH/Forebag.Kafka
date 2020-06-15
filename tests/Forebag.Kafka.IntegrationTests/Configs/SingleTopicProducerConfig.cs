using Confluent.Kafka;

namespace Forebag.Kafka.IntegrationTests
{
    public class SingleTopicProducerConfig : ProducerConfig
    {
        public string? TopicForSingleTopicConsumer { get; set; }
    }
}
