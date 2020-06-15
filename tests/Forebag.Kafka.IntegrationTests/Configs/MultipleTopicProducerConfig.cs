using Confluent.Kafka;

namespace Forebag.Kafka.IntegrationTests
{
    public class MultipleTopicProducerConfig : ProducerConfig
    {
        public string[]? TopicsForMultipleTopicConsumer { get; set; }
    }
}
