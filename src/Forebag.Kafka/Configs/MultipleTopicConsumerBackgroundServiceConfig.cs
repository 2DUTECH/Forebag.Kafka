using Confluent.Kafka;

namespace Forebag.Kafka
{
    public class MultipleTopicConsumerBackgroundServiceConfig : ConsumerConfig
    {
        public string[]? TopicsForConsume { get; set; }
    }
}
