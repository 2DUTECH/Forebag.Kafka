using Confluent.Kafka;

namespace Forebag.Kafka
{
    public class SingleTopicConsumerBackgroundServiceConfig : ConsumerConfig
    {
        public string? TopicForConsume { get; set; }
    }
}
