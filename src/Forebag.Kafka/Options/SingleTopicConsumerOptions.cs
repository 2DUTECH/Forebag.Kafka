using Confluent.Kafka;

namespace Forebag.Kafka
{
    /// <summary>
    /// Параметры консьюмера работающего с одним топиком
    /// </summary>
    public class SingleTopicConsumerOptions : ConsumerConfig
    {
        /// <inheritdoc/>
        public string? TopicForConsume { get; set; }
    }
}
