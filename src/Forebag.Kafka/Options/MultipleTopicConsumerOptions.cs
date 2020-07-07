using Confluent.Kafka;

namespace Forebag.Kafka
{
    /// <summary>
    /// Параметры консьюмера работающего с несколькими топиками
    /// </summary>
    public class MultipleTopicConsumerOptions : ConsumerConfig
    {
        public string[]? TopicsForConsume { get; set; }
    }
}
