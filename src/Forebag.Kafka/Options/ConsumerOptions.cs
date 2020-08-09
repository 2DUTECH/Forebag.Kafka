using System.Collections.Generic;
using Confluent.Kafka;

namespace Forebag.Kafka
{
    /// <summary>
    /// Options for initialization Kafka consumer.
    /// This object is wrapper for <see cref="ConsumerConfig"/>.
    /// </summary>
    public class ConsumerOptions : ConsumerConfig
    {
        /// <summary>
        /// List of topics for consume.
        /// </summary>
        public List<string>? TopicsForConsume { get; set; }
    }
}
