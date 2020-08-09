using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class TestProducer : Producer
    {
        private readonly TestProducerOptions _options;

        public TestProducer(
            IOptions<TestProducerOptions> options,
            ILogger<TestProducer> logger) : base(options, logger)
        {
            _options = options.Value;
        }

        public async Task<List<TopicPartitionOffset>> Produce(string key, TestKafkaMessage value)
        {
            var topicPartitionOffsets = new List<TopicPartitionOffset>();

            foreach (var topic in _options.TopicsForProduce!)
            {
                var deliveryResult = await Produce(key, value, topic);

                topicPartitionOffsets.Add(deliveryResult);
            }

            return topicPartitionOffsets;
        }
    }
}
