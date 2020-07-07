using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class MultipleTopicProducer : Producer
    {
        private readonly MultipleTopicProducerConfig _options;

        public MultipleTopicProducer(
            IOptions<MultipleTopicProducerConfig> options,
            ILogger<MultipleTopicProducer> logger) : base(options, logger)
        {
            _options = options.Value;
        }

        public async Task<TopicPartitionOffset[]> Produce<T>(string key, T value)
        {
            var topicPartitionOffsets = new TopicPartitionOffset[_options.TopicsForMultipleTopicConsumer!.Length];

            for (var i = 0; i < _options.TopicsForMultipleTopicConsumer!.Length; i++)
            {
                topicPartitionOffsets[i] = await Produce(key, value, _options.TopicsForMultipleTopicConsumer[i]);
            }

            return topicPartitionOffsets;
        }
    }
}
