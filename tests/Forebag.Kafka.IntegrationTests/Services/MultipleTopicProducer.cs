using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class MultipleTopicProducer : KafkaProducer
    {
        private readonly MultipleTopicProducerConfig _config;

        public MultipleTopicProducer(
            MultipleTopicProducerConfig config,
            ILogger<MultipleTopicProducer> logger) : base(config, logger)
        {
            _config = config;
        }

        public async Task<TopicPartitionOffset[]> Produce<T>(string key, T value)
        {
            var topicPartitionOffsets = new TopicPartitionOffset[_config.TopicsForMultipleTopicConsumer!.Length];

            for (var i = 0; i < _config.TopicsForMultipleTopicConsumer!.Length; i++)
            {
                topicPartitionOffsets[i] = await Produce(key, value, _config.TopicsForMultipleTopicConsumer[i]);
            }

            return topicPartitionOffsets;
        }
    }
}
