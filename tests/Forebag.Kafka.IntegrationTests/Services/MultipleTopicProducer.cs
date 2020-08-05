using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class MultipleTopicProducer : Producer<TestKafkaMessage>
    {
        private readonly MultipleTopicProducerConfig _options;

        public MultipleTopicProducer(
            IOptions<MultipleTopicProducerConfig> options,
            ILogger<MultipleTopicProducer> logger,
            ISerializer<TestKafkaMessage> serializer) : base(options, logger, serializer)
        {
            _options = options.Value;
        }

        public async Task<TopicPartitionOffset[]> Produce(string key, TestKafkaMessage value)
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
