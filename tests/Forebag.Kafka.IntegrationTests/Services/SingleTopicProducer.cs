using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class SingleTopicProducer : Producer<TestKafkaMessage>
    {
        private readonly SingleTopicProducerConfig _options;

        public SingleTopicProducer(
            IOptions<SingleTopicProducerConfig> options,
            ILogger<SingleTopicProducer> logger,
            ISerializer<TestKafkaMessage> serializer) : base(options, logger, serializer)
        {
            _options = options.Value;
        }

        public async Task<TopicPartitionOffset> Produce(string key, TestKafkaMessage value)
            => await Produce(key, value, _options.TopicForSingleTopicConsumer!);
    }
}
