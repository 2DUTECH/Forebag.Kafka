using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class SingleTopicProducer : Producer<TestKafkaMessage>
    {
        private readonly SingleTopicProducerOptions _options;

        public SingleTopicProducer(
            IOptions<SingleTopicProducerOptions> options,
            ILogger<SingleTopicProducer> logger,
            ISerializer<TestKafkaMessage> serializer) : base(options, logger, serializer)
        {
            _options = options.Value;
        }

        public async Task<TopicPartitionOffset> Produce(string key, TestKafkaMessage value)
            => await Produce(key, value, _options.TopicForSingleTopicConsumer!);
    }
}
