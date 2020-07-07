using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class SingleTopicProducer : Producer
    {
        private readonly SingleTopicProducerConfig _options;

        public SingleTopicProducer(
            IOptions<SingleTopicProducerConfig> options,
            ILogger<SingleTopicProducer> logger) : base(options, logger)
        {
            _options = options.Value;
        }

        public async Task<TopicPartitionOffset> Produce<T>(string key, T value)
            => await Produce(key, value, _options.TopicForSingleTopicConsumer!);
    }
}
