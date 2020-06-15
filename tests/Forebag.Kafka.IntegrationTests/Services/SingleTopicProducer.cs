using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class SingleTopicProducer : KafkaProducer
    {
        private readonly SingleTopicProducerConfig _config;

        public SingleTopicProducer(
            SingleTopicProducerConfig config,
            ILogger<SingleTopicProducer> logger) : base(config, logger)
        {
            _config = config;
        }

        public async Task<TopicPartitionOffset> Produce<T>(string key, T value)
            => await Produce(key, value, _config.TopicForSingleTopicConsumer!);
    }
}
