using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class SingleTopicConsumer : SingleTopicConsumerBackgroundService<TestKafkaMessage>
    {
        private readonly SingleTopicConsumerBackgroundServiceConfig _config;
        private readonly TestConsumerBuffer _buffer;

        public SingleTopicConsumer(
            TestConsumerBuffer buffer,
            IOptions<SingleTopicConsumerBackgroundServiceConfig> config,
            ILogger<SingleTopicConsumer> logger) : base(logger)
        {
            _config = config.Value;
            _buffer = buffer;
        }

        public override Task ProcessMessage(string key, TestKafkaMessage value, TopicPartitionOffset offset)
        {
            _buffer.AddMessage(offset.Topic, key, value);

            return Task.CompletedTask;
        }

        protected override SingleTopicConsumerBackgroundServiceConfig BuildConfig() => _config;
    }
}
