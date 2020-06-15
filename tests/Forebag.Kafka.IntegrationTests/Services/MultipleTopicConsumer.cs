using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class MultipleTopicConsumer : MultipleTopicConsumerBackgroundService<TestKafkaMessage>
    {
        private readonly MultipleTopicConsumerBackgroundServiceConfig _config;
        private readonly TestConsumerBuffer _buffer;

        public MultipleTopicConsumer(
            TestConsumerBuffer buffer,
            IOptions<MultipleTopicConsumerBackgroundServiceConfig> config,
            ILogger<MultipleTopicConsumer> logger) : base(logger)
        {
            _config = config.Value;
            _buffer = buffer;
        }

        public override Task ProcessMessage(string key, TestKafkaMessage value, TopicPartitionOffset offset)
        {
            _buffer.AddMessage(offset.Topic, key, value);

            return Task.CompletedTask;
        }

        protected override MultipleTopicConsumerBackgroundServiceConfig BuildConfig() => _config;
    }
}
