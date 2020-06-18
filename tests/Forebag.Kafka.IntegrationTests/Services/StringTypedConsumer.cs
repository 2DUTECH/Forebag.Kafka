using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class StringTypedConsumer : StringTypedConsumerBackgroundService
    {
        private readonly StringTypedConsumerBackgroundServiceConfig _config;
        private readonly TestConsumerBuffer _buffer;

        public StringTypedConsumer(
            TestConsumerBuffer buffer,
            IOptions<StringTypedConsumerBackgroundServiceConfig> config,
            ILogger<StringTypedConsumer> logger) : base(logger)
        {
            _config = config.Value;
            _buffer = buffer;
        }

        public override Task ProcessMessage(string key, string value, TopicPartitionOffset offset)
        {
            _buffer.AddMessage(offset.Topic, key, new TestKafkaMessage { Message = value });

            return Task.CompletedTask;
        }

        protected override (ConsumerConfig? ConsumerConfig, string[]? TopicsForRead) BuildParameters() => (_config, _config.TopicsForConsume);
    }
}
