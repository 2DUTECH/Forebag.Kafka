using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class StringTypedConsumer : Kafka.StringTypedConsumer
    {
        private readonly StringTypedConsumerOptions _options;
        private readonly TestConsumerBuffer _buffer;

        public StringTypedConsumer(
            TestConsumerBuffer buffer,
            IOptions<StringTypedConsumerOptions> options,
            ILogger<StringTypedConsumer> logger) : base(logger)
        {
            _options = options.Value;
            _buffer = buffer;
        }

        public override Task ProcessMessage(string key, string value, TopicPartitionOffset offset)
        {
            _buffer.AddMessage(offset.Topic, key, new TestKafkaMessage { Message = value });

            return Task.CompletedTask;
        }

        protected override StringTypedConsumerOptions BuildOptions() => _options;
    }
}
