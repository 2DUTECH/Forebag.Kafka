using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class MultipleTopicConsumer : MultipleTopicConsumer<TestKafkaMessage>
    {
        private readonly MultipleTopicConsumerOptions _options;
        private readonly TestConsumerBuffer _buffer;

        public MultipleTopicConsumer(
            TestConsumerBuffer buffer,
            IOptions<MultipleTopicConsumerOptions> options,
            ILogger<MultipleTopicConsumer> logger) : base(logger)
        {
            _options = options.Value;
            _buffer = buffer;
        }

        public override Task ProcessMessage(string key, TestKafkaMessage value, TopicPartitionOffset offset)
        {
            _buffer.AddMessage(offset.Topic, key, value);

            return Task.CompletedTask;
        }

        protected override MultipleTopicConsumerOptions BuildOptions() => _options;
    }
}
