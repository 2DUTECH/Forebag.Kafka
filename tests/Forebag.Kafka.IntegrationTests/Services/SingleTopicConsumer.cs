using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class SingleTopicConsumer : SingleTopicConsumer<TestKafkaMessage>
    {
        private readonly SingleTopicConsumerOptions _options;
        private readonly TestConsumerBuffer _buffer;

        public SingleTopicConsumer(
            TestConsumerBuffer buffer,
            IOptions<SingleTopicConsumerOptions> options,
            ILogger<SingleTopicConsumer> logger) : base(logger)
        {
            _options = options.Value;
            _buffer = buffer;
        }

        public override Task ProcessMessage(string key, TestKafkaMessage value, TopicPartitionOffset offset)
        {
            _buffer.AddMessage(offset.Topic, key, value);

            return Task.CompletedTask;
        }

        protected override SingleTopicConsumerOptions BuildOptions() => _options;
    }
}
