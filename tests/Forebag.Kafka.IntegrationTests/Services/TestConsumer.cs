using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class TestConsumer : Consumer<TestKafkaMessage>
    {
        private readonly TestConsumerBuffer _buffer;

        public TestConsumer(
            TestConsumerBuffer buffer,
            IOptions<ConsumerOptions> options,
            ILogger<TestConsumer> logger) : base(options, logger)
        {
            _buffer = buffer;
        }

        public override Task ProcessMessage(string key, TestKafkaMessage value, TopicPartitionOffset offset)
        {
            _buffer.AddMessage(offset.Topic, key, value);

            return Task.CompletedTask;
        }
    }
}
