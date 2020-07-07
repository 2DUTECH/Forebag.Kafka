using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace Forebag.Kafka.Demo.Consumer
{
    public class CreateUserCommandConsumer : SingleTopicConsumer<CreateUserCommand>
    {
        public CreateUserCommandConsumer(ILogger<CreateUserCommandConsumer> logger)
            : base(logger)
        {
        }

        public override Task ProcessMessage(string key, CreateUserCommand value, TopicPartitionOffset offset)
        {
            return Task.CompletedTask;
        }

        protected override SingleTopicConsumerOptions BuildOptions()
            => new SingleTopicConsumerOptions
            {
                GroupId = "create_user_service",
                BootstrapServers = "localhost:29092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                TopicForConsume = "test_topic"
            };
    }
}
