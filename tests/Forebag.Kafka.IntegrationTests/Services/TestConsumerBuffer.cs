using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class TestConsumerBuffer
    {
        private readonly TestConsumerBufferOptions _config;
        private readonly ConcurrentDictionary<string, MessageBuffer> _topics;

        public TestConsumerBuffer(IOptions<TestConsumerBufferOptions> config)
        {
            _config = config.Value;
            _topics = new ConcurrentDictionary<string, MessageBuffer>();
            _topics.AddOrUpdate(_config.TopicA!, new MessageBuffer(), (topicName, MessageBuffer) => MessageBuffer);
            _topics.AddOrUpdate(_config.TopicB1!, new MessageBuffer(), (topicName, MessageBuffer) => MessageBuffer);
            _topics.AddOrUpdate(_config.TopicB2!, new MessageBuffer(), (topicName, MessageBuffer) => MessageBuffer);
            _topics.AddOrUpdate(_config.TopicC1!, new MessageBuffer(), (topicName, MessageBuffer) => MessageBuffer);
            _topics.AddOrUpdate(_config.TopicC2!, new MessageBuffer(), (topicName, MessageBuffer) => MessageBuffer);
            _topics.AddOrUpdate(_config.TopicC3!, new MessageBuffer(), (topicName, MessageBuffer) => MessageBuffer);
        }

        public MessageBuffer GetTopicByName(string topicName)
        {
            if (_topics.TryGetValue(topicName, out var buffer))
                return buffer;
            else throw new Exception($"Can't find collection for topic with name {topicName}");
        }

        public void AddMessage(string topicName, string key, TestKafkaMessage message) =>
            GetTopicByName(topicName)
            .AddMessage(key, message);

        public async Task<TestKafkaMessage> TryConsumeFromA(string key, CancellationToken cancellationToken) =>
            await GetTopicByName(_config.TopicA!).TryConsume(key, cancellationToken);

        public async Task<TestKafkaMessage> TryConsumeFromB1(string key, CancellationToken cancellationToken) =>
            await GetTopicByName(_config.TopicB1!).TryConsume(key, cancellationToken);

        public async Task<TestKafkaMessage> TryConsumeFromB2(string key, CancellationToken cancellationToken) =>
            await GetTopicByName(_config.TopicB2!).TryConsume(key, cancellationToken);

        public async Task<TestKafkaMessage> TryConsumeFromC1(string key, CancellationToken cancellationToken) =>
            await GetTopicByName(_config.TopicC1!).TryConsume(key, cancellationToken);

        public async Task<TestKafkaMessage> TryConsumeFromC2(string key, CancellationToken cancellationToken) =>
            await GetTopicByName(_config.TopicC2!).TryConsume(key, cancellationToken);

        public async Task<TestKafkaMessage> TryConsumeFromC3(string key, CancellationToken cancellationToken) =>
            await GetTopicByName(_config.TopicC3!).TryConsume(key, cancellationToken);

        public class MessageBuffer
        {
            public readonly ConcurrentDictionary<string, TestKafkaMessage> Messages = new ConcurrentDictionary<string, TestKafkaMessage>();

            public void AddMessage(string key, TestKafkaMessage message)
            {
                Messages.AddOrUpdate(key, message, (k, v) => v);
            }

            public async Task<TestKafkaMessage> TryConsume(string key, CancellationToken cancellationToken)
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    if (Messages.TryGetValue(key, out var message))
                        return message;

                    await Task.Delay(100);
                }

                return default!;
            }
        }
    }
}
