using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Forebag.Kafka.IntegrationTests
{
    public class TestConsumerBuffer
    {
        private readonly TestConsumerBufferOptions _options;
        private readonly ConcurrentDictionary<string, MessageBuffer> _topics;

        public List<string> Topics => _options.Topics!.Keys.ToList();

        public TestConsumerBuffer(IOptions<TestConsumerBufferOptions> options)
        {
            _options = options.Value;
            _topics = new ConcurrentDictionary<string, MessageBuffer>();

            foreach (var topic in _options.Topics!)
            {
                _topics.AddOrUpdate(topic.Key, new MessageBuffer(), (topicName, MessageBuffer) => MessageBuffer);
            }
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

        public async Task<TestKafkaMessage> TryConsume(string topicName, string key, CancellationToken cancellationToken) =>
            await GetTopicByName(topicName).TryConsume(key, cancellationToken);

        public List<TestKafkaMessage> Consume(string key, CancellationToken cancellationToken)
        {
            SpinWait.SpinUntil(() => !_topics.All(t => t.Value.Messages.ContainsKey(key)) || !cancellationToken.IsCancellationRequested);

            return _topics.Values.Select(v => v.Messages.Single(m => m.Key.Equals(key)).Value).ToList();
        }

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
