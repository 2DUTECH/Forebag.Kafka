using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Forebag.Kafka.IntegrationTests
{
    public class TestConsumerBuffer
    {
        private readonly TestConsumerBufferOptions _options;
        private readonly Dictionary<string, Dictionary<string, TestKafkaMessage>> _topics;

        public List<string> Topics => _options.Topics!.Keys.ToList();

        public TestConsumerBuffer(IOptions<TestConsumerBufferOptions> options)
        {
            _options = options.Value;
            _topics = new Dictionary<string, Dictionary<string, TestKafkaMessage>>();

            foreach (var topic in _options.Topics!)
            {
                _topics.Add(topic.Key, new Dictionary<string, TestKafkaMessage>());
            }
        }

        public Dictionary<string, TestKafkaMessage> GetTopicByName(string topicName)
        {
            if (_topics.TryGetValue(topicName, out var buffer))
                return buffer;
            else throw new Exception($"Can't find collection for topic with name {topicName}");
        }

        public void AddMessage(string topicName, string key, TestKafkaMessage message) =>
            _topics[topicName].Add(key, message);

        public List<TestKafkaMessage> Consume(string key, CancellationToken cancellationToken)
        {
            SpinWait.SpinUntil(() => false ||
                    !_topics.All(t => t.Value.ContainsKey(key)) ||
                    !cancellationToken.IsCancellationRequested);

            return _topics.Values.Select(dict => dict.Single(msg => msg.Key.Equals(key)).Value).ToList();
        }
    }
}
