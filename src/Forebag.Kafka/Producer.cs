using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace Forebag.Kafka
{
    /// <summary>
    ///     Wrapper for <see href="Confluent.Kafka.IProducer"/>.
    /// </summary>
    /// <remarks>
    ///     All messages which is sending to topics serialize to JSON string. 
    /// </remarks>
    public class Producer : IDisposable
    {
        private readonly ILogger<Producer> _logger;
        private readonly ProducerOptions _options;
        private readonly Lazy<IProducer<string, string>> _lazyProducer;
        private IProducer<string, string> _producer => _lazyProducer.Value;

        /// <inheritdoc/>
        public Producer(
            IOptions<ProducerOptions> options,
            ILogger<Producer> logger)
        {
            _options = options.Value;
            _logger = logger;

            _lazyProducer = new Lazy<IProducer<string, string>>(() =>
            {
                var producer = new ProducerBuilder<string, string>(_options).Build();

                _logger.LogInfoStartProducing(producer);

                return producer;
            });
        }

        /// <summary>
        /// Serializes message to JSON and sends it to the topic.
        /// </summary>
        /// <param name="key">Key for message.</param>
        /// <param name="value">Message.</param>
        /// <param name="topicName">Topic for sending.</param>
        public async Task<TopicPartitionOffset> Produce<T>(string key, T value, string topicName)
        {
            string? serializedValue = null;

            try
            {
                serializedValue = JsonConvert.SerializeObject(value, Formatting.None);

                var message = new Message<string, string>
                {
                    Key = key,
                    Value = serializedValue
                };

                var deliveryResult = await _producer.ProduceAsync(topicName, message);

                _logger.LogDelivery(_producer, key, serializedValue, deliveryResult.TopicPartitionOffset);

                return deliveryResult.TopicPartitionOffset;
            }
            catch (ProduceException<string, string> ex)
            {
                _logger.LogDeliveryError(_producer, key, serializedValue!, ex.Error.Reason);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogDeliveryError(_producer, key, serializedValue!, ex.Message);

                throw;
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (_lazyProducer.IsValueCreated)
            {
                _producer.Flush(TimeSpan.FromSeconds(5));

                _producer.Dispose();
            }
        }
    }
}
