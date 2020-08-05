using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading.Tasks;

namespace Forebag.Kafka
{
    /// <summary>
    /// Представляет Producer.
    /// </summary>
    public class Producer<T> : IDisposable
    {
        private readonly ILogger<Producer<T>> _logger;
        private readonly ProducerOptions _options;
        private readonly ISerializer<T> _serializer;
        private readonly Lazy<IProducer<string, string>> _lazyProducer;
        private IProducer<string, string> _producer => _lazyProducer.Value;

        /// <inheritdoc/>
        public Producer(
            IOptions<ProducerOptions> options,
            ILogger<Producer<T>> logger,
            ISerializer<T> serializer)
        {
            _options = options.Value;
            _logger = logger;
            _serializer = serializer;

            _lazyProducer = new Lazy<IProducer<string, string>>(() =>
            {
                var producer = new ProducerBuilder<string, string>(_options).Build();

                _logger.LogInfoStartProducing(producer);

                return producer;
            });
        }

        /// <summary>
        /// Выполняет сериализацию сообщения в JSON и отправку в указанный топик.
        /// </summary>
        /// <param name="key">Ключ сообщения.</param>
        /// <param name="value">Экземпляр сообщения.</param>
        /// <param name="topicName">Топик для отправляемого сообщения.</param>
        protected async Task<TopicPartitionOffset> Produce(string key, T value, string topicName)
        {
            string? serializedValue = null;

            try
            {
                serializedValue = _serializer.Serialize(value);

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
