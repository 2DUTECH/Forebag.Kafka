using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace Forebag.Kafka
{
    /// <summary>
    /// Представляет Producer.
    /// </summary>
    public class Producer : IDisposable
    {
        private const int TimeForProduceFlush = 1000;

        private readonly ILogger<Producer> _logger;
        private readonly ProducerOptions _options;
        private readonly Lazy<IProducer<string, string>> _lazyProducer;
        private bool _disposed = false;

        private IProducer<string, string> _producer => _lazyProducer.Value;

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
        /// Выполняет сериализацию сообщения в JSON и отправку в указанный топик.
        /// </summary>
        /// <param name="key">Ключ сообщения.</param>
        /// <param name="value">Экземпляр сообщения.</param>
        /// <param name="topicName">Топик для отправляемого сообщения.</param>
        public async Task<TopicPartitionOffset> Produce<T>(string key, T value, string topicName)
        {
            string? serializedValue = null;

            try
            {
                serializedValue = JsonConvert.SerializeObject(value);

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

        /// <summary>
        /// Выполняет отправку подготовленного, в виде JSON, сообщения в указанный топик.
        /// </summary>
        /// <param name="key">Ключ сообщения.</param>
        /// <param name="value">Экземпляр сообщения.</param>
        /// <param name="topicName">Топик для отправляемого сообщения.</param>
        public async Task<TopicPartitionOffset> ProduceAsJsonString(string key, string value, string topicName)
        {
            try
            {
                var message = new Message<string, string>
                {
                    Key = key,
                    Value = value
                };

                var deliveryResult = await _producer.ProduceAsync(topicName, message);

                _logger.LogDelivery(_producer, key, value, deliveryResult.TopicPartitionOffset);

                return deliveryResult.TopicPartitionOffset;
            }
            catch (ProduceException<string, string> ex)
            {
                _logger.LogDeliveryError(_producer, key, value!, ex.Error.Reason);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogDeliveryError(_producer, key, value!, ex.Message);

                throw;
            }
        }

        ~Producer()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing && _lazyProducer.IsValueCreated)
            {
                _producer.Flush(TimeSpan.FromMilliseconds(TimeForProduceFlush));
                _producer.Dispose();
            }

            _disposed = true;
        }
    }
}
