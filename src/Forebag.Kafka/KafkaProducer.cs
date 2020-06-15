using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace Forebag.Kafka
{
    /// <summary>
    /// Представляет Producer.
    /// </summary>
    public class KafkaProducer : IDisposable
    {
        private const int TimeForProduceFlush = 1000;

        private readonly ILogger<KafkaProducer> _logger;
        private readonly ProducerConfig _config;
        private readonly Lazy<IProducer<string, string>> _lazyProducer;
        private bool _disposed = false;

        private IProducer<string, string> Producer => _lazyProducer.Value;

        public KafkaProducer(ProducerConfig config, ILogger<KafkaProducer> logger)
        {
            _config = config;
            _logger = logger;

            _lazyProducer = new Lazy<IProducer<string, string>>(() =>
            {
                var producer = new ProducerBuilder<string, string>(_config).Build();

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

                var deliveryResult = await Producer.ProduceAsync(topicName, message);

                _logger.LogDelivery(Producer, key, serializedValue, deliveryResult.TopicPartitionOffset);

                return deliveryResult.TopicPartitionOffset;
            }
            catch (ProduceException<string, string> ex)
            {
                _logger.LogDeliveryError(Producer, key, serializedValue!, ex.Error.Reason);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogDeliveryError(Producer, key, serializedValue!, ex.Message);

                throw;
            }
        }

        ~KafkaProducer()
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
                Producer.Flush(TimeSpan.FromMilliseconds(TimeForProduceFlush));
                Producer.Dispose();
            }

            _disposed = true;
        }
    }
}
