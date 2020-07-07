using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Forebag.Kafka
{
    public abstract class BaseConsumer<T> : BackgroundService
    {
        private readonly ILogger<BaseConsumer<T>> _logger;
        private IConsumer<string, string>? _consumer;
        private string[]? _topicsForSubsciption;

        protected BaseConsumer(ILogger<BaseConsumer<T>> logger) => _logger = logger;

        /// <summary>
        /// Метод для инициализации сервиса.
        /// </summary>
        /// <param name="cancellationToken">Токен для отмены работы сервиса.</param>
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            var parameters = BuildParameters();

            if (parameters.ConsumerConfig == null)
            {
                throw new NullReferenceException($"The {nameof(ConsumerConfig)} wasn't initialized.");
            }
            else if (parameters.TopicsForRead == null || !parameters.TopicsForRead.Any())
            {
                throw new NullReferenceException($"Collection for subscribable topics wasn't initialized.");
            }

            _topicsForSubsciption = parameters.TopicsForRead;

            _consumer = new ConsumerBuilder<string, string>(parameters.ConsumerConfig).Build();

            _logger.LogInfoStartConsuming(_consumer);

            return base.StartAsync(cancellationToken);
        }

        /// <summary>
        /// Создание параметров для инициализации консьюмера.
        /// </summary>
        /// <returns>Набор из конфигурации консьюмера и списка топиков для чтения.</returns>
        protected abstract (ConsumerConfig? ConsumerConfig, string[]? TopicsForRead) BuildParameters();

        /// <summary>
        /// Метод запускающий работу консьюмера.
        /// </summary>
        /// <param name="stoppingToken">Токен для прерывания работы.</param>
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(async () =>
            {
                _consumer!.Subscribe(_topicsForSubsciption!);

                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        var consumeResult = _consumer.Consume(stoppingToken);

                        try
                        {
                            var value = JsonConvert.DeserializeObject<T>(consumeResult.Value);

                            _logger.LogConsume(_consumer, consumeResult.Key, consumeResult.Value, consumeResult.TopicPartitionOffset);

                            await ProcessMessage(consumeResult.Key, value, consumeResult.TopicPartitionOffset);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogProcessMessageError(_consumer, consumeResult.Key, consumeResult.Value, consumeResult.TopicPartitionOffset, ex);
                        }

                        Commit(consumeResult.TopicPartitionOffset);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation($"The consumer was stopped by cancellation token.");
                }
                catch (Exception ex)
                {
                    _logger.LogConsumeError(_consumer, ex);
                }
                finally
                {
                    _consumer.Unsubscribe();
                }

            }, stoppingToken);
        }

        /// <summary>
        /// Обработчик полученного сообщения.
        /// </summary>
        /// <param name="key">Ключ сообщения.</param>
        /// <param name="value">Экземпляр сообщения.</param>
        /// <param name="offset">Оффсет полученного сообщения.</param>
        public abstract Task ProcessMessage(string key, T value, TopicPartitionOffset offset);

        /// <summary>
        /// Коммит сообщений для текущего консьюмера.
        /// </summary>
        /// <param name="offset">Оффсеты для коммита.</param>
        private void Commit(TopicPartitionOffset offset)
        {
            try
            {
                _consumer!.Commit(new[] { offset });

                _logger.LogCommit(_consumer, offset);
            }
            catch (Exception ex)
            {
                _logger.LogCommitError(_consumer!, offset, ex);
            }
        }

        public override void Dispose()
        {
            base.Dispose();
            _consumer?.Dispose();
        }
    }
}
