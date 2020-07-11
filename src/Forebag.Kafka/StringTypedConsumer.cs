using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Forebag.Kafka
{
    /// <summary>
    /// Представляет Consumer, построенный на базе BackgroundService для чтения нескольких топиков.
    /// </summary>
    /// <remarks> 
    /// Сообщения полученые из заданных топиков приходят в виде JSON.
    /// Конфигурация компонента производится из класса наследника.
    /// </remarks>
    public abstract class StringTypedConsumer : BackgroundService
    {
        private readonly ILogger<StringTypedConsumer> _logger;
        private IConsumer<string, string>? _consumer;
        private string[]? _topicsForSubsciption;

        /// <inheritdoc/>
        protected StringTypedConsumer(ILogger<StringTypedConsumer> logger) => _logger = logger;

        /// <summary>
        /// Метод для инициализации сервиса.
        /// </summary>
        /// <param name="cancellationToken">Токен для отмены работы сервиса.</param>
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            var options = BuildOptions();

            if (options == null)
            {
                throw new NullReferenceException($"The {nameof(ConsumerConfig)} wasn't initialized.");
            }
            else if (options.TopicsForConsume == null || !options.TopicsForConsume.Any())
            {
                throw new NullReferenceException($"Collection for subscribable topics wasn't initialized.");
            }

            _topicsForSubsciption = options.TopicsForConsume;

            _consumer = new ConsumerBuilder<string, string>(options).Build();

            _logger.LogInfoStartConsuming(_consumer);

            return base.StartAsync(cancellationToken);
        }

        /// <summary>
        /// Создание конфигурации для консьюмера.
        /// </summary>
        /// <returns>Объект конфигурации.</returns>
        protected abstract StringTypedConsumerOptions BuildOptions();

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
                            _logger.LogConsume(_consumer, consumeResult.Key, consumeResult.Value, consumeResult.TopicPartitionOffset);

                            await ProcessMessage(consumeResult.Key, consumeResult.Value, consumeResult.TopicPartitionOffset);
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
        public abstract Task ProcessMessage(string key, string value, TopicPartitionOffset offset);

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

        /// <inheritdoc/>
        public override void Dispose()
        {
            base.Dispose();
            _consumer?.Dispose();
        }
    }
}
