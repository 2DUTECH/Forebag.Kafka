using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Forebag.Kafka
{
    /// <inheritdoc/>
    public abstract class BaseConsumer<T> : BackgroundService
    {
        private readonly ILogger<BaseConsumer<T>> _logger;
        private IConsumer<string, string>? _consumer;
        private string[]? _topicsForSubsciption;
        private IDeserializer<T>? _deserializer;
        private readonly CancellationTokenSource _stoppingCts = new CancellationTokenSource();
        private SemaphoreSlim _consumerStopedSignal = new SemaphoreSlim(0);

        /// <inheritdoc/>
        protected BaseConsumer(ILogger<BaseConsumer<T>> logger) => _logger = logger;

        /// <inheritdoc/>
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
            else if (parameters.Serializer == null)
            {
                throw new NullReferenceException($"The {nameof(parameters.Serializer)} wasn't initialized.");
            }

            _deserializer = parameters.Serializer;

            _topicsForSubsciption = parameters.TopicsForRead;

            _consumer = new ConsumerBuilder<string, string>(parameters.ConsumerConfig).Build();

            _logger.LogInfoStartConsuming(_consumer);

            return base.StartAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Signal cancellation to the executing method
                _stoppingCts.Cancel();
            }
            finally
            {
                // Wait until the task completes
                await Task.WhenAny(_consumerStopedSignal.WaitAsync(), Task.Delay(Timeout.Infinite));
            }

            await base.StopAsync(cancellationToken);
        }

        /// <inheritdoc/>
        protected abstract (ConsumerConfig? ConsumerConfig, string[]? TopicsForRead, IDeserializer<T> Serializer) BuildParameters();

        /// <inheritdoc/>
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            async Task Consume()
            {
                using var consumerCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _stoppingCts.Token);

                _consumer!.Subscribe(_topicsForSubsciption!);

                try
                {
                    while (!consumerCts.IsCancellationRequested)
                    {
                        var consumeResult = _consumer.Consume(consumerCts.Token);

                        try
                        {
                            var deserializedValue = _deserializer!.Deserialize(consumeResult.Value);

                            _logger.LogConsume(_consumer, consumeResult.Key, consumeResult.Value, consumeResult.TopicPartitionOffset);

                            await ProcessMessage(consumeResult.Key, deserializedValue, consumeResult.TopicPartitionOffset);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogProcessMessageError(_consumer, consumeResult.Key, consumeResult.Value, consumeResult.TopicPartitionOffset, ex);
                        }

                        Commit(consumeResult.TopicPartitionOffset);
                    }
                }
                catch (OperationCanceledException ex)
                {
                    _logger.LogWarning(ex, $"The consumer was stopped by cancellation token.");
                }
                catch (Exception ex)
                {
                    _logger.LogConsumeError(_consumer, ex);
                }
                finally
                {
                    _consumer.Unsubscribe();
                }

                _consumerStopedSignal.Release();
            }

            return Task.Run(Consume, stoppingToken);
        }

        /// <inheritdoc/>
        public abstract Task ProcessMessage(string key, T value, TopicPartitionOffset offset);

        /// <inheritdoc/>
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
            _consumerStopedSignal?.Dispose();
            _consumer?.Dispose();

            base.Dispose();
        }
    }
}
