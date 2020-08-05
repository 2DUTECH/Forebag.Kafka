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
        private readonly CancellationTokenSource _internalCancellationTokenSource = new CancellationTokenSource();
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
            else if (parameters.Deserializer == null)
            {
                throw new NullReferenceException($"The {nameof(parameters.Deserializer)} wasn't initialized.");
            }

            _deserializer = parameters.Deserializer;

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
                _internalCancellationTokenSource.Cancel();
            }
            finally
            {
                // Wait until the task completes
                await Task.WhenAny(_consumerStopedSignal.WaitAsync(), Task.Delay(Timeout.Infinite));
            }

            await base.StopAsync(cancellationToken);
        }

        /// <inheritdoc/>
        protected abstract (ConsumerConfig? ConsumerConfig, string[]? TopicsForRead, IDeserializer<T> Deserializer) BuildParameters();

        /// <inheritdoc/>
        protected override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            async Task Consume()
            {
                using var consumerCts =
                    CancellationTokenSource.CreateLinkedTokenSource(
                        cancellationToken,
                        _internalCancellationTokenSource.Token);

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

            return Task.Run(Consume, cancellationToken);
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
            _internalCancellationTokenSource.Cancel();
            _internalCancellationTokenSource.Dispose();
            _consumerStopedSignal?.Dispose();
            _consumer?.Dispose();

            base.Dispose();
        }
    }
}
