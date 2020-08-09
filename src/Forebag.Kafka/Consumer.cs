using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Forebag.Kafka
{
    /// <summary>
    ///     Wrapper for <see href="Confluent.Kafka.IConsumer"/>.
    /// </summary>
    /// <remarks>
    ///     All messages which is received from topics deserialize from JSON string. 
    /// </remarks>
    public abstract class Consumer<T> : BackgroundService
    {
        private readonly ConsumerOptions _options;
        private readonly ILogger<Consumer<T>> _logger;
        private IConsumer<string, string>? _consumer;
        private readonly CancellationTokenSource _internalCancellationTokenSource = new CancellationTokenSource();
        private SemaphoreSlim _consumerStopedSignal = new SemaphoreSlim(0);

        /// <inheritdoc/>
        protected Consumer(IOptions<ConsumerOptions> options, ILogger<Consumer<T>> logger)
        {
            _options = options.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            if (_options.TopicsForConsume == null || !_options.TopicsForConsume.Any())
            {
                throw new NullReferenceException($"Collection for subscribable topics wasn't initialized.");
            }

            _consumer = new ConsumerBuilder<string, string>(_options).Build();

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
        protected override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            async Task Consume()
            {
                using var consumerCts =
                    CancellationTokenSource.CreateLinkedTokenSource(
                        cancellationToken,
                        _internalCancellationTokenSource.Token);

                _consumer!.Subscribe(_options.TopicsForConsume!);

                try
                {
                    while (!consumerCts.IsCancellationRequested)
                    {
                        ConsumeResult<string, string> consumeResult = null!;

                        try
                        {
                            consumeResult = _consumer.Consume(consumerCts.Token);

                            var deserializedValue = JsonConvert.DeserializeObject<T>(consumeResult.Message.Value);

                            _logger.LogConsume(
                                _consumer,
                                consumeResult.Message.Key,
                                consumeResult.Message.Value,
                                consumeResult.TopicPartitionOffset);

                            await ProcessMessage(
                                consumeResult.Message.Key,
                                deserializedValue,
                                consumeResult.TopicPartitionOffset);

                            Commit(consumeResult.TopicPartitionOffset);
                        }
                        catch (ConsumeException ex)
                        {
                            if (consumeResult == null)
                                _logger.LogConsumeError(_consumer, ex);
                            else
                                _logger.LogProcessMessageError(
                                    _consumer,
                                    consumeResult.Message.Key,
                                    consumeResult.Message.Value,
                                    consumeResult.TopicPartitionOffset,
                                    ex);
                        }
                        catch (OperationCanceledException ex)
                        {
                            _logger.LogWarning(ex, $"The consumer was stopped by cancellation token.");
                        }
                        catch (Exception)
                        {
                            throw;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogConsumeError(_consumer, ex);
                }
                finally
                {
                    _consumer.Close();
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
