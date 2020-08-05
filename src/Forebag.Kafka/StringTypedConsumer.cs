using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Forebag.Kafka
{
    /// <summary>
    /// Представляет Consumer, построенный на базе BackgroundService для чтения нескольких топиков.
    /// </summary>
    /// <remarks> 
    /// Сообщения полученые из заданных топиков приходят в виде JSON.
    /// Конфигурация компонента производится из класса наследника.
    /// </remarks>
    public abstract class StringTypedConsumer : BaseConsumer<string>
    {
        /// <inheritdoc/>
        protected StringTypedConsumer(ILogger<StringTypedConsumer> logger)
            : base(logger) { }

        /// <summary>
        /// Создание конфигурации для консьюмера.
        /// </summary>
        /// <returns>Объект конфигурации.</returns>
        protected abstract StringTypedConsumerOptions BuildOptions();

        /// <inheritdoc/>
        protected sealed override (ConsumerConfig?, string[]?, IDeserializer<string>) BuildParameters()
        {
            var config = BuildOptions();

            return (config, config.TopicsForConsume, new NoneDeserializer());
        }
    }
}
