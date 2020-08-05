using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Forebag.Kafka
{
    /// <summary>
    /// Представляет Consumer, построенный на базе BackgroundService для чтения нескольких топиков.
    /// </summary>
    /// <remarks> 
    /// Сообщения полученые из заданного топика десериализуются из JSON.
    /// Конфигурация компонента производится из класса наследника.
    /// </remarks>
    public abstract class MultipleTopicConsumer<T> : BaseConsumer<T>
    {
        /// <inheritdoc/>
        protected MultipleTopicConsumer(ILogger<MultipleTopicConsumer<T>> logger)
            : base(logger) { }

        /// <summary>
        /// Создание конфигурации для консьюмера.
        /// </summary>
        /// <returns>Объект конфигурации.</returns>
        protected abstract MultipleTopicConsumerOptions BuildOptions();

        /// <inheritdoc/>
        protected sealed override (ConsumerConfig?, string[]?, IDeserializer<T>) BuildParameters()
        {
            var config = BuildOptions();

            return (config, config.TopicsForConsume, new JsonDeserializer<T>());
        }
    }
}
