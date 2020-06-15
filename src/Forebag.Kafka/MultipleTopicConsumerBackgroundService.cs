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
    public abstract class MultipleTopicConsumerBackgroundService<T> : BaseConsumerBackgroundService<T>
    {
        protected MultipleTopicConsumerBackgroundService(ILogger<MultipleTopicConsumerBackgroundService<T>> logger)
            : base(logger) { }

        /// <summary>
        /// Создание конфигурации для консьюмера.
        /// </summary>
        /// <returns>Объект конфигурации.</returns>
        protected abstract MultipleTopicConsumerBackgroundServiceConfig BuildConfig();

        protected sealed override (ConsumerConfig?, string[]?) BuildParameters()
        {
            var config = BuildConfig();

            return (config, config.TopicsForConsume);
        }
    }
}
