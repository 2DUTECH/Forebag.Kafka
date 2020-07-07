using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Forebag.Kafka
{
    /// <summary>
    /// Представляет Consumer, построенный на базе BackgroundService для чтения одного топика.
    /// </summary>
    /// <remarks> 
    /// Сообщения полученые из заданного топика десериализуются из JSON.
    /// Конфигурация компонента производится из класса наследника.
    /// </remarks>
    public abstract class SingleTopicConsumer<T> : BaseConsumer<T>
    {
        protected SingleTopicConsumer(ILogger<SingleTopicConsumer<T>> logger)
            : base(logger) { }

        /// <summary>
        /// Создание конфигурации для консьюмера.
        /// </summary>
        /// <returns>Объект конфигурации.</returns>
        protected abstract SingleTopicConsumerOptions BuildOptions();

        protected sealed override (ConsumerConfig?, string[]?) BuildParameters()
        {
            var config = BuildOptions();

            return (config, config.TopicForConsume != null ? new[] { config.TopicForConsume } : null);
        }
    }
}
