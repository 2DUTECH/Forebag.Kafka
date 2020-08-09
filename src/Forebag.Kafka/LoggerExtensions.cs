using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;

namespace Forebag.Kafka
{
    internal static class LoggerExtensions
    {
        public static void LogInfoStartConsuming(this ILogger logger, IClient client)
            => logger.LogInformation($"The consumer {client.Name} started working.");

        public static void LogInfoStartProducing(this ILogger logger, IClient client)
            => logger.LogInformation($"The producer {client.Name} started working.");

        public static void LogDelivery(this ILogger logger, IClient producer, string key, string value, TopicPartitionOffset offset)
            => logger.LogInformation($"The message delivered: producer={producer.Name}, key={key}, value={value}, offset={offset}.");

        public static void LogDeliveryError(this ILogger logger, IClient producer, string key, string value, string reason)
            => logger.LogError($"Delivery failed: producer={producer.Name}, key={key}, value={value}, reason={reason}.");

        public static void LogConsume(this ILogger logger, IClient consumer, string key, string value, TopicPartitionOffset offset)
            => logger.LogInformation($"The message received: consumer={consumer.Name}, key={key}, value={value}, offset={offset}.");

        public static void LogCommit(this ILogger logger, IClient consumer, TopicPartitionOffset offset)
            => logger.LogInformation($"The message commited: consumer={consumer.Name}, offset={offset}.");

        public static void LogCommitError(this ILogger logger, IClient consumer, TopicPartitionOffset offset, Exception ex)
            => logger.LogError($"Commit failed: consumer={consumer.Name}, offset={offset}, message={ex.Message}.");

        public static void LogProcessMessageError(this ILogger logger, IClient consumer, string key, string value, TopicPartitionOffset offset, Exception ex)
            => logger.LogError(ex, $"An error occured while executing the ProcessMessage method: consumer={consumer.Name}, key={key}, value={value}, offset={offset}, message={ex.Message}.");

        public static void LogConsumeError(this ILogger logger, IClient consumer, Exception ex)
            => logger.LogError(ex, $"An error occured while executing the Consume method: consumer={consumer.Name}, message={ex.Message}.");
    }
}
