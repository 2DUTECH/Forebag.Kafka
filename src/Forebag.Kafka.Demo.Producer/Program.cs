using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Forebag.Kafka.Demo.Producer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var logger = GetLogger();
            using var producer = new KafkaProducer(new Confluent.Kafka.ProducerConfig { BootstrapServers = "localhost:29092" }, logger);

            while (true)
            {
                var value = Console.ReadLine();
                var values = value.Split(" ", StringSplitOptions.RemoveEmptyEntries);

                if (values.Length != 2)
                {
                    continue;
                }

                var command = new CreateUserCommand
                {
                    Name = values[1]
                };

                await producer.Produce(values[0], command, "test_topic");

                Console.ReadKey();
            }
        }

        private static ILogger<KafkaProducer> GetLogger()
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging(c => c.AddConsole());
            var serviceProvider = serviceCollection.BuildServiceProvider();

            return serviceProvider.GetService<ILogger<KafkaProducer>>();
        }
    }
}
