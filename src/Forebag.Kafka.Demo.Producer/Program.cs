using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading.Tasks;

namespace Forebag.Kafka.Demo.Producer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();

            serviceCollection.AddLogging(c => c.AddConsole());

            ConfigurationBuilder builder = new ConfigurationBuilder();

            builder.AddJsonFile("appsettings.json");

            IConfiguration configuration = builder.Build();

            serviceCollection.Configure<ProducerOptions>(configuration.GetSection(nameof(Producer)));

            serviceCollection.AddSingleton<Kafka.Producer>();

            using var serviceProvider = serviceCollection.BuildServiceProvider();
            using var producer = serviceProvider.GetService<Kafka.Producer>();

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
    }
}
