using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Forebag.Kafka.IntegrationTests
{
    public class BaseFixture : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly IHost _host;
        private IConfiguration? _configuration;
        protected readonly Lazy<IServiceProvider> ServiceProvider;
        private Task StartHostTask;

        public BaseFixture(ITestOutputHelper output)
        {
            ServiceProvider = new Lazy<IServiceProvider>(() => _host.Services);
            _output = output;
            _host = CreateHostBuilder().Build();

            var lifetime = _host.Services.GetRequiredService<IHostApplicationLifetime>();

            lifetime.ApplicationStarted.Register(() =>
            {
                _output.WriteLine("Host started.");
            });

            lifetime.ApplicationStopping.Register(() =>
            {
                _output.WriteLine("Host stopping firing.");
            });

            lifetime.ApplicationStopped.Register(() =>
            {
                _output.WriteLine("Host stopped firing.");
            });

            StartHostTask = _host.StartAsync();
        }

        public IHostBuilder CreateHostBuilder(params string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((hostContext, configuration) =>
                {
                    configuration
                        .AddJsonFile("appsettings.json")
                        .AddJsonFile($"appsettings.{hostContext.HostingEnvironment.EnvironmentName}.json", optional: true);

                    _configuration = configuration.Build();
                })
                .ConfigureLogging((hostContext, logging) =>
                {
                    logging
                        .ClearProviders()
                        .AddXUnit(_output);
                })
                .ConfigureServices((hostContext, services) =>
                {
                    // Logging:
                    services.AddLogging();

                    // Options:
                    services.Configure<SingleTopicProducerConfig>(
                        _configuration!.GetSection(nameof(SingleTopicProducer)));

                    services.Configure<MultipleTopicProducerConfig>(
                        _configuration!.GetSection(nameof(MultipleTopicProducer)));

                    services.Configure<SingleTopicConsumerOptions>(
                        _configuration!.GetSection(nameof(SingleTopicConsumer)));

                    services.Configure<MultipleTopicConsumerOptions>(
                        _configuration!.GetSection(nameof(MultipleTopicConsumer)));

                    services.Configure<StringTypedConsumerOptions>(
                        _configuration!.GetSection(nameof(StringTypedConsumer)));

                    services.Configure<TestConsumerBufferConfig>(
                        _configuration!.GetSection(nameof(TestConsumerBuffer)));

                    // Services: 
                    services.AddSingleton<ISerializer<TestKafkaMessage>>(
                        (provider) => new JsonSerializer<TestKafkaMessage>());

                    services.AddSingleton<TestConsumerBuffer>();
                    services.AddSingleton<SingleTopicProducer>();
                    services.AddSingleton<MultipleTopicProducer>();

                    services.AddHostedService<MultipleTopicConsumer>();
                    services.AddHostedService<SingleTopicConsumer>();
                    services.AddHostedService<StringTypedConsumer>();
                });

        public void Dispose()
        {
            try
            {
                var result = Task.WhenAny(
                    _host.StopAsync(),
                    Task.Delay(Timeout.Infinite));

                result.Wait();
            }
            catch (Exception ex)
            {
                var a = ex;
            }
            finally
            {
                _host?.Dispose();
            }
        }
    }
}
