using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using Xunit.Abstractions;

namespace Forebag.Kafka.IntegrationTests
{
    public class BaseFixture : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly IHost _host;
        private IConfiguration? _configuration;

        protected readonly Lazy<IServiceProvider> ServiceProvider;

        public BaseFixture(ITestOutputHelper output)
        {
            ServiceProvider = new Lazy<IServiceProvider>(() => _host.Services);
            _output = output;
            _host = CreateHostBuilder().Build();
            _host.Start();
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

                    services.Configure<SingleTopicConsumerBackgroundServiceConfig>(
                        _configuration!.GetSection(nameof(SingleTopicConsumer)));

                    services.Configure<MultipleTopicConsumerBackgroundServiceConfig>(
                        _configuration!.GetSection(nameof(MultipleTopicConsumer)));

                    services.Configure<StringTypedConsumerBackgroundServiceConfig>(
                        _configuration!.GetSection(nameof(StringTypedConsumer)));

                    services.Configure<TestConsumerBufferConfig>(
                        _configuration!.GetSection(nameof(TestConsumerBuffer)));

                    // Services: 
                    services.AddSingleton<TestConsumerBuffer>();

                    services.AddSingleton(serviceProvider =>
                        new SingleTopicProducer(
                            serviceProvider.GetService<IOptions<SingleTopicProducerConfig>>().Value,
                            serviceProvider.GetService<ILogger<SingleTopicProducer>>()));

                    services.AddSingleton(serviceProvider =>
                        new MultipleTopicProducer(
                            serviceProvider.GetService<IOptions<MultipleTopicProducerConfig>>().Value,
                            serviceProvider.GetService<ILogger<MultipleTopicProducer>>()));

                    services.AddHostedService<SingleTopicConsumer>();
                    services.AddHostedService<MultipleTopicConsumer>();
                    services.AddHostedService<StringTypedConsumer>();
                });

        public void Dispose()
        {
            _host?.StopAsync().Wait();
            _host?.Dispose();
        }
    }
}
