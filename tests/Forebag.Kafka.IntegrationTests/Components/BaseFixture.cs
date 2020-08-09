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
        private ILogger<IHost> _logger;
        private readonly IHost _host;
        private IConfiguration? _configuration;
        protected readonly Lazy<IServiceProvider> ServiceProvider;

        public BaseFixture(ITestOutputHelper output)
        {
            ServiceProvider = new Lazy<IServiceProvider>(() => _host.Services);
            _output = output;
            _host = CreateHostBuilder().Build();

            _logger = _host.Services.GetRequiredService<ILogger<IHost>>();
            var lifetime = _host.Services.GetRequiredService<IHostApplicationLifetime>();

            lifetime.ApplicationStarted.Register(() => _logger.LogInformation("Host started."));

            lifetime.ApplicationStopping.Register(() => _logger.LogInformation("Host stopping firing."));

            lifetime.ApplicationStopped.Register(() => _logger.LogInformation("Host stopped firing."));

            _host.StartAsync();
        }

        public IHostBuilder CreateHostBuilder(params string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((hostContext, configuration) =>
                {
                    configuration
                        .AddJsonFile("appsettings.json")
                        .AddJsonFile(
                            $"appsettings.{hostContext.HostingEnvironment.EnvironmentName}.json",
                            optional: true);

                    _configuration = configuration.Build();
                })
                .ConfigureLogging((hostContext, logging) =>
                {
                    logging
                        .ClearProviders()
                        .AddConsole()
                        .AddXUnit(_output);
                })
                .ConfigureServices((hostContext, services) =>
                {
                    // Logging:
                    services.AddLogging();

                    // Options:
                    services.Configure<TestProducerOptions>(
                        _configuration!.GetSection(nameof(TestProducer)));

                    services.Configure<ConsumerOptions>(
                        _configuration!.GetSection(nameof(TestConsumer)));

                    services.Configure<TestConsumerBufferOptions>(
                        _configuration!.GetSection(nameof(TestConsumerBuffer)));

                    // Services: 
                    services.AddSingleton<TestConsumerBuffer>();
                    services.AddSingleton<TestProducer>();

                    services.AddHostedService<TestConsumer>();
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
