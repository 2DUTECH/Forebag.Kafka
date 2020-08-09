using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Forebag.Kafka.IntegrationTests
{
    public class BasicTests : BaseFixture
    {
        public BasicTests(ITestOutputHelper output) : base(output) { }

        [Fact]
        public async Task ProduceConsumeMessagesInSequence()
        {
            using var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

#if DEBUG
            cancellationTokenSource.CancelAfter(Timeout.InfiniteTimeSpan);
#else
            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(60));
#endif

            try
            {
                await ProcessOneMessage("first", cancellationToken);

                await ProcessOneMessage("second", cancellationToken);

                var messageBuffer = ServiceProvider.Value.GetRequiredService<TestConsumerBuffer>();


                foreach (var topic in messageBuffer.Topics)
                {
                    Assert.Equal(2, messageBuffer.GetTopicByName(topic).Count);
                }
            }
            catch (Exception ex)
            {
                var a = ex;
            }
            finally
            {
                cancellationTokenSource.Cancel(true);
            }
        }

        private async Task ProcessOneMessage(string messagePattern, CancellationToken cancellationToken)
        {
            var testKey = $"testKey-{messagePattern}";
            var testMessage = new TestKafkaMessage { Message = $"testValue-{messagePattern}" };

            var messageBuffer = ServiceProvider.Value.GetRequiredService<TestConsumerBuffer>();

            using var scope = ServiceProvider.Value.CreateScope();
            var singleTopicProducer = scope.ServiceProvider.GetRequiredService<TestProducer>();

            await singleTopicProducer.Produce(testKey, testMessage);

            var consumeResults = messageBuffer.Consume(testKey, cancellationToken);

            foreach (var consumeResult in consumeResults)
            {
                Assert.NotNull(consumeResult);
                Assert.Equal(testMessage.Message, consumeResult.Message);
            }
        }
    }
}
