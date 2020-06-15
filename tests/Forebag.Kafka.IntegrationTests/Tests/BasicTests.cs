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
        public async Task ProduceConsumeMessagesInSequenceFromDifferentProducersToDifferentConsumersWithCommits()
        {
            using var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(30));

            try
            {
                await ProcessOneMessage("first", cancellationToken);

                await ProcessOneMessage("second", cancellationToken);

                var messageBuffer = ServiceProvider.Value.GetRequiredService<TestConsumerBuffer>();

                Assert.Equal(2, messageBuffer.GetTopicByName(nameof(TestConsumerBufferConfig.TopicA)).Messages.Count);
                Assert.Equal(2, messageBuffer.GetTopicByName(nameof(TestConsumerBufferConfig.TopicB1)).Messages.Count);
                Assert.Equal(2, messageBuffer.GetTopicByName(nameof(TestConsumerBufferConfig.TopicB2)).Messages.Count);
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
            var singleTopicProducer = scope.ServiceProvider.GetRequiredService<SingleTopicProducer>();
            var multipleTopicProducer = scope.ServiceProvider.GetRequiredService<MultipleTopicProducer>();

            await singleTopicProducer.Produce(testKey, testMessage);
            await multipleTopicProducer.Produce(testKey, testMessage);

            var consumeResultA = await messageBuffer.TryConsumeFromA(testKey, cancellationToken);

            Assert.NotNull(consumeResultA);
            Assert.Equal(testMessage.Message, consumeResultA.Message);

            var consumeResultB1 = await messageBuffer.TryConsumeFromB1(testKey, cancellationToken);

            Assert.NotNull(consumeResultB1);
            Assert.Equal(testMessage.Message, consumeResultB1.Message);

            var consumeResultB2 = await messageBuffer.TryConsumeFromB2(testKey, cancellationToken);

            Assert.NotNull(consumeResultB2);
            Assert.Equal(testMessage.Message, consumeResultB2.Message);
        }
    }
}
