namespace Forebag.Kafka.IntegrationTests
{
    public class MultipleTopicProducerOptions : ProducerOptions
    {
        public string[]? TopicsForMultipleTopicConsumer { get; set; }
    }
}
