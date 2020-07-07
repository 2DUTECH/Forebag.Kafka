namespace Forebag.Kafka.IntegrationTests
{
    public class MultipleTopicProducerConfig : ProducerOptions
    {
        public string[]? TopicsForMultipleTopicConsumer { get; set; }
    }
}
