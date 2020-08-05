namespace Forebag.Kafka.IntegrationTests
{
    public class SingleTopicProducerOptions : ProducerOptions
    {
        public string? TopicForSingleTopicConsumer { get; set; }
    }
}
