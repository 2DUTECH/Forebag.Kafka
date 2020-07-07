namespace Forebag.Kafka.IntegrationTests
{
    public class SingleTopicProducerConfig : ProducerOptions
    {
        public string? TopicForSingleTopicConsumer { get; set; }
    }
}
