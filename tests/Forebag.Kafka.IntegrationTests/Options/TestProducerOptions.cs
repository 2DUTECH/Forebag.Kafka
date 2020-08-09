using System.Collections.Generic;

namespace Forebag.Kafka.IntegrationTests
{
    public class TestProducerOptions : ProducerOptions
    {
        public List<string>? TopicsForProduce { get; set; }
    }
}
