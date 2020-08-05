using System;

namespace Forebag.Kafka.IntegrationTests
{
    public class TestKafkaMessage : IEquatable<TestKafkaMessage>
    {
        public string? Message { get; set; }

        bool IEquatable<TestKafkaMessage>.Equals(TestKafkaMessage? other)
        {
            return other != null && other.Message != null && this.Message != null && this.Message.Equals(other.Message);
        }
    }
}
