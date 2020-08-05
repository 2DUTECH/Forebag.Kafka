using Confluent.Kafka;

namespace Forebag.Kafka
{
    /// <inheritdoc/>
    public class NoneSerializer : ISerializer<string>
    {
        /// <inheritdoc/>
        public string Serialize(string value) => value;
    }
}
