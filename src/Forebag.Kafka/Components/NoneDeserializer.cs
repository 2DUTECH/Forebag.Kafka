namespace Forebag.Kafka
{
    /// <inheritdoc/>
    public class NoneDeserializer : IDeserializer<string>
    {
        /// <inheritdoc/>
        public string Deserialize(string value) => value;
    }
}
