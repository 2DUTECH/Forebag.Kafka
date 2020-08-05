namespace Forebag.Kafka
{
    /// <inheritdoc/>
    public interface IDeserializer<T>
    {
        /// <inheritdoc/>
        T Deserialize(string value);
    }
}
