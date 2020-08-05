namespace Forebag.Kafka
{
    /// <inheritdoc/>
    public interface IDeserializer<TOut>
    {
        /// <inheritdoc/>
        TOut Deserialize(string value);
    }
}
