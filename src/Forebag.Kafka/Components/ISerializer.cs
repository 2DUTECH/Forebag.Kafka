namespace Forebag.Kafka
{
    /// <inheritdoc/>
    public interface ISerializer<T>
    {
        /// <inheritdoc/>
        string Serialize(T value);
    }
}
