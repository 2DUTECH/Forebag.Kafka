using Newtonsoft.Json;

namespace Forebag.Kafka
{
    /// <inheritdoc/>
    public class JsonSerializer<T> : ISerializer<T>
    {
        /// <inheritdoc/>
        public string Serialize(T value) => JsonConvert.SerializeObject(value);
    }
}
