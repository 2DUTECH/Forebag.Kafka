using Newtonsoft.Json;

namespace Forebag.Kafka
{
    /// <inheritdoc/>
    public class JsonDeserializer<T> : IDeserializer<T>
    {
        /// <inheritdoc/>
        public T Deserialize(string value) => JsonConvert.DeserializeObject<T>(value);
    }
}
