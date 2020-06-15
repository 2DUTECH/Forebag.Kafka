using System;

namespace Forebag.Kafka
{
    /// <summary>
    /// Представляет стандартное сообщение об ошибке обработки объекта.
    /// </summary>
    public class ProcessMessageError
    {
        public Type? ExceptionType { get; set; }

        public string? Message { get; set; }
    }
}
