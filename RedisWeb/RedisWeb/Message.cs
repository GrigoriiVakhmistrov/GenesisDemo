using System;

namespace RedisWeb
{
    public class Message
    {
        public DateTime PublicationTime { get; set; }

        public string? Content { get; set; }
    }
}