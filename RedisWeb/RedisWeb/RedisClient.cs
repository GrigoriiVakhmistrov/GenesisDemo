using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace RedisWeb
{
    public interface IRedisClient
    {
        public ConnectionMultiplexer ConnectionMultiplexer { get; set; }


        public Task SetMessageAsync(Message message);

        public Task<Message?> GetMessageAsync();

        public Task DeleteMessage();
    }

    public class RedisClient : IRedisClient
    {
        private double _postfix = 1_000_000_000;
        public ConnectionMultiplexer ConnectionMultiplexer { get; set; }

        private readonly IDatabase _database;

        private readonly RedisKey _key = new("messages");

        public RedisClient()
        {
            Configuration = new RedisConfiguration
            {
                ConnectionString = "localhost:6379",
                DatabaseNumber = 15
            };

            ConnectionMultiplexer = ConnectionMultiplexer.Connect(Configuration.ConnectionString) ??
                                    throw new InvalidComObjectException("can't connect to redis");

            _database = ConnectionMultiplexer.GetDatabase(Configuration.DatabaseNumber) ??
                        throw new InvalidComObjectException("can't connect to redis db");
        }

        public RedisConfiguration Configuration { get; set; }

        public async Task SetMessageAsync(Message message)
        {
            var value = new RedisValue(message.Content);

            //TTL не указываем - храним сообщение до тех пор, пока не обработаем
            await _database.SortedSetAddAsync(_key, $"{value}:{message.PublicationTime.Ticks.ToString() + _postfix.ToString()}", message.PublicationTime.Ticks);
            
            IncrementPostfix();
        }

        public async Task<Message?> GetMessageAsync()
        {
            var entries = await _database.SortedSetRangeByRankWithScoresAsync(_key, 0, 0);

            if (entries.Length > 0)
            {
                var entry = entries[0];

                var array = entry.Element.ToString().Split(":");
                
                var content = string.Join(":", array.Take(array.Length - 1));
                
                return new Message
                {
                    Content = content,
                    PublicationTime = new DateTime((long)entry.Score)
                };
            }

            return default;
        }

        public async Task DeleteMessage()
        {
            await _database.SortedSetRemoveRangeByRankAsync(_key, 0, 0);
        }

        private void IncrementPostfix()
        {
            _postfix++;
            if (_postfix >= 2_000_000_000)
                _postfix -= 1_000_000_000;
        }
    }

    public class RedisConfiguration
    {
        public string ConnectionString { get; set; } = null!;

        public int DatabaseNumber { get; set; } = -1;
    }
}