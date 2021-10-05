using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;

namespace RedisWeb.Workers
{
    public class WorkerConfiguration
    {
        public int WorkersCount { get; set; }
        public TimeSpan Interval { get; set; }
    }
    
    public class TimeWorker : IHostedService
    {
        private readonly IRedisClient _redis;
        
        private readonly RedLockFactory _lock;

        private readonly WorkerConfiguration _configuration = new()
        {
            WorkersCount = 1,
            Interval = TimeSpan.FromSeconds(30)
        };
        
        
        private Task[] _tasks = null!;
        private readonly CancellationTokenSource _ctSource = new();

        public TimeWorker(IRedisClient redis)
        {
            _redis = redis;
            
            _lock = RedLockFactory.Create(new List<RedLockMultiplexer>
            {
                _redis.ConnectionMultiplexer
            });
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _tasks = new Task[_configuration.WorkersCount];
            for (var i = 0; i < _configuration.WorkersCount; i++)
            {
                _tasks[i] = Run(_ctSource.Token);
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            try
            {
                _ctSource.Cancel();
                return Task.WhenAll(_tasks);
            }
            catch (Exception)
            {
                // ignored
            }

            return Task.CompletedTask;
        }

        private async Task Run(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Work();
                }
                catch (Exception)
                {
                    // ignored
                }

                await Task.Delay(_configuration.Interval, cancellationToken);
            }
        }


        public async Task Work()
        {
            while (true)
            {
                var message = await _redis.GetMessageAsync();
                var expiry = TimeSpan.FromMinutes(1);
            
                if (message is null)
                    return;
                
                if (message.PublicationTime > DateTime.Now)
                    return;

                await using var redLock = await _lock.CreateLockAsync(message.Content, expiry);
            
                if (redLock.IsAcquired)
                {
                    Console.WriteLine(message.Content);

                    await _redis.DeleteMessage();
                }
            }
        }
    }
}