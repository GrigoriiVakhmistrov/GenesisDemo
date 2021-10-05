using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace RedisWeb.Controllers
{
    [Route("printmeat")]
    public class MessageController : ControllerBase
    {
        private readonly IRedisClient _redis;

        public MessageController(IRedisClient redis)
        {
            _redis = redis;
        }

        [Route(""), HttpGet]
        public async Task<IActionResult> SetMessage(string? message, DateTime? printAt)
        {
            await Task.Yield();

            if (printAt is not null && printAt > DateTime.Now)
            {
                var msg = new Message
                {
                    Content = message,
                    PublicationTime = printAt.Value
                };
                
                await _redis.SetMessageAsync(msg);

                return Ok();
            }

            return BadRequest();
        }
    }
}