using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace kafkatest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        // GET api/values/5
        [HttpGet]
        public ActionResult<string> Get()
        {
            var conf = new ConsumerConfig
            { 
                GroupId = "test-consumer-group",
                BootstrapServers = "confluence-cp-kafka-headless:9092",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // eariest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            using (var c = new Consumer<Ignore, string>(conf))
            {
                c.Subscribe("test-topic");

                bool consuming = true;
                // The client will automatically recover from non-fatal errors. You typically
                // don't need to take any action unless an error is marked as fatal.
                c.OnError += (_, e) => consuming = !e.IsFatal;

                while (consuming)
                {
                    try
                    {
                        var cr = c.Consume();
                        return $"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.";
                    }
                    catch (ConsumeException e)
                    {
                        return $"Error occured: {e.Error.Reason}";
                    }
                }
            
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
            }

            return "nothing found";
        }

        // POST api/values
        [HttpPost]
        public async Task<ActionResult<string>> Post([FromBody] string value)
        {
            var config = new ProducerConfig {BootstrapServers = "confluence-cp-kafka-headless:9092"};

            // A Producer for sending messages with null keys and UTF-8 encoded values.
            using (var p = new Producer<Null, string>(config))
            {
                try
                {
                    var dr = await p.ProduceAsync("test-topic", new Message<Null, string> {Value = value});
                    return $"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'";
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }

            return "something went wrong";
        }
    }
}
