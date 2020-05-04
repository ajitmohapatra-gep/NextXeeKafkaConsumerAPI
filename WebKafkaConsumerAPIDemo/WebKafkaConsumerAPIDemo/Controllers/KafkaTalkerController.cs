using System;
using System.Web.Http;
using System.Text;
using KafkaNet;
using KafkaNet.Model;
using System.Collections;

namespace WebKafkaConsumerAPIDemo.Controllers
{
    [RoutePrefix("kafka")]
    public class KafkaTalkerController : ApiController
    {
        [HttpGet]
        [Route("Get")]
        public IHttpActionResult GetFromKafka()
        {
            string topic = "ForecastCollaboration-SubmittedCommit";
            //string message = String.Empty;
            Uri uri = new Uri(@"http://172.20.188.11:9092");
            var options = new KafkaOptions(uri);
            BrokerRouter brokerRouter = new BrokerRouter(options);
            Consumer kafkaConsumer = new Consumer(new ConsumerOptions(topic, brokerRouter));


            ArrayList al = new ArrayList();
            int count = 0;
            //Consume returns a blocking IEnumerable (ie: never ending stream)
            int maxMsg = 3;
            foreach (var message1 in kafkaConsumer.Consume())
            {

                if (message1 == null || count == maxMsg)
                {
                    break;
                }
                /*Console.WriteLine("Response: P{0},O{1} : {2}",
                   message.Meta.PartitionId, message.Meta.Offset,
                   Encoding.UTF8.GetString(message.Value));*/
                al.Add(Encoding.UTF8.GetString(message1.Value));
                count++;
            }
            

            //Console.WriteLine("Total No Msg Count Size {0}", al.Count);
            return Ok(al);
        }
    }
}
