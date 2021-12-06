using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.SystemTextJson;
using Confluent.Kafka.Admin;

namespace EdwardHsu.Lab.MigrateKafkaEvent
{
    class Program
    {
        private const string eventTopic = "Test";
        private const string consumerGroup = "Test"; 
        static async Task Main(string[] args)
        {
            await SendEvents();
            Console.WriteLine("SendEvents OK!");
            await ReceiveEvents();
            Console.WriteLine("ReceiveEvents OK!");
        }


        static async Task ReceiveEvents()
        {
            List<Task> consumers = new List<Task>();
            for (int i = 0; i < 2; i++)
            {
                consumers.Add(Task.Run(
                    async () =>
                    {
                        var       id               = Guid.NewGuid();
                        using var oldKafkaConsumer = InitConsumer("kafka1:9092");
                        using var newKafkaConsumer = InitConsumer("kafka2:9093");
                        Console.WriteLine($"Start Consume Old Kafka - {id}");
                        await StartConsumeLoop("kafka1:9092", oldKafkaConsumer);
                        Console.WriteLine($"End Consume Old Kafka - {id}");
                        Console.WriteLine($"Start Consume New Kafka - {id}");
                        await StartConsumeLoop("kafka2:9093", newKafkaConsumer);
                        Console.WriteLine($"End Consume New Kafka - {id}");
                    }));
            }

            await Task.WhenAll(consumers);
        }


        static async Task StartConsumeLoop(string server,IConsumer<string, byte[]> consumer)
        { 
            var progressBar = new Dictionary<int, bool>();
            var random      = new Random((int)DateTime.Now.Ticks);
            while (true)
            {
                try
                {
                    var data = consumer.Consume();
                    if (data == null) continue;

                    if (data.IsPartitionEOF)
                    {
                        var addAssignments = consumer.Assignment.Where(x => !x.Partition.IsSpecial)
                                                  .Select(x => x.Partition.Value)
                                                  .Where(x=>!progressBar.ContainsKey(x)).ToArray();
                        var removeAssignments = consumer.Assignment.Where(x => !x.Partition.IsSpecial)
                                                     .Select(x => x.Partition.Value)
                                                     .Where(x => !progressBar.ContainsKey(x)).ToArray();
                        foreach (var pId in addAssignments)
                        {
                            progressBar[pId] = false;
                        }
                        foreach (var pId in removeAssignments)
                        {
                            progressBar.Remove(pId);
                        }
                        progressBar[data.Partition.Value] = true;
                        if(progressBar.All(x=>x.Value))break;
                    }
                    else
                    {
                        Console.WriteLine(
                            $"Receive: ({data.Partition.Value}) {data.Message.ToCloudEvent(new JsonEventFormatter()).Data} - Instance: {consumer.GetHashCode()}");
                        Thread.Sleep(random.Next(100, 500));

                        consumer.Commit(data);
                    }
                }
                catch(Exception e)
                {
                    Console.WriteLine($"Consume Error: {e.ToString()}");
                }
            }
        }

        static IConsumer<string, byte[]> InitConsumer(string server)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = server,
                GroupId          = consumerGroup,
                AutoOffsetReset  = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                EnablePartitionEof = true
            };

            var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
            consumer.Subscribe(eventTopic);
            return consumer;
        }



        static async Task SendEvents()
        {
            using var oldKafkaProducer = InitProducer("kafka1:9092");
            using var newKafkaProducer = InitProducer("kafka2:9093");

            await CreateTestTopic("kafka1:9092",4);
            await CreateTestTopic("kafka2:9093",4);

            for (int i = 1; i <= 30; i++)
            {
                await oldKafkaProducer.ProduceAsync(
                    eventTopic,
                    CreateTestEvent(i).ToKafkaMessage(
                        ContentMode.Structured, new JsonEventFormatter()));
            }

            for (int i = 31; i <= 60; i++)
            {
                await newKafkaProducer.ProduceAsync(
                    eventTopic,
                    CreateTestEvent(i).ToKafkaMessage(
                        ContentMode.Structured, new JsonEventFormatter()));
            }
        }


        static async Task<IAdminClient> CreateAdminClient(string server)
        {
            return new AdminClientBuilder(new AdminClientConfig {BootstrapServers = server}).Build();
        }
        static async Task CreateTestTopic(string server, int numPartitions = 4)
        {
            using (var adminClient = await CreateAdminClient(server))
            {
                try
                {
                    try
                    {
                        await adminClient.DeleteTopicsAsync(new string[] {eventTopic});
                    }catch{}

                    await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = eventTopic, ReplicationFactor = 1, NumPartitions = numPartitions } });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }

        static CloudEvent CreateTestEvent(int id)
        {
            var result = new CloudEvent();
            result.Source          = new Uri("https://edward-hsu.net/events/test");
            result.Type            = "testEvent";
            result.Id              = id.ToString();
            result.DataContentType = "application/json";
            result.Data            = $"{{\"id\": {id}}}";
            result.SetPartitionKey(id.ToString());
            return result;
        }

        static IProducer<string, byte[]> InitProducer(string server)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = server,
                ClientId         = Dns.GetHostName(),
                Partitioner      = Partitioner.Consistent
            };

            return new ProducerBuilder<string, byte[]>(config).Build();
        }
    }
}
