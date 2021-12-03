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
        private const int numPartitions = 4;
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
                        using var oldKafkaConsumer = InitConsumer("kafka1:9092");
                        using var newKafkaConsumer = InitConsumer("kafka2:9093");
                        Console.WriteLine("Start Consume Old Kafka");
                        await StartConsumeLoop("kafka1:9092", oldKafkaConsumer);
                        Console.WriteLine("End Consume Old Kafka");
                        Console.WriteLine("Start Consume New Kafka");
                        await StartConsumeLoop("kafka2:9093", newKafkaConsumer);
                        Console.WriteLine("End Consume New Kafka");
                    }));
            }

            await Task.WhenAll(consumers);
        }


        static async Task StartConsumeLoop(string server,IConsumer<string, byte[]> consumer)
        {
            var adminClient = await CreateAdminClient(server);
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));

            bool loop = true;

            void CheckEOF(bool initCheck)
            {
                try
                {
                    var partitions = metadata.Topics.Find(x => x.Topic == eventTopic).Partitions
                                             .Select(x => new TopicPartition(eventTopic, x.PartitionId));
                    var partitionOffsets = consumer.Committed(partitions, TimeSpan.FromSeconds(30));

                    if (partitionOffsets.Count == 0 && initCheck)
                    {
                        return;
                    }

                    bool hasLag = false;
                    foreach (var partitionOffset in partitionOffsets)
                    {
                        var watermarkOffset = consumer.GetWatermarkOffsets(partitionOffset.TopicPartition);

                        if (watermarkOffset.High.Value != partitionOffset.Offset &&
                            watermarkOffset.High != watermarkOffset.Low)
                        {
                            hasLag = true;
                            break;
                        }
                        else
                        {
                            Console.WriteLine(
                                $"EOF: ({partitionOffset.Partition.Value}) - Instance: {consumer.GetHashCode()}");
                        }
                    }

                    loop = hasLag;
                }
                catch(Exception e)
                {
                    Console.WriteLine($"{e.ToString()}");
                }
            }
            
            
            //CheckEOF(true);
            var random = new Random((int)DateTime.Now.Ticks);
            while (loop)
            {
                try
                {
                    var data = consumer.Consume();
                    if (data == null) continue;

                    if (data.IsPartitionEOF)
                    {
                        CheckEOF(false);
                        if (!loop) break;
                    }
                    else
                    {
                        Console.WriteLine(
                            $"Receive: ({data.Partition.Value}) {data.Message.ToCloudEvent(new JsonEventFormatter()).Data} - Instance: {consumer.GetHashCode()}");
                        Thread.Sleep(random.Next(100, 1000));

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

            await CreateTestTopic("kafka1:9092");
            await CreateTestTopic("kafka2:9093");

            for (int i = 1; i <= 20; i++)
            {
                await oldKafkaProducer.ProduceAsync(
                    eventTopic,
                    CreateTestEvent(i).ToKafkaMessage(
                        ContentMode.Structured, new JsonEventFormatter()));
            }

            for (int i = 21; i <= 40; i++)
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
        static async Task CreateTestTopic(string server)
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

            return result;
        }

        static IProducer<string, byte[]> InitProducer(string server)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = server,
                ClientId         = Dns.GetHostName(),
                Partitioner      = Partitioner.Random
            };

            return new ProducerBuilder<string, byte[]>(config).Build();
        }
    }
}
