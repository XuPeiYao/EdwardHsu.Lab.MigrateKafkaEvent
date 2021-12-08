using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.SystemTextJson;
using Confluent.Kafka.Admin;
using Partitioner = Confluent.Kafka.Partitioner;

namespace EdwardHsu.Lab.MigrateKafkaEvent
{
    class Program
    {
        private const string eventTopic = "Test";
        private const string consumerGroup = "Test";
        static int count = 0;
        static async Task Main(string[] args)
        {
            await SendEvents();
            Console.WriteLine("SendEvents OK!");
            await ReceiveEvents();
            Console.WriteLine("ReceiveEvents OK!");
            Console.WriteLine(count.ToString());
        }


        static async Task ReceiveEvents()
        {
            List<Task> consumers = new List<Task>();


            // 此處
            var partitionStatus_Old = new ConcurrentDictionary<int, bool>();
            var partitionStatus_New = new ConcurrentDictionary<int, bool>();

            for (int i = 0; i < 2; i++)
            {
                string consumerName = "consumer"+ i.ToString();
                // 這裡每個Task代表一個獨立的Consumer的Job instance
                consumers.Add(Task.Run(
                    async () =>
                    {
                        var       id               = Guid.NewGuid();
                        using var oldKafkaConsumer = InitConsumer("kafka1:9092");
                        using var newKafkaConsumer = InitConsumer("kafka2:9093");
                        Console.WriteLine($"Start Consume Old Kafka - {consumerName}");
                        await StartConsumeLoop("kafka1:9092", oldKafkaConsumer, consumerName+"_OLD", partitionStatus_Old);
                        Console.WriteLine($"End Consume Old Kafka - {id}");
                        Console.WriteLine($"Start Consume New Kafka - {consumerName}");
                        await StartConsumeLoop("kafka2:9093", newKafkaConsumer, consumerName + "_NEW", partitionStatus_New);
                        Console.WriteLine($"End Consume New Kafka - {consumerName}");
                    }));
            }

            await Task.WhenAll(consumers);
        }


        static async Task StartConsumeLoop(string server, IConsumer<string, byte[]> consumer, string consumerName, ConcurrentDictionary<int, bool> partitionStatus = null)
        {
            partitionStatus = partitionStatus ?? new ConcurrentDictionary<int, bool>();


            var adminClient = await CreateAdminClient(server);
            var topicMetadata = adminClient.GetMetadata(eventTopic, TimeSpan.FromSeconds(30)); 
            var allPartitionIds = topicMetadata.Topics.Find(x => x.Topic == eventTopic).Partitions.Select(x => x.PartitionId);


            var random = new Random((int)DateTime.Now.Ticks);
            
            while (true)
            {
                try
                {
                    var data = consumer.Consume();
                    if (data == null) continue;

                    if (data.IsPartitionEOF)
                    {
                        Console.WriteLine($"{consumerName} Assignment:" + string.Join(",", consumer.Assignment.Select(x => x.Partition.Value)));

                        var partitionIds = consumer.Assignment.Where(x => !x.Partition.IsSpecial)
                                                   .Select(x => x.Partition.Value);
                       
                        partitionStatus[data.Partition.Value] = true; //將收到EOF的partition狀態設為已完成

                        // 檢查目前Consumer被分配到的partition是否已經都完成
                        if (partitionIds.Select(x => partitionStatus.ContainsKey(x) && partitionStatus[x]).All(x=>x))
                        {
                            // 若完成則跳出ConsumeLoop
                            break;
                        }
                    }
                    else
                    {
                        var cloudeventData = data.Message.ToCloudEvent(new JsonEventFormatter()).Data.ToString();
                        Console.WriteLine(
                            $"Receive: ({data.Partition.Value}) {cloudeventData} - Instance: {consumerName}");
                        consumer.Commit(data);
                        Thread.Sleep(random.Next(100,500));
                        lock (eventTopic)
                        {
                            count++;
                        }
                    }
                }
                catch(Exception e)
                {
                    Console.WriteLine($"Consume Error: {e.ToString()} - Instance: {consumerName}");
                }
            } 
            consumer.Close();

            Console.WriteLine($"{consumerName} Done");
            // 等候所有Partition完成
            while (true)
            {
                await Task.Delay(1000);
                // 檢查所有partition是否已經都完成
                if (allPartitionIds.Select(x => partitionStatus.ContainsKey(x) && partitionStatus[x]).All(x => x))
                {
                    // 若完成則跳出ConsumeLoop
                    break;
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
                EnablePartitionEof = true // 這個屬性一定要打開
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
            result.Data            = $"{{\"id\":{id}, \"type\":\"{(id<=30?"舊":"新")}\"}}";
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
