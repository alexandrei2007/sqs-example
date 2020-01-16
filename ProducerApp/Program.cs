using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ProducerApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("[Producer]");

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

            // configuration
            var accessKey = configuration.GetSection("sqs").GetSection("accessKey").Value;
            var secretKey = configuration.GetSection("sqs").GetSection("secretKey").Value;
            var region = configuration.GetSection("sqs").GetSection("region").Value;
            var queueName = configuration.GetSection("sqs").GetSection("queueName").Value;
            bool batch = false;
            bool.TryParse(configuration.GetSection("sqs").GetSection("batch").Value, out batch);

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                var service = new Services.QueueService(accessKey, secretKey, queueName, region);
                await service.EnsureQueueCreationAsync();

                while(true)
                {
                    Console.Write("Send message: ");
                    var message = Console.ReadLine();

                    if (!string.IsNullOrEmpty(message))
                    {
                        if (batch)
                        {
                            var messages = new string[10];
                            for (int i = 1; i <= 10; i++)
                            {
                                messages[i-1] = message + "-" + i;
                            }

                            await service.SendBatchAsync(messages, cts.Token);
                        } 
                        else
                        {
                            await service.SendAsync(message, cts.Token);
                        }

                    }
                }
                
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Force stop");
            }

            Console.WriteLine("Ended");
        }

    }
}
