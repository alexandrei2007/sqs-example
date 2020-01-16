using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ConsumerApp
{
    class Program
    {        static async Task Main(string[] args)
        {
            Console.WriteLine("[Consumer]");

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

            // configuration
            var accessKey = configuration.GetSection("sqs").GetSection("accessKey").Value;
            var secretKey = configuration.GetSection("sqs").GetSection("secretKey").Value;
            var region = configuration.GetSection("sqs").GetSection("region").Value;
            var queueName = configuration.GetSection("sqs").GetSection("queueName").Value;

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                var service = new Services.QueueService(accessKey, secretKey, queueName, region);
                await service.EnsureQueueCreationAsync();
                await service.ConsumeAsync(3, cts.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Force stop");
            }

            Console.WriteLine("Ended");

        }

    }
}
