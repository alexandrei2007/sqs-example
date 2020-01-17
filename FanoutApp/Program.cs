using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace FanoutApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("[Fanout]");

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

            // configuration
            var accessKey = configuration.GetSection("sns").GetSection("accessKey").Value;
            var secretKey = configuration.GetSection("sns").GetSection("secretKey").Value;
            var region = configuration.GetSection("sns").GetSection("region").Value;
            var topicName = configuration.GetSection("sns").GetSection("topicName").Value;

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                var service = new Services.NotificationService(accessKey, secretKey, region);
                await service.CreateTopicAsync(topicName, cts.Token);

                while (true)
                {
                    Console.Write("Send message: ");
                    var message = Console.ReadLine();

                    if (!string.IsNullOrEmpty(message))
                    {
                        await service.SendAsync(topicName, message, cts.Token);
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
