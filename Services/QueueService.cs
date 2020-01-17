using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Services
{
    public class QueueService
    {
        private readonly string accessKey;
        private readonly string secretKey;
        private readonly string queueName;
        private readonly RegionEndpoint region;
        private readonly bool isFIFO;
        private readonly NotificationService notificationService;

        public QueueService(string accessKey, string secretKey, string queueName, string region)
        {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.queueName = queueName;
            this.isFIFO = queueName.EndsWith("fifo");
            this.region = RegionEndpoint.GetBySystemName(region);
            this.notificationService = new NotificationService(accessKey, secretKey, region);
        }

        /// <summary>
        /// Ensure sqs queue creation
        /// </summary>
        /// <param name="snsTopicArn">Subscribe to a sns topic</param>
        /// <returns></returns>
        public async Task EnsureQueueCreationAsync(string snsTopicArn = null)
        {
            // an integer representing seconds, from 60 (1 minute) to 1,209,600 (14 days). Default: 345,600 (4 days). 
            var messageRetentionPeriod = "1209600";

            using (var client = new Amazon.SQS.AmazonSQSClient(accessKey, secretKey, region))
            {
                // dead letter queue
                var req = new CreateQueueRequest("dead-" + queueName);
                req.Attributes.Add(QueueAttributeName.MessageRetentionPeriod, messageRetentionPeriod);
                if (isFIFO)
                {
                    req.Attributes.Add(QueueAttributeName.FifoQueue, "true");
                }

                var r = await client.CreateQueueAsync(req);

                string deadQueueArn = r.QueueUrl
                    .Replace("https://sqs.", "arn:aws:sqs:")
                    .Replace(".amazonaws.com", "")
                    .Replace("/", ":");

                // queue
                req = new CreateQueueRequest(queueName);
                req.Attributes.Add(QueueAttributeName.MessageRetentionPeriod, messageRetentionPeriod);
                req.Attributes.Add(QueueAttributeName.RedrivePolicy, "{\"maxReceiveCount\":3, \"deadLetterTargetArn\": \"" + deadQueueArn + "\"}");
                if (isFIFO)
                {
                    req.Attributes.Add(QueueAttributeName.FifoQueue, "true");
                }

                r = await client.CreateQueueAsync(req);

                // subscribe to sns topic
                if (!string.IsNullOrEmpty(snsTopicArn))
                {
                    await notificationService.SubscribeSQS(snsTopicArn, client, r.QueueUrl);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task SendBatchAsync(string[] messages, CancellationToken token)
        {
            Console.WriteLine(string.Format("Sending {0} messages: ", messages.Length));

            using (var client = new Amazon.SQS.AmazonSQSClient(accessKey, secretKey, region))
            {
                var queueUrlResult = await client.GetQueueUrlAsync(queueName, token);
                var queueUrl = queueUrlResult.QueueUrl;

                var entries = new List<SendMessageBatchRequestEntry>();
                foreach (var message in messages)
                {
                    var req = new SendMessageBatchRequestEntry(Guid.NewGuid().ToString(), message);
                    if (isFIFO)
                    {
                        // required
                        req.MessageGroupId = "group"; // fifo atua sobre o grupo
                        req.MessageDeduplicationId = Guid.NewGuid().ToString();
                    }

                    entries.Add(req);
                }

                var batch = new SendMessageBatchRequest(queueUrl, entries);
                var r = await client.SendMessageBatchAsync(batch);

                if (r.Failed != null && r.Failed.Count > 0)
                {
                    foreach (var f in r.Failed)
                    {
                        Console.WriteLine("Failed : ", f.Message);
                    }
                }
            }
        }

        public async Task SendAsync(string message, CancellationToken token)
        {
            // Console.WriteLine("Sending messages:");

            using (var client = new Amazon.SQS.AmazonSQSClient(accessKey, secretKey, region))
            {
                var queueUrlResult = await client.GetQueueUrlAsync(queueName, token);
                var queueUrl = queueUrlResult.QueueUrl;

                var req = new SendMessageRequest(queueUrl, message);
                if (isFIFO)
                {
                    // required
                    req.MessageGroupId = "group";
                    req.MessageDeduplicationId = message;
                }
                req.MessageAttributes.Add("CorrelationID", new MessageAttributeValue()
                {
                    DataType = "string",
                    StringValue = Guid.NewGuid().ToString()
                });
                //req.DelaySeconds = 10;

                var r = await client.SendMessageAsync(req);

                Console.WriteLine("[" + DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss.fff") + "] [P] " + message + " | " + r.MessageId);
            }
        }

        public async Task ConsumeAsync(int maxMessages = 5, CancellationToken token = default)
        {
            Console.WriteLine("\nConsuming messages:");

            using (var client = new Amazon.SQS.AmazonSQSClient(accessKey, secretKey, region))
            {
                var queueUrlResult = await client.GetQueueUrlAsync(queueName, token);
                var queueUrl = queueUrlResult.QueueUrl;

                while (true)
                {
                    /// Console.WriteLine("Fetching messages:");

                    var req = new ReceiveMessageRequest(queueUrl);
                    req.WaitTimeSeconds = 20; // long pooling
                    req.MaxNumberOfMessages = maxMessages;

                    var r = await client.ReceiveMessageAsync(req, token);

                    if (r.Messages.Count > 0)
                    {
                        foreach (var m in r.Messages)
                        {
                            Console.WriteLine("[" + DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss.fff") + "] [C] " + m.Body + " | " + m.MessageId);

                            // ack
                            await client.DeleteMessageAsync(queueUrl, m.ReceiptHandle);
                        }
                    }
                }

            }
        }
    }
}
