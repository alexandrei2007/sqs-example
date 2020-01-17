using Amazon;
using Amazon.Runtime.SharedInterfaces;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using System.Threading;
using System.Threading.Tasks;

namespace Services
{
    public class NotificationService
    {
        private readonly string accessKey;
        private readonly string secretKey;
        private readonly RegionEndpoint region;

        public NotificationService(string accessKey, string secretKey, string region)
        {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.region = RegionEndpoint.GetBySystemName(region);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="topicName"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task CreateTopicAsync(string topicName, CancellationToken token = default)
        {
            using (var client = new AmazonSimpleNotificationServiceClient(accessKey, secretKey, region))
            {
                var req = new CreateTopicRequest(topicName);
                req.Tags.Add(new Tag()
                {
                    Key = "Name",
                    Value = "Topic example"
                });

                await client.CreateTopicAsync(req, token);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="topicArn"></param>
        /// <param name="sqsClient"></param>
        /// <param name="sqsQueueUrl"></param>
        public async Task SubscribeSQS(string topicArn, ICoreAmazonSQS sqsClient, string sqsQueueUrl)
        {
            using (var client = new AmazonSimpleNotificationServiceClient(accessKey, secretKey, region))
            {
                var subscriptionArn = await client.SubscribeQueueAsync(topicArn, sqsClient, sqsQueueUrl);
                await client.SetSubscriptionAttributesAsync(subscriptionArn, "RawMessageDelivery", "true");
            }
        }

        /// <summary>
        /// Send message
        /// </summary>
        /// <param name="topicName"></param>
        /// <param name="message"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task SendAsync(string topicName, string message, CancellationToken token)
        {
            using (var client = new AmazonSimpleNotificationServiceClient(accessKey, secretKey, region))
            {
                var topic = await client.FindTopicAsync(topicName);

                var req = new PublishRequest(topic.TopicArn, message);
                await client.PublishAsync(req, token);
            }
        }
    }
}
