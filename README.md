# AWS SQS Example

SQS example with dotnet core.

## Settings

Each project has a `appsettings.json` file.

Just inform your AWS credentials.

To test a FIFO queue, the name must end in `.fifo` e.g. 'test-01.fifo'

```
{
  "sqs": {
    "accessKey": "",
    "secretKey": "",
    "region": "us-east-1",
    "queueName": "test-01"
  }
}
```

In the `ProducerApp` project you can configure to send 10 messages in batch. In the settings you need to set `batch` to `true`.

## Fanout (Publish / Subscribe)

Then `FanoutApp` project will create a SNS topic and deliver messages to it.

In the `ConsumerApp` you can add the topicArn to create a subscription to the consumer queue.
