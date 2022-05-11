using EasyNetQ;
using EasyNetQ.Consumer;
using EasyNetQ.Management.Client;
using EasyNetQ.Topology;
using RabbitMqRateLimiting.EasyNetQ;
using RabbitMqRateLimiting.EasyNetQ.Worker;

var bus = RabbitHutch.CreateBus("amqp://localhost/");
var managementClient = new ManagementClient("localhost", "guest", "guest");

var rootCts = new CancellationTokenSource();
var rootTask = Task.Factory.StartNew(async () =>
    {
        var queueListeners = new Dictionary<string, (Task, CancellationTokenSource)>();

        while (true)
        {
            var queues = await managementClient.GetQueuesAsync();
            var notListeningTo = queues
                .Select(q => q.Name)
                .Except(queueListeners.Keys)
                .Where(q => q.StartsWith("Messages_"))
                .ToArray();

            foreach (var queueName in notListeningTo)
            {
                var cts = new CancellationTokenSource();
                var consumerTask = Task.Run(() => ConsumingFunc(queueName, cts.Token), cts.Token);
                queueListeners.Add(queueName, (consumerTask, cts));
            }

            // Hack to avoid exception on cancellation
            await Task.Delay(5000, rootCts.Token).ContinueWith(_ => { });
            if (rootCts.IsCancellationRequested)
            {
                Console.WriteLine("Destroying listeners");
                foreach (var taskCts in queueListeners.Values)
                {
                    taskCts.Item2.Cancel();
                }

                await Task.WhenAll(queueListeners.Values.Select(tc => tc.Item1).ToArray());
                return;
            }
        }
    });

Console.ReadLine();
Console.WriteLine("Terminating");

rootCts.Cancel();
await rootTask.WaitAsync(TimeSpan.FromSeconds(10));

async Task ConsumingFunc(string queueName, CancellationToken cancellationToken)
{
    var queue = await bus.Advanced.QueueDeclareAsync(queueName);
    using var consumer = bus.Advanced.CreatePullingConsumer<string>(queue, false);

    var bucketConfiguration = new BucketConfiguration()
    {
        Capacity = 1,
        CapacityResetInterval = TimeSpan.FromSeconds(1)
    };

    var bucket = new LeakyBucket<PullResult<string>>(
        async pullResult =>
        {
            Console.WriteLine($"{pullResult.ReceivedInfo.Queue}: {pullResult.Message.Body}");
            await consumer.AckAsync(pullResult.ReceivedInfo.DeliveryTag);
        },
        bucketConfiguration);

    while (true)
    {
        if (!cancellationToken.IsCancellationRequested)
        {
            if (bucket.IsFull)
            {
                await Task.Delay(bucketConfiguration.CapacityResetInterval / 2, cancellationToken).ContinueWith(t => { });
            }

            var pullResult = await consumer.PullAsync();
            if (pullResult.IsAvailable)
            {
                var putResult = await bucket.AttemptPutAsync(pullResult);
                if (!putResult)
                {
                    await consumer.RejectAsync(pullResult.ReceivedInfo.DeliveryTag, true);
                    await Task.Delay(bucketConfiguration.CapacityResetInterval / 2, cancellationToken).ContinueWith(t => { });
                }
            }
            else
            {
                if (pullResult.MessagesCount == 0)
                {
                    await Task.Delay(1000, cancellationToken).ContinueWith(t => { });
                }
            }
        }
        else
        {
            Console.WriteLine("Terminating consumer ");
            return;
        }
    }
}
