using EasyNetQ;

var bus = RabbitHutch.CreateBus("amqp://localhost/");

Console.WriteLine("Enter queue name");
var queueName = Console.ReadLine();

var queue = await bus.Advanced.QueueDeclareAsync(
    "Messages_" + queueName,
    x => x.WithArgument("x-single-active-consumer", true));

using var consumer = bus.Advanced.CreatePullingConsumer<string>(queue, false);

while (true)
{
    Console.WriteLine("ENTER to consume, any other string to exit");
    var command = Console.ReadLine();

    if (command != "")
    {
        break;
    }

    var pullResult = await consumer.PullAsync();
    if (pullResult.IsAvailable)
    {
        Console.WriteLine(pullResult.Message.Body);
        Console.WriteLine("ENTER to ack");
        Console.ReadLine();

        await consumer.AckAsync(pullResult.ReceivedInfo.DeliveryTag);
    }
    else
    {
        Console.WriteLine("No messages");
    }
}
