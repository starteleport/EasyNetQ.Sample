// See https://aka.ms/new-console-template for more information

using EasyNetQ;
using EasyNetQ.Topology;

var bus = RabbitHutch.CreateBus("amqp://localhost:5672/");

var exchange = await bus.Advanced.ExchangeDeclareAsync(
    "Messages",
    c => c.WithType("direct").AsDurable(true));

string queueName;

while ((queueName = Console.ReadLine()!) != string.Empty)
{
    var queue = await bus.Advanced.QueueDeclareAsync(
        "Messages_" + queueName,
        x => x.WithArgument("x-single-active-consumer", true));

    await bus.Advanced.BindAsync(exchange, queue, queueName, CancellationToken.None);

    Console.WriteLine("Messages_" + queueName);

    for (int i = 0; i < 15; i++)
    {
        // Need to handle Returns here
        await bus.Advanced.PublishAsync((IExchange)exchange, queueName, true, new Message<string>(i.ToString()));
    }
}
