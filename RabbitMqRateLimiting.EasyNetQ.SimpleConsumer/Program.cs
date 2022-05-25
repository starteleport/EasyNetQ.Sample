// See https://aka.ms/new-console-template for more information

using EasyNetQ;
using RabbitMqRateLimiting.EasyNetQ.MessagingModel;

var bus = RabbitHutch.CreateBus("amqp://localhost:5672/");

var consumer = await bus.PubSub.SubscribeAsync<NewOutboundMessage>(
    "shared",
    async (message, ct) =>
    {
        Console.WriteLine(message.Sid);
        await Task.Delay(1000, ct);
    },
    s => s.WithTopic("#"));

Console.ReadLine();

consumer.Dispose();
