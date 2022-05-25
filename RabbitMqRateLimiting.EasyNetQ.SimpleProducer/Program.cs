// See https://aka.ms/new-console-template for more information

using EasyNetQ;
using RabbitMqRateLimiting.EasyNetQ.MessagingModel;

var bus = RabbitHutch.CreateBus("amqp://localhost:5672/");

bus.PubSub.PublishAsync(new NewOutboundMessage {Sid = "1"}, "shared.1");
