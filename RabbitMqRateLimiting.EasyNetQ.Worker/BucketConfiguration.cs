namespace RabbitMqRateLimiting.EasyNetQ.Worker;

class BucketConfiguration
{
    public TimeSpan CapacityResetInterval { get; set; }
    public int Capacity { get; set; }
}
