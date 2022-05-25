using System.Collections.Concurrent;

namespace RabbitMqRateLimiting.EasyNetQ.Worker;

class LeakyBucket<T> : IAsyncDisposable
{
    private readonly Func<T, Task> _queueItemHandler;
    private readonly BucketConfiguration _bucketConfiguration;
    private readonly CancellationTokenSource _leakCancellationTokenSource;
    private readonly ConcurrentQueue<T> _currentItems;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private Task? _leakTask;
    private Task? _resetTask;
    private int _remaining;

    public LeakyBucket(Func<T, Task> queueItemHandler, BucketConfiguration bucketConfiguration)
    {
        _queueItemHandler = queueItemHandler;
        _bucketConfiguration = bucketConfiguration;
        _leakCancellationTokenSource = new();
        _currentItems = new();
    }

    public async Task<bool> AttemptPutAsync(T item)
    {
        //Only allow one thread at a time in.
        await _semaphore.WaitAsync();
        try
        {
            //If this is the first time, kick off our thread to monitor the bucket.
            if (_leakTask == null)
            {
                _leakTask = Task.Run(() => Leak(_leakCancellationTokenSource.Token));
            }

            if (_resetTask == null)
            {
                _resetTask = Task.Run(() => Reset(_leakCancellationTokenSource.Token));
            }

            if (_currentItems.Count >= _bucketConfiguration.Capacity)
            {
                return false;
            }

            _currentItems.Enqueue(item);
            return true;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public bool IsFull => _remaining == 0;

    //Infinite loop to keep leaking.
    private async Task Leak(CancellationToken ct)
    {
        //Wait for our first queue item.
        while (_currentItems.Count == 0)
        {
            await Task.Delay(1000, ct).ContinueWith(t => { });
        }

        while (!ct.IsCancellationRequested)
        {
            while (_currentItems.Count > 0 && _remaining > 0 && !ct.IsCancellationRequested)
            {
                var hasItem = _currentItems.TryDequeue(out var dequeueItem);
                if (hasItem)
                {
                    _queueItemHandler(dequeueItem!);
                    Interlocked.Decrement(ref _remaining);
                }
            }
        }
    }

    private async Task Reset(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            Interlocked.Exchange(ref _remaining, _bucketConfiguration.Capacity);
            await Task.Delay(_bucketConfiguration.CapacityResetInterval, ct).ContinueWith(t => { });
        }
    }

    public ValueTask DisposeAsync()
    {
        _leakCancellationTokenSource.Cancel();
        return new(Task.WhenAll(_leakTask ?? Task.CompletedTask, _resetTask ?? Task.CompletedTask));
    }
}
