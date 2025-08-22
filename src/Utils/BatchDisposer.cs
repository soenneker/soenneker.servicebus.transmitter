using Azure.Messaging.ServiceBus;
using System;
using System.Threading.Tasks;

namespace Soenneker.ServiceBus.Transmitter.Utils;

/// <summary>
/// Small helper to ensure the current batch is disposed properly and allow swapping batches
/// without extra allocations or confusing control flow.
/// </summary>
internal sealed class BatchDisposer : IAsyncDisposable, IDisposable
{
    private ServiceBusMessageBatch? _batch;

    public ServiceBusMessageBatch? Batch
    {
        get => _batch;
        set => _batch = value;
    }

    public void Replace(ServiceBusMessageBatch? newBatch)
    {
        if (newBatch == null)
        {
            throw new ArgumentNullException(nameof(newBatch), "Cannot replace batch with null");
        }
        
        _batch?.Dispose();
        _batch = newBatch;
    }

    public ValueTask DisposeAsync()
    {
        _batch?.Dispose();
        _batch = null;
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        _batch?.Dispose();
        _batch = null;
    }
}