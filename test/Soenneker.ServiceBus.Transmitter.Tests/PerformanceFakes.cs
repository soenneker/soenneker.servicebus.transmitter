using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Soenneker.ServiceBus.Message;
using Soenneker.ServiceBus.Message.Abstract;
using Soenneker.ServiceBus.Sender.Abstract;
using Soenneker.ServiceBus.Transmitter;
using Soenneker.Utils.BackgroundQueue;

namespace Audit;

public sealed class Payload : Soenneker.Messages.Base.Message
{
    public string Content { get; set; } = "hello";
    public static Payload Create(string content = "hello") => new()
    {
        Type = "audit.v1", Queue = "audit", Id = "id", Sender = "audit", CreatedAt = DateTimeOffset.UnixEpoch, Content = content
    };
}

public sealed class EnabledLogger<T> : ILogger<T>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel level) => true;
    public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
}

public sealed class FakeSender : ServiceBusSender
{
    public int BatchCapacity { get; set; } = 2;
    public int CreatedBatches;
    public int BatchSends;
    public int SingleSends;
    public int FailBatchCreation;
    public bool CancelCreation;
    public bool FailReplacement;
    public bool Capture = true;
    public List<ServiceBusMessage> Sent { get; } = [];
    public List<ServiceBusMessageBatch> Batches { get; } = [];
    private readonly Dictionary<ServiceBusMessageBatch, List<ServiceBusMessage>> _stores = [];
    public override ValueTask<ServiceBusMessageBatch> CreateMessageBatchAsync(CancellationToken cancellationToken = default)
    {
        CreatedBatches++;
        if (CancelCreation) throw new OperationCanceledException(cancellationToken);
        if (FailReplacement && CreatedBatches > 1) throw new InvalidOperationException("replacement failed");
        if (FailBatchCreation-- > 0) throw new InvalidOperationException("creation failed");
        var store = new List<ServiceBusMessage>();
        var batch = ServiceBusModelFactory.ServiceBusMessageBatch(0, store, tryAddCallback: m =>
            !m.ApplicationProperties.ContainsKey("oversize") && store.Count < BatchCapacity);
        _stores[batch] = store;
        Batches.Add(batch);
        return ValueTask.FromResult(batch);
    }
    public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        SingleSends++;
        if (Capture) Sent.Add(message);
        return Task.CompletedTask;
    }
    public override Task SendMessagesAsync(ServiceBusMessageBatch batch, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        BatchSends++;
        if (Capture) Sent.AddRange(_stores[batch]);
        return Task.CompletedTask;
    }
}

public sealed class FakeSenderUtil(FakeSender sender) : IServiceBusSenderUtil
{
    public int Gets;
    public ValueTask<ServiceBusSender> Get(string queueName, CancellationToken cancellationToken = default)
    {
        Gets++;
        return ValueTask.FromResult<ServiceBusSender>(sender);
    }
    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

public sealed class CountingBuilder(IServiceBusMessageUtil inner) : IServiceBusMessageUtil
{
    public int Calls;
    public bool RejectAll;
    public ServiceBusMessage? BuildMessage<T>(T message, string type) where T : Soenneker.Messages.Base.Message
    {
        Calls++;
        if (RejectAll || message.Id == "reject") return null;
        var built = inner.BuildMessage(message, type);
        if (message.Id == "oversize" && built is not null) built.ApplicationProperties["oversize"] = true;
        return built;
    }
}

public sealed class Fixture
{
    public static IConfiguration Config(bool logging = false, bool counts = true) => new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["Azure:ServiceBus:Enable"] = "true", ["Azure:ServiceBus:TransmitterLogging"] = logging.ToString(),
        ["Background:QueueLength"] = "32", ["Background:LockCounts"] = counts.ToString()
    }).Build();
    public FakeSender Sender { get; } = new();
    public FakeSenderUtil Senders { get; }
    public CountingBuilder Builder { get; }
    public QueueInformationUtil Info { get; }
    public BackgroundQueue Queue { get; }
    public ServiceBusTransmitter Transmitter { get; }
    public Fixture(bool logging = false)
    {
        var config = Config(logging);
        Senders = new(Sender);
        Builder = new(new ServiceBusMessageUtil(TestJsonContext.Default, config, NullLogger<ServiceBusMessageUtil>.Instance));
        Info = new(config);
        Queue = new(config, NullLogger<BackgroundQueue>.Instance, Info);
        Transmitter = new(logging ? new EnabledLogger<ServiceBusTransmitter>() : NullLogger<ServiceBusTransmitter>.Instance, Queue, Builder, Senders, config);
    }
    public async ValueTask DrainOne()
    {
        var item = await Queue.Dequeue();
        try { await item.Invoke(default); }
        finally { await Info.DecrementValueTaskCounter(); }
    }
}
