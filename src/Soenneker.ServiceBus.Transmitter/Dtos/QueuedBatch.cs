using Azure.Messaging.ServiceBus;

namespace Soenneker.ServiceBus.Transmitter.Dtos;

internal sealed class QueuedBatch(ServiceBusTransmitter self, string queue, string typeName, ServiceBusMessage[] messages, int count)
{
    public ServiceBusTransmitter Self { get; } = self;
    public string Queue { get; } = queue;
    public string TypeName { get; } = typeName;
    public ServiceBusMessage[] Messages { get; } = messages;
    public int Count { get; } = count;
}
