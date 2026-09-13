using Azure.Messaging.ServiceBus;

namespace Soenneker.ServiceBus.Transmitter.Dtos;

internal sealed class QueuedSingle(ServiceBusTransmitter self, string queue, string typeName, ServiceBusMessage message)
{
    public ServiceBusTransmitter Self { get; } = self;
    public string Queue { get; } = queue;
    public string TypeName { get; } = typeName;
    public ServiceBusMessage Message { get; } = message;
}
