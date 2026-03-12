using Azure.Messaging.ServiceBus;

namespace Soenneker.ServiceBus.Transmitter.Dtos;

internal sealed class QueuedSingle
{
    public string Queue { get; set; }

    public string TypeName { get; set; }

    public ServiceBusMessage SbMessage { get; set; }

    public string? Json { get; init; } // only when transmitter logging enabled
}