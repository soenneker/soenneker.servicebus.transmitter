using Azure.Messaging.ServiceBus;

namespace Soenneker.ServiceBus.Transmitter.Dtos;

internal sealed class QueuedSingle
{
    public required string Queue { get; set; }

    public required string TypeName { get; set; }

    public required ServiceBusMessage SbMessage { get; set; }

    public string? Json { get; init; } // only when transmitter logging enabled
}
