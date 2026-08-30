using Azure.Messaging.ServiceBus;

namespace Soenneker.ServiceBus.Transmitter.Dtos;

internal sealed class QueuedBatch
{
    public required string Queue { get; set; }

    public required string TypeName { get; set; }

    public required ServiceBusMessage[] Messages { get; set; }

    public string?[]? Jsons { get; init; } // only when transmitter logging enabled
}
