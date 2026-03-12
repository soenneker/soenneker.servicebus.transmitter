using Azure.Messaging.ServiceBus;

namespace Soenneker.ServiceBus.Transmitter.Dtos;

internal sealed class QueuedBatch
{
    public string Queue { get; set; }

    public string TypeName { get; set; }

    public ServiceBusMessage[] Messages { get; set; }

    public string[]? Jsons { get; init; } // only when transmitter logging enabled
}