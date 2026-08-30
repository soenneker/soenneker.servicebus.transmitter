[![](https://img.shields.io/nuget/v/Soenneker.ServiceBus.Transmitter.svg?style=for-the-badge)](https://www.nuget.org/packages/Soenneker.ServiceBus.Transmitter/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.servicebus.transmitter/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.servicebus.transmitter/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/Soenneker.ServiceBus.Transmitter.svg?style=for-the-badge)](https://www.nuget.org/packages/Soenneker.ServiceBus.Transmitter/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.servicebus.transmitter/codeql.yml?label=CodeQL&style=for-the-badge)](https://github.com/soenneker/soenneker.servicebus.transmitter/actions/workflows/codeql.yml)

# Soenneker.ServiceBus.Transmitter

Builds and sends Soenneker message envelopes to Azure Service Bus, either immediately or through a bounded in-process background queue.

## Installation

```bash
dotnet add package Soenneker.ServiceBus.Transmitter
```

## Configuration

```json
{
  "Azure": {
    "ServiceBus": {
      "ConnectionString": "Endpoint=sb://...",
      "Enable": true,
      "Log": false,
      "TransmitterLogging": false
    }
  },
  "Background": {
    "QueueLength": 5000,
    "Log": false
  }
}
```

`Azure:ServiceBus:Enable` is required. When false, send calls log a warning and return without contacting Service Bus. The connection-string credential needs queue-management and send permissions because the sender stack creates missing queues with Azure defaults.

`Azure:ServiceBus:Log` enables payload logging in the message builder and changes its JSON option to pretty output. `TransmitterLogging` writes complete serialized payloads at information level. Keep both disabled for sensitive messages unless the log destination is explicitly approved for that data.

## Registration

```csharp
using Soenneker.ServiceBus.Transmitter.Registrars;

services.AddServiceBusTransmitterAsSingleton();
```

This also registers the singleton background queue, message builder, sender cache, queue utility, administration client, and top-level Service Bus client.

`AddServiceBusTransmitterAsScoped()` makes only `IServiceBusTransmitter` scoped; its background queue and Service Bus dependencies remain singleton.

## Send one message

Messages must derive from `Soenneker.Messages.Base.Message` and supply `Type`, `Id`, `Queue`, `Sender`, and `CreatedAt`:

```csharp
OrderCreated message = new()
{
    Type = "order.created.v1",
    Id = Guid.NewGuid().ToString("N"),
    Queue = "orders",
    Sender = "checkout-api",
    CreatedAt = DateTimeOffset.UtcNow,
    OrderId = orderId
};

await transmitter.SendMessage(
    message,
    useQueue: false,
    cancellationToken);
```

With `useQueue: false`, the call attempts the send before returning. Send and cancellation exceptions are caught and logged by the transmitter rather than returned to the caller, so successful completion is not a delivery receipt. Message-build failures and oversized bodies are also logged and skipped.

With `useQueue: true` (the default), the message is serialized on the caller's path and the materialized work item is added to the bounded background queue:

```csharp
await transmitter.SendMessage(message, cancellationToken: cancellationToken);
```

That call completes when the queue accepts the work item, not when Azure Service Bus accepts the message. The queue exists only in process and is not an outbox. Process failure or shutdown can lose queued work; use foreground sending or a durable outbox when loss is unacceptable.

## Send a batch

```csharp
await transmitter.SendMessages(
    messages,
    useQueue: false,
    cancellationToken);
```

Every item must target the same `Message.Queue`; otherwise the entire call is logged and skipped. The transmitter builds Azure batches and sends each full batch before continuing. A message that cannot be built is skipped. If a message cannot fit into a fresh batch, the remaining messages fall back to individual sends.

Batch sends are not atomic. Earlier batches may already have reached Service Bus when a later operation fails, and retries can create duplicates. Use stable identifiers and idempotent consumers.

The message builder places `Message.Type` in `ApplicationProperties["type"]`, but it does not copy `Message.Id` into the broker `MessageId`. Set transport-level deduplication metadata separately if your contract requires it.
