[![](https://img.shields.io/nuget/v/Soenneker.ServiceBus.Transmitter.svg?style=for-the-badge)](https://www.nuget.org/packages/Soenneker.ServiceBus.Transmitter/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.servicebus.transmitter/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.servicebus.transmitter/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/Soenneker.ServiceBus.Transmitter.svg?style=for-the-badge)](https://www.nuget.org/packages/Soenneker.ServiceBus.Transmitter/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.servicebus.transmitter/codeql.yml?label=CodeQL&style=for-the-badge)](https://github.com/soenneker/soenneker.servicebus.transmitter/actions/workflows/codeql.yml)

# Soenneker.ServiceBus.Transmitter

A utility library for sending Service Bus messages Singleton IoC.

## Install

```bash
dotnet add package Soenneker.ServiceBus.Transmitter
```

## Quick start

```csharp
using Soenneker.ServiceBus.Transmitter.Registrars;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
var result = services.AddServiceBusTransmitterAsSingleton();
```

Registers Service Bus Transmitter with a singleton lifetime.

## What you get

- `IServiceBusTransmitter` — A utility library for sending Service Bus messages Singleton IoC.
- `ServiceBusTransmitterRegistrar` — A utility library for sending Service Bus messages.

## API at a glance

| API | What it does | Result / important behavior |
| --- | --- | --- |
| `IServiceBusTransmitter.SendMessage(msgModel, useQueue, cancellationToken)` | Wraps `InternalSendMessage{TMsg}` with `IBackgroundQueue`. | A task that completes when the message has been sent. |
| `IServiceBusTransmitter.InternalSendMessage(msg, cancellationToken)` | Actually sends the message after getting the connection, etc. Not supposed to be accessed directly besides tests. | A task that completes when the internal send message operation is complete. |
| `IServiceBusTransmitter.SendMessages(msgModels, useQueue, cancellationToken)` | Wraps `InternalSendMessages{TMsg}` with TaskQueue. | A task that completes when the messages has been sent. |
| `IServiceBusTransmitter.InternalSendMessages(msgModels, cancellationToken)` | Actually sends the message after getting the connection, etc. Not supposed to be accessed directly besides tests. | A task that completes when the internal send messages operation is complete. |
| `ServiceBusTransmitterRegistrar.AddServiceBusTransmitterAsSingleton(services)` | Registers Service Bus Transmitter with a singleton lifetime. | The same service collection, so additional registrations can be chained. |
| `ServiceBusTransmitterRegistrar.AddServiceBusTransmitterAsScoped(services)` | Registers Service Bus Transmitter with a scoped lifetime. | The same service collection, so additional registrations can be chained. |

## Practical notes

- Cancellation stops pending work; it does not undo work that has already completed.
- Calls that return a cached or singleton value reuse the same instance until the owning service is disposed.
