namespace Soenneker.ServiceBus.Transmitter.Dtos;

internal readonly record struct QueuedSingleState(ServiceBusTransmitter Self, QueuedSingle Work);