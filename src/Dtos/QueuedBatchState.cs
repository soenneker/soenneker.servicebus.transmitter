namespace Soenneker.ServiceBus.Transmitter.Dtos;

internal readonly record struct QueuedBatchState(ServiceBusTransmitter Self, QueuedBatch Work);