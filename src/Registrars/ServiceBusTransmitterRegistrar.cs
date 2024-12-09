using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Soenneker.ServiceBus.Message.Registrars;
using Soenneker.ServiceBus.Sender.Registrars;
using Soenneker.ServiceBus.Transmitter.Abstract;
using Soenneker.Utils.BackgroundQueue.Registrars;

namespace Soenneker.ServiceBus.Transmitter.Registrars;

/// <summary>
/// A utility library for sending Service Bus messages
/// </summary>
public static class ServiceBusTransmitterRegistrar
{
    /// <summary>
    /// As Singleton
    /// </summary>
    public static void AddServiceBusTransmitter(this IServiceCollection services)
    {
        services.AddBackgroundQueue();
        services.AddServiceBusMessageUtil();
        services.AddServiceBusSenderUtil();
        services.TryAddSingleton<IServiceBusTransmitter, ServiceBusTransmitter>();
    }

    public static void AddServiceBusTransmitterAsScoped(this IServiceCollection services)
    {
        services.AddBackgroundQueue();
        services.AddServiceBusMessageUtil();
        services.AddServiceBusSenderUtil();
        services.TryAddScoped<IServiceBusTransmitter, ServiceBusTransmitter>();
    }
}