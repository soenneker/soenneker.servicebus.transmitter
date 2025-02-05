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
    public static IServiceCollection AddServiceBusTransmitterAsSingleton(this IServiceCollection services)
    {
        services.AddBackgroundQueue();
        services.AddServiceBusMessageUtilAsSingleton();
        services.AddServiceBusSenderUtilAsSingleton();
        services.TryAddSingleton<IServiceBusTransmitter, ServiceBusTransmitter>();

        return services;
    }

    public static IServiceCollection AddServiceBusTransmitterAsScoped(this IServiceCollection services)
    {
        services.AddBackgroundQueue();
        services.AddServiceBusMessageUtilAsSingleton();
        services.AddServiceBusSenderUtilAsSingleton();
        services.TryAddScoped<IServiceBusTransmitter, ServiceBusTransmitter>();

        return services;
    }
}