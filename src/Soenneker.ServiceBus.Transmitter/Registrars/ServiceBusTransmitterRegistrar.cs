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
    /// Registers Service Bus Transmitter with a singleton lifetime.
    /// </summary>
    /// <param name="services">Service collection that receives the registration.</param>
    /// <returns>The same service collection, so additional registrations can be chained.</returns>
    public static IServiceCollection AddServiceBusTransmitterAsSingleton(this IServiceCollection services)
    {
        services.AddBackgroundQueueAsSingleton()
                .AddServiceBusMessageUtilAsSingleton()
                .AddServiceBusSenderUtilAsSingleton()
                .TryAddSingleton<IServiceBusTransmitter, ServiceBusTransmitter>();

        return services;
    }

    /// <summary>
    /// Registers Service Bus Transmitter with a scoped lifetime.
    /// </summary>
    /// <param name="services">Service collection that receives the registration.</param>
    /// <returns>The same service collection, so additional registrations can be chained.</returns>
    public static IServiceCollection AddServiceBusTransmitterAsScoped(this IServiceCollection services)
    {
        services.AddBackgroundQueueAsSingleton()
                .AddServiceBusMessageUtilAsSingleton()
                .AddServiceBusSenderUtilAsSingleton()
                .TryAddScoped<IServiceBusTransmitter, ServiceBusTransmitter>();

        return services;
    }
}
