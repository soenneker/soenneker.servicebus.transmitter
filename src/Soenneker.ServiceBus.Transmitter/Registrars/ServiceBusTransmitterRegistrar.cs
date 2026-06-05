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
    /// Adds service bus transmitter as singleton.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The result of the operation.</returns>
    public static IServiceCollection AddServiceBusTransmitterAsSingleton(this IServiceCollection services)
    {
        services.AddBackgroundQueueAsSingleton()
                .AddServiceBusMessageUtilAsSingleton()
                .AddServiceBusSenderUtilAsSingleton()
                .TryAddSingleton<IServiceBusTransmitter, ServiceBusTransmitter>();

        return services;
    }

    /// <summary>
    /// Adds service bus transmitter as scoped.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The result of the operation.</returns>
    public static IServiceCollection AddServiceBusTransmitterAsScoped(this IServiceCollection services)
    {
        services.AddBackgroundQueueAsSingleton()
                .AddServiceBusMessageUtilAsSingleton()
                .AddServiceBusSenderUtilAsSingleton()
                .TryAddScoped<IServiceBusTransmitter, ServiceBusTransmitter>();

        return services;
    }
}