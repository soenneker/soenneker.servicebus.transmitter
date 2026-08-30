using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Utils.BackgroundQueue.Abstract;

namespace Soenneker.ServiceBus.Transmitter.Abstract;

/// <summary>
/// Builds and sends Soenneker message envelopes through Azure Service Bus, directly or through an in-process background queue.
/// </summary>
public interface IServiceBusTransmitter
{
    /// <summary>
    /// Builds and sends one message. By default, materializes it immediately and enqueues the Azure send for in-process background execution.
    /// </summary>
    /// <typeparam name="T">Type of value handled by the Service Bus Transmitter.</typeparam>
    /// <param name="msgModel">Msg Model for the send message operation.</param>
    /// <param name="useQueue">Whether to enqueue the Azure send for background execution instead of attempting it before returning.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task that completes when queued work has been accepted or the foreground send attempt has finished. Send failures are logged rather than returned.</returns>
    ValueTask SendMessage<T>(T msgModel, bool useQueue = true, CancellationToken cancellationToken = default) where T : Messages.Base.Message;

    /// <summary>
    /// Builds and attempts to send one message without using the background queue. Failures are logged rather than returned.
    /// </summary>
    /// <typeparam name="TMsg">Type of msg used by the operation.</typeparam>
    /// <param name="msg">Msg for the internal send message operation.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task that completes when the internal send message operation is complete.</returns>
    ValueTask InternalSendMessage<TMsg>(TMsg msg, CancellationToken cancellationToken = default) where TMsg : Messages.Base.Message;

    /// <summary>
    /// Builds and sends a same-queue message collection, using the in-process background queue by default.
    /// </summary>
    /// <typeparam name="T">Type of value handled by the Service Bus Transmitter.</typeparam>
    /// <param name="msgModels">msg Models to process.</param>
    /// <param name="useQueue">Whether to enqueue the Azure sends for background execution instead of attempting them before returning.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task that completes when queued work has been accepted or the foreground batch attempt has finished. Send failures are logged rather than returned.</returns>
    ValueTask SendMessages<T>(IList<T> msgModels, bool useQueue = true, CancellationToken cancellationToken = default) where T : Messages.Base.Message;

    /// <summary>
    /// Builds and attempts to send a same-queue message collection without using the background queue. Failures are logged rather than returned.
    /// </summary>
    /// <typeparam name="T">Type of value handled by the Service Bus Transmitter.</typeparam>
    /// <param name="msgModels">msg Models to process.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task that completes when the internal send messages operation is complete.</returns>
    ValueTask InternalSendMessages<T>(IList<T> msgModels, CancellationToken cancellationToken = default) where T : Messages.Base.Message;
}
