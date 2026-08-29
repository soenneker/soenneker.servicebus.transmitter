using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Utils.BackgroundQueue.Abstract;

namespace Soenneker.ServiceBus.Transmitter.Abstract;

/// <summary>
/// A utility library for sending Service Bus messages <para/>
/// Singleton IoC
/// </summary>
public interface IServiceBusTransmitter
{
    /// <summary>
    /// Wraps <see cref="InternalSendMessage{TMsg}"/> with <see cref="IBackgroundQueue"/>
    /// </summary>
    /// <typeparam name="T">Type of value handled by the Service Bus Transmitter.</typeparam>
    /// <param name="msgModel">Msg Model for the send message operation.</param>
    /// <param name="useQueue">Whether to enqueue the write for background execution instead of awaiting Redis directly.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task that completes when the message has been sent.</returns>
    ValueTask SendMessage<T>(T msgModel, bool useQueue = true, CancellationToken cancellationToken = default) where T : Messages.Base.Message;

    /// <summary>
    /// Actually sends the message after getting the connection, etc. Not supposed to be accessed directly besides tests.
    /// </summary>
    /// <typeparam name="TMsg">Type of msg used by the operation.</typeparam>
    /// <param name="msg">Msg for the internal send message operation.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task that completes when the internal send message operation is complete.</returns>
    ValueTask InternalSendMessage<TMsg>(TMsg msg, CancellationToken cancellationToken = default) where TMsg : Messages.Base.Message;

    /// <summary>
    /// Wraps <see cref="InternalSendMessages{TMsg}"/> with TaskQueue
    /// </summary>
    /// <typeparam name="T">Type of value handled by the Service Bus Transmitter.</typeparam>
    /// <param name="msgModels">msg Models to process.</param>
    /// <param name="useQueue">Whether to enqueue the write for background execution instead of awaiting Redis directly.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task that completes when the messages has been sent.</returns>
    ValueTask SendMessages<T>(IList<T> msgModels, bool useQueue = true, CancellationToken cancellationToken = default) where T : Messages.Base.Message;

    /// <summary>
    /// Actually sends the message after getting the connection, etc. Not supposed to be accessed directly besides tests.
    /// </summary>
    /// <typeparam name="T">Type of value handled by the Service Bus Transmitter.</typeparam>
    /// <param name="msgModels">msg Models to process.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task that completes when the internal send messages operation is complete.</returns>
    ValueTask InternalSendMessages<T>(IList<T> msgModels, CancellationToken cancellationToken = default) where T : Messages.Base.Message;
}
