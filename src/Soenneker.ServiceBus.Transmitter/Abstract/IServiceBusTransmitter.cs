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
    ValueTask SendMessage<T>(T msgModel, bool useQueue = true, CancellationToken cancellationToken = default) where T : Messages.Base.Message;

    /// <summary>
    /// Actually sends the message after getting the connection, etc. Not supposed to be accessed directly besides tests.
    /// </summary>
    ValueTask InternalSendMessage<TMsg>(TMsg msg, CancellationToken cancellationToken = default) where TMsg : Messages.Base.Message;

    /// <summary>
    /// Wraps <see cref="InternalSendMessages{TMsg}"/> with TaskQueue
    /// </summary>
    ValueTask SendMessages<T>(IList<T> msgModels, bool useQueue = true, CancellationToken cancellationToken = default) where T : Messages.Base.Message;

    /// <summary>
    /// Actually sends the message after getting the connection, etc. Not supposed to be accessed directly besides tests.
    /// </summary>
    ValueTask InternalSendMessages<T>(IList<T> msgModels, CancellationToken cancellationToken = default) where T : Messages.Base.Message;
}