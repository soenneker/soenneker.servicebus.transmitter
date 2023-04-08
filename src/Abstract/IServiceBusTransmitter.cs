using System.Collections.Generic;
using System.Threading.Tasks;

namespace Soenneker.ServiceBus.Transmitter.Abstract;

/// <summary>
/// A utility library for sending Service Bus messages <para/>
/// Singleton IoC
/// </summary>
public interface IServiceBusTransmitter
{
    /// <summary>
    /// Wraps <see cref="InternalSendMessage{TMsg}"/> with TaskQueue
    /// </summary>
    ValueTask SendMessage<T>(T msgModel) where T : Messages.Base.Message;

    /// <summary>
    /// Actually sends the message after getting the connection, etc. Not supposed to be accessed directly besides tests.
    /// </summary>
    ValueTask<bool> InternalSendMessage<TMsg>(TMsg msg) where TMsg : Messages.Base.Message;

    /// <summary>
    /// Wraps <see cref="InternalSendMessages{TMsg}"/> with TaskQueue
    /// </summary>
    ValueTask SendMessages<T>(IList<T> msgModels) where T : Messages.Base.Message;

    /// <summary>
    /// Actually sends the message after getting the connection, etc. Not supposed to be accessed directly besides tests.
    /// </summary>
    ValueTask<bool> InternalSendMessages<T>(IList<T> msgModels) where T : Messages.Base.Message;
}