using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Soenneker.ServiceBus.Message.Abstract;
using Soenneker.ServiceBus.Sender.Abstract;
using Soenneker.ServiceBus.Transmitter.Abstract;
using Soenneker.Utils.BackgroundQueue.Abstract;

namespace Soenneker.ServiceBus.Transmitter;

///<inheritdoc cref="IServiceBusTransmitter"/>
public class ServiceBusTransmitter : IServiceBusTransmitter
{
    private readonly ILogger<ServiceBusTransmitter> _logger;

    private readonly IBackgroundQueue _backgroundQueue;
    private readonly IServiceBusMessageUtil _serviceBusMessageUtil;
    private readonly IServiceBusSenderUtil _serviceBusSenderUtil;

    public ServiceBusTransmitter(ILogger<ServiceBusTransmitter> logger, IBackgroundQueue backgroundQueue, IServiceBusMessageUtil serviceBusMessageUtil,
        IServiceBusSenderUtil serviceBusSenderUtil)
    {
        _logger = logger;
        _backgroundQueue = backgroundQueue;
        _serviceBusMessageUtil = serviceBusMessageUtil;
        _serviceBusSenderUtil = serviceBusSenderUtil;
    }

    public ValueTask SendMessage<TMessage>(TMessage message) where TMessage : Messages.Base.Message
    {
        return _backgroundQueue.QueueValueTask(async _ => { await InternalSendMessage(message); });
    }

    public async ValueTask<bool> InternalSendMessage<TMessage>(TMessage message) where TMessage : Messages.Base.Message
    {
        Type type = typeof(TMessage);

        try
        {
            ServiceBusSender sender = await _serviceBusSenderUtil.GetSender(message.Queue);

            ServiceBusMessage? serviceBusMessage = _serviceBusMessageUtil.BuildMessage(message, type);

            if (serviceBusMessage == null)
                return false;

            await sender.SendMessageAsync(serviceBusMessage);

            return true;
        }
        catch (Exception e)
        {
            // TODO: make this louder
            _logger.LogError(e, "== SERVICEBUS: Problem sending message, type: {type}", type.ToString());
            return false;
        }
    }

    public ValueTask SendMessages<TMessage>(IList<TMessage> messages) where TMessage : Messages.Base.Message
    {
        return _backgroundQueue.QueueValueTask(async _ => { await InternalSendMessages(messages); });
    }

    public async ValueTask<bool> InternalSendMessages<TMessage>(IList<TMessage> messages) where TMessage : Messages.Base.Message
    {
        Type type = typeof(TMessage);

        try
        {
            var queueName = $"{messages.First().Queue}";

            ServiceBusSender sender = await _serviceBusSenderUtil.GetSender(queueName);

            List<ServiceBusMessage> serviceBusMessages = new();

            foreach (TMessage message in messages)
            {
                ServiceBusMessage? serviceBusMessage = _serviceBusMessageUtil.BuildMessage(message, type);

                if (serviceBusMessage != null)
                    serviceBusMessages.Add(serviceBusMessage);
            }

            if (!serviceBusMessages.Any())
            {
                _logger.LogError("== SERVICEBUS: No messages to send...");
                return false;
            }

            await sender.SendMessagesAsync(serviceBusMessages);

            return true;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "== SERVICEBUS: Problem sending (batched) message, type: {type}", type.ToString());
            return false;
        }
    }
}