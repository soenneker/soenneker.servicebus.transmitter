using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Soenneker.Extensions.Configuration;
using Soenneker.Extensions.Task;
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
    private readonly bool _enabled;

    public ServiceBusTransmitter(ILogger<ServiceBusTransmitter> logger, IBackgroundQueue backgroundQueue, IServiceBusMessageUtil serviceBusMessageUtil,
        IServiceBusSenderUtil serviceBusSenderUtil, IConfiguration config)
    {
        _logger = logger;
        _backgroundQueue = backgroundQueue;
        _serviceBusMessageUtil = serviceBusMessageUtil;
        _serviceBusSenderUtil = serviceBusSenderUtil;

        _enabled = config.GetValueStrict<bool>("Azure:ServiceBus:Enable");
    }

    public ValueTask SendMessage<TMessage>(TMessage message) where TMessage : Messages.Base.Message
    {
        if (!_enabled)
        {
            _logger.LogWarning("ServiceBus has been disabled from config, not sending message");
            return ValueTask.CompletedTask;
        }

        return _backgroundQueue.QueueValueTask(cancellationToken => InternalSendMessage(message, cancellationToken));
    }

    public async ValueTask InternalSendMessage<TMessage>(TMessage message, CancellationToken cancellationToken = default) where TMessage : Messages.Base.Message
    {
        Type type = typeof(TMessage);

        try
        {
            ServiceBusSender sender = await _serviceBusSenderUtil.GetSender(message.Queue);

            ServiceBusMessage? serviceBusMessage = await _serviceBusMessageUtil.BuildMessage(message, type);

            if (serviceBusMessage == null)
                throw new Exception("There was a problem building the ServiceBus message, cannot send");

            await sender.SendMessageAsync(serviceBusMessage, cancellationToken).NoSync();
        }
        catch (Exception e)
        {
            // TODO: make this louder
            _logger.LogError(e, "== ServiceBusTransmitter: Problem sending message, type: {type}", type.ToString());
        }
    }

    public ValueTask SendMessages<TMessage>(IList<TMessage> messages) where TMessage : Messages.Base.Message
    {
        if (!_enabled)
        {
            _logger.LogWarning("ServiceBus has been disabled from config, not sending message");
            return ValueTask.CompletedTask;
        }

        return _backgroundQueue.QueueValueTask(cancellationToken => InternalSendMessages(messages, cancellationToken));
    }

    public async ValueTask InternalSendMessages<TMessage>(IList<TMessage> messages, CancellationToken cancellationToken = default) where TMessage : Messages.Base.Message
    {
        Type type = typeof(TMessage);

        try
        {
            var queueName = $"{messages.First().Queue}";

            ServiceBusSender sender = await _serviceBusSenderUtil.GetSender(queueName);

            List<ServiceBusMessage> serviceBusMessages = [];

            foreach (TMessage message in messages)
            {
                ServiceBusMessage? serviceBusMessage = await _serviceBusMessageUtil.BuildMessage(message, type);

                if (serviceBusMessage != null)
                    serviceBusMessages.Add(serviceBusMessage);
            }

            if (!serviceBusMessages.Any())
            {
                _logger.LogError("== ServiceBusTransmitter: No messages to send...");
                return;
            }

            await sender.SendMessagesAsync(serviceBusMessages, cancellationToken);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "== ServiceBusTransmitter: Problem sending (batched) message, type: {type}", type.ToString());
        }
    }
}