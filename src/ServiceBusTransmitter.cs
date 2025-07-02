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
using Soenneker.Extensions.ValueTask;
using Soenneker.ServiceBus.Message.Abstract;
using Soenneker.ServiceBus.Sender.Abstract;
using Soenneker.ServiceBus.Transmitter.Abstract;
using Soenneker.Utils.BackgroundQueue.Abstract;

namespace Soenneker.ServiceBus.Transmitter;

///<inheritdoc cref="IServiceBusTransmitter"/>
public sealed class ServiceBusTransmitter : IServiceBusTransmitter
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

    public ValueTask SendMessage<TMessage>(TMessage message, bool useQueue = true, CancellationToken cancellationToken = default) where TMessage : Messages.Base.Message
    {
        if (!_enabled)
        {
            _logger.LogWarning("ServiceBus has been disabled from config, not sending message");
            return ValueTask.CompletedTask;
        }

        if (useQueue)
            return _backgroundQueue.QueueValueTask(token => InternalSendMessage(message, token), cancellationToken);

        return InternalSendMessage(message, cancellationToken);
    }

    public async ValueTask InternalSendMessage<TMessage>(TMessage message, CancellationToken cancellationToken = default) where TMessage : Messages.Base.Message
    {
        Type type = typeof(TMessage);

        ServiceBusSender sender = await _serviceBusSenderUtil.Get(message.Queue, cancellationToken).NoSync();

        try
        {
            ServiceBusMessage? serviceBusMessage = _serviceBusMessageUtil.BuildMessage(message, type);

            if (serviceBusMessage == null)
                throw new Exception("There was a problem building the ServiceBus message, cannot send");

            await sender.SendMessageAsync(serviceBusMessage, cancellationToken).NoSync();
        }
        catch (Exception e)
        {
            // TODO: make this louder
            _logger.LogError(e, "== {name}: Problem sending message, type: {type}", nameof(ServiceBusTransmitter), type.ToString());
        }
    }

    public ValueTask SendMessages<TMessage>(IList<TMessage> messages, bool useQueue = true, CancellationToken cancellationToken = default) where TMessage : Messages.Base.Message
    {
        if (!_enabled)
        {
            _logger.LogWarning("ServiceBus has been disabled from config, not sending message");
            return ValueTask.CompletedTask;
        }

        if (useQueue)
            return _backgroundQueue.QueueValueTask(token => InternalSendMessages(messages, token), cancellationToken);

        return InternalSendMessages(messages, cancellationToken);
    }

    public async ValueTask InternalSendMessages<TMessage>(IList<TMessage> messages, CancellationToken cancellationToken = default) where TMessage : Messages.Base.Message
    {
        Type type = typeof(TMessage);

        string queueName = messages.First().Queue;

        ServiceBusSender sender = await _serviceBusSenderUtil.Get(queueName, cancellationToken).NoSync();

        List<ServiceBusMessage> serviceBusMessages = [];

        try
        {
            for (var i = 0; i < messages.Count; i++)
            {
                TMessage message = messages[i];
                ServiceBusMessage? serviceBusMessage = _serviceBusMessageUtil.BuildMessage(message, type);

                if (serviceBusMessage != null)
                    serviceBusMessages.Add(serviceBusMessage);
            }

            if (serviceBusMessages.Count == 0)
            {
                _logger.LogError("== {name}: No messages to send...", nameof(ServiceBusTransmitter));
                return;
            }

            await sender.SendMessagesAsync(serviceBusMessages, cancellationToken).NoSync();
        }
        catch (Exception e)
        {
            _logger.LogError(e, "== {name}: Problem sending (batched) message, type: {type}", nameof(ServiceBusTransmitter), type.ToString());
        }
    }
}