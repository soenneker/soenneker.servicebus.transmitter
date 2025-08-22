using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Soenneker.Extensions.Configuration;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using Soenneker.ServiceBus.Message.Abstract;
using Soenneker.ServiceBus.Sender.Abstract;
using Soenneker.ServiceBus.Transmitter.Abstract;
using Soenneker.ServiceBus.Transmitter.Utils;
using Soenneker.Utils.BackgroundQueue.Abstract;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Utils.Json;

namespace Soenneker.ServiceBus.Transmitter;

///<inheritdoc cref="IServiceBusTransmitter"/>
public sealed class ServiceBusTransmitter : IServiceBusTransmitter
{
    private readonly ILogger<ServiceBusTransmitter> _logger;

    private readonly IBackgroundQueue _backgroundQueue;
    private readonly IServiceBusMessageUtil _serviceBusMessageUtil;
    private readonly IServiceBusSenderUtil _serviceBusSenderUtil;
    private readonly bool _enabled;
    private readonly bool _transmitterLogging;

    public ServiceBusTransmitter(ILogger<ServiceBusTransmitter> logger, IBackgroundQueue backgroundQueue, IServiceBusMessageUtil serviceBusMessageUtil,
        IServiceBusSenderUtil serviceBusSenderUtil, IConfiguration config)
    {
        _logger = logger;
        _backgroundQueue = backgroundQueue;
        _serviceBusMessageUtil = serviceBusMessageUtil;
        _serviceBusSenderUtil = serviceBusSenderUtil;

        _enabled = config.GetValueStrict<bool>("Azure:ServiceBus:Enable");
        _transmitterLogging = config.GetValue<bool>("Azure:ServiceBus:TransmitterLogging");
    }

    public ValueTask SendMessage<TMessage>(TMessage message, bool useQueue = true, CancellationToken cancellationToken = default)
        where TMessage : Messages.Base.Message
    {
        if (!_enabled)
        {
            _logger.LogWarning("ServiceBus disabled via config; skipping send.");
            return ValueTask.CompletedTask;
        }

        return useQueue
            ? _backgroundQueue.QueueValueTask(token => InternalSendMessage(message, token), cancellationToken)
            : InternalSendMessage(message, cancellationToken);
    }

    public async ValueTask InternalSendMessage<TMessage>(TMessage message, CancellationToken cancellationToken = default) where TMessage : Messages.Base.Message
    {
        if (!_enabled) return;

        string queue = message.Queue;
        string typeName = TypeCache<TMessage>.TypeName;

        using IDisposable? _ = _logger.BeginScope(new Dictionary<string, object>(2)
        {
            ["sb.queue"] = queue,
            ["sb.type"] = typeName
        });

        try
        {
            if (_transmitterLogging)
                _logger.LogInformation("TX: {json}", JsonUtil.Serialize(message));

            ServiceBusSender sender = await _serviceBusSenderUtil.Get(queue, cancellationToken).NoSync();
            ServiceBusMessage? sbMessage = _serviceBusMessageUtil.BuildMessage(message, TypeCache<TMessage>.Type);

            if (sbMessage is null)
                return;

            await sender.SendMessageAsync(sbMessage, cancellationToken).NoSync();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TX: error sending single message.");
        }
    }

    public ValueTask SendMessages<TMessage>(IList<TMessage> messages, bool useQueue = true, CancellationToken cancellationToken = default)
        where TMessage : Messages.Base.Message
    {
        if (!_enabled)
        {
            _logger.LogWarning("ServiceBus disabled via config; skipping batch send.");
            return ValueTask.CompletedTask;
        }

        return useQueue
            ? _backgroundQueue.QueueValueTask(token => InternalSendMessages(messages, token), cancellationToken)
            : InternalSendMessages(messages, cancellationToken);
    }

    public async ValueTask InternalSendMessages<TMessage>(IList<TMessage> messages, CancellationToken cancellationToken = default)
        where TMessage : Messages.Base.Message
    {
        if (!_enabled || messages is null || messages.Count == 0) return;

        // Validate all messages target the same queue
        string queue = messages[0].Queue;
        for (var i = 1; i < messages.Count; i++)
        {
            if (messages[i].Queue != queue)
            {
                _logger.LogError("All messages in a batch must target the same queue. Expected: {ExpectedQueue}, Found: {FoundQueue} at index {Index}", queue,
                    messages[i].Queue, i);
                return;
            }
        }

        string typeName = TypeCache<TMessage>.TypeName;

        using IDisposable? _ = _logger.BeginScope(new Dictionary<string, object>(2)
        {
            ["sb.queue"] = queue,
            ["sb.type"] = typeName
        });

        try
        {
            ServiceBusSender sender = await _serviceBusSenderUtil.Get(queue, cancellationToken).NoSync();

            // we still batch for efficiency, but log JSON for every message
            await using var disposer = new BatchDisposer();
            disposer.Batch = await sender.CreateMessageBatchAsync(cancellationToken).NoSync();

            for (var i = 0; i < messages.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                TMessage msg = messages[i];

                if (_transmitterLogging)
                    _logger.LogInformation("TX: {json}", JsonUtil.Serialize(msg));

                ServiceBusMessage? sbMsg = _serviceBusMessageUtil.BuildMessage(msg, TypeCache<TMessage>.Type);

                if (sbMsg is null)
                    continue;

                if (!disposer.Batch.TryAddMessage(sbMsg))
                {
                    // Send current batch
                    await sender.SendMessagesAsync(disposer.Batch, cancellationToken).NoSync();

                    // Create new batch with retry logic
                    ServiceBusMessageBatch? newBatch = null;
                    var retryCount = 0;
                    const int maxRetries = 3;

                    while (newBatch == null && retryCount < maxRetries)
                    {
                        try
                        {
                            newBatch = await sender.CreateMessageBatchAsync(cancellationToken).NoSync();
                        }
                        catch (Exception ex) when (retryCount < maxRetries - 1)
                        {
                            retryCount++;
                            _logger.LogWarning(ex, "Failed to create new batch, retry {RetryCount}/{MaxRetries}", retryCount, maxRetries);
                            await Task.Delay(100 * retryCount, cancellationToken).NoSync();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Failed to create new batch after {MaxRetries} retries, falling back to individual message sending",
                                maxRetries);
                            // Fallback to individual message sending for the remaining messages
                            await SendRemainingMessagesIndividually(sender, messages, i, cancellationToken).NoSync();
                            return;
                        }
                    }

                    if (newBatch != null)
                    {
                        disposer.Replace(newBatch);

                        // Try to add the message to the new batch
                        if (!disposer.Batch.TryAddMessage(sbMsg))
                        {
                            _logger.LogError("Failed to add message to new batch, falling back to individual message sending");
                            await SendRemainingMessagesIndividually(sender, messages, i, cancellationToken).NoSync();
                            return;
                        }
                    }
                }
            }

            if (disposer.Batch.Count > 0)
                await sender.SendMessagesAsync(disposer.Batch, cancellationToken).NoSync();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // silence on cancel
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TX: error sending batch.");
        }
    }

    private async ValueTask SendRemainingMessagesIndividually<TMessage>(ServiceBusSender sender, IList<TMessage> messages, int startIndex,
        CancellationToken cancellationToken) where TMessage : Messages.Base.Message
    {
        for (int i = startIndex; i < messages.Count; i++)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                TMessage msg = messages[i];
                ServiceBusMessage? sbMsg = _serviceBusMessageUtil.BuildMessage(msg, TypeCache<TMessage>.Type);

                if (sbMsg != null)
                {
                    await sender.SendMessageAsync(sbMsg, cancellationToken).NoSync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send individual message at index {Index}", i);
            }
        }
    }
}