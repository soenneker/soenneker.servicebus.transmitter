using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Soenneker.Extensions.Configuration;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using Soenneker.ServiceBus.Message.Abstract;
using Soenneker.ServiceBus.Sender.Abstract;
using Soenneker.ServiceBus.Transmitter.Abstract;
using Soenneker.ServiceBus.Transmitter.Dtos;
using Soenneker.ServiceBus.Transmitter.Utils;
using Soenneker.Utils.BackgroundQueue.Abstract;
using Soenneker.Utils.Json;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.ServiceBus.Transmitter;

/// <inheritdoc cref="IServiceBusTransmitter"/>
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

        if (!useQueue)
            return InternalSendMessage(message, cancellationToken);

        // IMPORTANT: materialize what we need NOW, so we don't keep the big message graph alive in the queued closure.
        QueuedSingle? work = BuildQueuedSingle(message);

        // If we couldn't build, skip
        if (work is null)
            return ValueTask.CompletedTask;

        return _backgroundQueue.QueueValueTask(new QueuedSingleState(this, work),
            static (state, token) => state.Self.InternalSendQueuedSingle(state.Work, token), cancellationToken);
    }

    public async ValueTask InternalSendMessage<TMessage>(TMessage message, CancellationToken cancellationToken = default) where TMessage : Messages.Base.Message
    {
        if (!_enabled)
            return;

        string queue = message.Queue;
        
        using IDisposable? _ = _logger.BeginScope(new Dictionary<string, object>(2)
        {
            ["sb.queue"] = queue,
            ["sb.type"] = message.Type
        });

        try
        {
            if (_transmitterLogging)
                _logger.LogInformation("TX: {json}", JsonUtil.Serialize(message));

            ServiceBusSender sender = await _serviceBusSenderUtil.Get(queue, cancellationToken)
                                                                 .NoSync();

            ServiceBusMessage? sbMessage = _serviceBusMessageUtil.BuildMessage(message, message.Type);

            if (sbMessage is null)
                return;

            await sender.SendMessageAsync(sbMessage, cancellationToken)
                        .NoSync();
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

        if (!useQueue)
            return InternalSendMessages(messages, cancellationToken);

        // IMPORTANT: materialize what we need NOW, so the queued closure doesn't retain the huge list / message graphs.
        QueuedBatch? work = BuildQueuedBatch(messages);

        if (work is null)
            return ValueTask.CompletedTask;

        return _backgroundQueue.QueueValueTask(new QueuedBatchState(this, work), static (state, token) => state.Self.InternalSendQueuedBatch(state.Work, token),
            cancellationToken);
    }

    public async ValueTask InternalSendMessages<TMessage>(IList<TMessage> messages, CancellationToken cancellationToken = default)
        where TMessage : Messages.Base.Message
    {
        if (!_enabled || messages is null || messages.Count == 0)
            return;

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

        Type runtimeType = messages[0]
            .GetType();
        string typeName = runtimeType.FullName ?? runtimeType.Name;

        using IDisposable? _ = _logger.BeginScope(new Dictionary<string, object>(2)
        {
            ["sb.queue"] = queue,
            ["sb.type"] = typeName
        });

        try
        {
            ServiceBusSender sender = await _serviceBusSenderUtil.Get(queue, cancellationToken)
                                                                 .NoSync();

            await using var disposer = new BatchDisposer();
            disposer.Batch = await sender.CreateMessageBatchAsync(cancellationToken)
                                         .NoSync();

            for (var i = 0; i < messages.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                TMessage message = messages[i];

                if (_transmitterLogging)
                    _logger.LogInformation("TX: {json}", JsonUtil.Serialize(message));

                ServiceBusMessage? sbMsg = _serviceBusMessageUtil.BuildMessage(message, message.Type);

                if (sbMsg is null)
                    continue;

                if (!disposer.Batch.TryAddMessage(sbMsg))
                {
                    await sender.SendMessagesAsync(disposer.Batch, cancellationToken)
                                .NoSync();

                    disposer.Replace(await sender.CreateMessageBatchAsync(cancellationToken)
                                                 .NoSync());

                    if (!disposer.Batch.TryAddMessage(sbMsg))
                    {
                        _logger.LogError("Failed to add message to new batch, falling back to individual message sending");
                        await SendRemainingMessagesIndividually(sender, messages, i, cancellationToken)
                            .NoSync();
                        return;
                    }
                }
            }

            if (disposer.Batch.Count > 0)
                await sender.SendMessagesAsync(disposer.Batch, cancellationToken)
                            .NoSync();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TX: error sending batch.");
        }
    }


    private QueuedSingle? BuildQueuedSingle<TMessage>(TMessage message) where TMessage : Messages.Base.Message
    {
        string queue = message.Queue;

        ServiceBusMessage? sbMessage = _serviceBusMessageUtil.BuildMessage(message, message.Type);

        if (sbMessage is null)
            return null;

        string? json = _transmitterLogging ? JsonUtil.Serialize(message) : null;

        return new QueuedSingle
        {
            Queue = queue,
            TypeName = message.Type,
            SbMessage = sbMessage,
            Json = json
        };
    }

    private QueuedBatch? BuildQueuedBatch<TMessage>(IList<TMessage> messages) where TMessage : Messages.Base.Message
    {
        if (messages is null || messages.Count == 0)
            return null;

        string queue = messages[0].Queue;

        for (var i = 1; i < messages.Count; i++)
        {
            if (messages[i].Queue != queue)
            {
                _logger.LogError("All messages in a batch must target the same queue. Expected: {ExpectedQueue}, Found: {FoundQueue} at index {Index}", queue,
                    messages[i].Queue, i);
                return null;
            }
        }

        Type runtimeType = messages[0]
            .GetType();
        string typeName = runtimeType.FullName ?? runtimeType.Name;

        var sbMessages = new ServiceBusMessage[messages.Count];
        string[]? jsons = _transmitterLogging ? new string[messages.Count] : null;

        var written = 0;

        for (var i = 0; i < messages.Count; i++)
        {
            TMessage message = messages[i];

            if (jsons is not null)
                jsons[i] = JsonUtil.Serialize(message);

            ServiceBusMessage? sb = _serviceBusMessageUtil.BuildMessage(message, message.Type);

            if (sb is null)
                continue;

            sbMessages[written++] = sb;
        }

        if (written == 0)
            return null;

        if (written != sbMessages.Length)
            Array.Resize(ref sbMessages, written);

        if (jsons is not null && written != jsons.Length)
            Array.Resize(ref jsons, written);

        return new QueuedBatch
        {
            Queue = queue,
            TypeName = typeName,
            Messages = sbMessages,
            Jsons = jsons
        };
    }

    private async ValueTask InternalSendQueuedSingle(QueuedSingle work, CancellationToken cancellationToken)
    {
        if (!_enabled)
            return;

        using IDisposable? _ = _logger.BeginScope(new Dictionary<string, object>(2)
        {
            ["sb.queue"] = work.Queue,
            ["sb.type"] = work.TypeName
        });

        try
        {
            if (_transmitterLogging && work.Json is not null)
                _logger.LogInformation("TX: {json}", work.Json);

            ServiceBusSender sender = await _serviceBusSenderUtil.Get(work.Queue, cancellationToken)
                                                                 .NoSync();
            await sender.SendMessageAsync(work.SbMessage, cancellationToken)
                        .NoSync();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TX: error sending single message.");
        }
    }

    private async ValueTask InternalSendQueuedBatch(QueuedBatch work, CancellationToken cancellationToken)
    {
        if (!_enabled || work.Messages.Length == 0)
            return;

        using IDisposable? _ = _logger.BeginScope(new Dictionary<string, object>(2)
        {
            ["sb.queue"] = work.Queue,
            ["sb.type"] = work.TypeName
        });

        try
        {
            ServiceBusSender sender = await _serviceBusSenderUtil.Get(work.Queue, cancellationToken)
                                                                 .NoSync();

            await using var disposer = new BatchDisposer();
            disposer.Batch = await sender.CreateMessageBatchAsync(cancellationToken)
                                         .NoSync();

            for (int i = 0; i < work.Messages.Length; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (_transmitterLogging && work.Jsons is not null)
                    _logger.LogInformation("TX: {json}", work.Jsons[i]);

                ServiceBusMessage sbMsg = work.Messages[i];

                if (!disposer.Batch.TryAddMessage(sbMsg))
                {
                    await sender.SendMessagesAsync(disposer.Batch, cancellationToken)
                                .NoSync();

                    ServiceBusMessageBatch? newBatch = null;
                    var retryCount = 0;
                    const int maxRetries = 3;

                    while (newBatch == null && retryCount < maxRetries)
                    {
                        try
                        {
                            newBatch = await sender.CreateMessageBatchAsync(cancellationToken)
                                                   .NoSync();
                        }
                        catch (Exception ex) when (retryCount < maxRetries - 1)
                        {
                            retryCount++;
                            _logger.LogWarning(ex, "Failed to create new batch, retry {RetryCount}/{MaxRetries}", retryCount, maxRetries);
                            await Task.Delay(100 * retryCount, cancellationToken)
                                      .NoSync();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Failed to create new batch after {MaxRetries} retries, falling back to individual message sending",
                                maxRetries);

                            // Fallback to individual sends for remaining
                            await SendRemainingServiceBusMessagesIndividually(sender, work.Messages, i, cancellationToken)
                                .NoSync();
                            return;
                        }
                    }

                    disposer.Replace(newBatch!);

                    if (!disposer.Batch.TryAddMessage(sbMsg))
                    {
                        _logger.LogError("Failed to add message to new batch, falling back to individual message sending");
                        await SendRemainingServiceBusMessagesIndividually(sender, work.Messages, i, cancellationToken)
                            .NoSync();
                        return;
                    }
                }
            }

            if (disposer.Batch.Count > 0)
                await sender.SendMessagesAsync(disposer.Batch, cancellationToken)
                            .NoSync();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TX: error sending batch.");
        }
    }

    private static async ValueTask SendRemainingServiceBusMessagesIndividually(ServiceBusSender sender, ServiceBusMessage[] messages, int startIndex,
        CancellationToken cancellationToken)
    {
        for (int i = startIndex; i < messages.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await sender.SendMessageAsync(messages[i], cancellationToken).NoSync();
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

                TMessage message = messages[i];
                ServiceBusMessage? sbMsg = _serviceBusMessageUtil.BuildMessage(message, message.Type);

                if (sbMsg != null)
                    await sender.SendMessageAsync(sbMsg, cancellationToken)
                                .NoSync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send individual message at index {Index}", i);
            }
        }
    }
}