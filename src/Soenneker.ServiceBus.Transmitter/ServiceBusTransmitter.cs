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
using Soenneker.Utils.BackgroundQueue.Abstract;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.ServiceBus.Transmitter;

public sealed class ServiceBusTransmitter : IServiceBusTransmitter
{
    private static readonly Func<ILogger, string, string, IDisposable?> _sendScope =
        LoggerMessage.DefineScope<string, string>("{sb.queue} {sb.type}");
    private static readonly Action<ILogger, string, Exception?> _logPayload =
        LoggerMessage.Define<string>(LogLevel.Information, new EventId(1, "Transmit"), "TX: {json}");

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

        cancellationToken.ThrowIfCancellationRequested();
        ServiceBusMessage? built = _serviceBusMessageUtil.BuildMessage(message, message.Type);
        if (built is null)
            return ValueTask.CompletedTask;

        // Only retain the materialized transport message, not the application object graph.
        var work = new QueuedSingle(this, message.Queue, message.Type, built);
        return _backgroundQueue.QueueValueTask(work,
            static (state, token) => state.Self.SendSingle(state.Queue, state.TypeName, state.Message, token), cancellationToken);
    }

    public ValueTask InternalSendMessage<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : Messages.Base.Message
    {
        if (!_enabled)
            return ValueTask.CompletedTask;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            ServiceBusMessage? built = _serviceBusMessageUtil.BuildMessage(message, message.Type);
            if (built is not null)
                return SendSingle(message.Queue, message.Type, built, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TX: error sending single message.");
        }
        return ValueTask.CompletedTask;
    }

    private async ValueTask SendSingle(string queue, string type, ServiceBusMessage message, CancellationToken cancellationToken)
    {
        using IDisposable? scope = _sendScope(_logger, queue, type);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            LogPayload(message);
            ServiceBusSender sender = await _serviceBusSenderUtil.Get(queue, cancellationToken).NoSync();
            await sender.SendMessageAsync(message, cancellationToken).NoSync();
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
        if (messages is null || messages.Count == 0)
            return ValueTask.CompletedTask;

        cancellationToken.ThrowIfCancellationRequested();
        if (messages.Count == 1)
            return SendMessage(messages[0], true, cancellationToken);
        if (!ValidateQueues(messages, out string queue))
            return ValueTask.CompletedTask;

        var built = new ServiceBusMessage[messages.Count];
        int count = 0;
        for (int i = 0; i < messages.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TMessage message = messages[i];
            ServiceBusMessage? transport = _serviceBusMessageUtil.BuildMessage(message, message.Type);
            if (transport is not null)
                built[count++] = transport;
        }

        if (count == 0)
            return ValueTask.CompletedTask;

        Type runtimeType = messages[0].GetType();
        var work = new QueuedBatch(this, queue, runtimeType.FullName ?? runtimeType.Name, built, count);
        return _backgroundQueue.QueueValueTask(work, static (state, token) => state.Self.SendQueuedBatch(state, token), cancellationToken);
    }

    public async ValueTask InternalSendMessages<TMessage>(IList<TMessage> messages, CancellationToken cancellationToken = default)
        where TMessage : Messages.Base.Message
    {
        if (!_enabled || messages is null || messages.Count == 0 || cancellationToken.IsCancellationRequested)
            return;
        if (messages.Count == 1)
        {
            await InternalSendMessage(messages[0], cancellationToken).NoSync();
            return;
        }
        if (!ValidateQueues(messages, out string queue))
            return;

        Type runtimeType = messages[0].GetType();
        using IDisposable? scope = _sendScope(_logger, queue, runtimeType.FullName ?? runtimeType.Name);
        ServiceBusMessageBatch? batch = null;
        try
        {
            ServiceBusSender? sender = null;
            for (int i = 0; i < messages.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                TMessage message = messages[i];
                ServiceBusMessage? built = _serviceBusMessageUtil.BuildMessage(message, message.Type);
                if (built is null)
                    continue;

                LogPayload(built);
                sender ??= await _serviceBusSenderUtil.Get(queue, cancellationToken).NoSync();
                batch ??= await sender.CreateMessageBatchAsync(cancellationToken).NoSync();
                if (batch.TryAddMessage(built))
                    continue;

                if (batch.Count > 0)
                {
                    await sender.SendMessagesAsync(batch, cancellationToken).NoSync();
                    batch.Dispose();
                    batch = null;
                    batch = await sender.CreateMessageBatchAsync(cancellationToken).NoSync();
                    if (batch.TryAddMessage(built))
                        continue;
                }

                // The individual message may fit even when its batch envelope does not.
                // Keep batching subsequent messages and never serialize this one twice.
                try
                {
                    await sender.SendMessageAsync(built, cancellationToken).NoSync();
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to send individual message at index {Index}", i);
                }
            }

            if (batch is { Count: > 0 })
                await sender!.SendMessagesAsync(batch, cancellationToken).NoSync();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TX: error sending batch.");
        }
        finally
        {
            batch?.Dispose();
        }
    }

    private async ValueTask SendQueuedBatch(QueuedBatch work, CancellationToken cancellationToken)
    {
        if (work.Count == 1)
        {
            await SendSingle(work.Queue, work.TypeName, work.Messages[0], cancellationToken).NoSync();
            return;
        }

        using IDisposable? scope = _sendScope(_logger, work.Queue, work.TypeName);
        ServiceBusMessageBatch? batch = null;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            ServiceBusSender sender = await _serviceBusSenderUtil.Get(work.Queue, cancellationToken).NoSync();
            batch = await sender.CreateMessageBatchAsync(cancellationToken).NoSync();
            for (int i = 0; i < work.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ServiceBusMessage message = work.Messages[i];
                LogPayload(message);
                if (batch.TryAddMessage(message))
                    continue;

                if (batch.Count > 0)
                {
                    await sender.SendMessagesAsync(batch, cancellationToken).NoSync();
                    batch.Dispose();
                    batch = null;
                    try
                    {
                        batch = await CreateReplacementBatch(sender, cancellationToken).NoSync();
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogError(ex, "Failed to create new batch, falling back to individual message sending");
                        for (; i < work.Count; i++)
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            await sender.SendMessageAsync(work.Messages[i], cancellationToken).NoSync();
                        }
                        return;
                    }
                    if (batch.TryAddMessage(message))
                        continue;
                }

                await sender.SendMessageAsync(message, cancellationToken).NoSync();
            }

            if (batch.Count > 0)
                await sender.SendMessagesAsync(batch, cancellationToken).NoSync();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TX: error sending batch.");
        }
        finally
        {
            batch?.Dispose();
        }
    }

    private async ValueTask<ServiceBusMessageBatch> CreateReplacementBatch(ServiceBusSender sender, CancellationToken cancellationToken)
    {
        for (int retry = 0; ; retry++)
        {
            try
            {
                return await sender.CreateMessageBatchAsync(cancellationToken).NoSync();
            }
            catch (Exception ex) when (ex is not OperationCanceledException && retry < 2)
            {
                _logger.LogWarning(ex, "Failed to create new batch, retry {RetryCount}/{MaxRetries}", retry + 1, 3);
                await Task.Delay(100 * (retry + 1), cancellationToken).NoSync();
            }
        }
    }

    private bool ValidateQueues<TMessage>(IList<TMessage> messages, out string queue) where TMessage : Messages.Base.Message
    {
        queue = messages[0].Queue;
        for (int i = 1; i < messages.Count; i++)
        {
            if (messages[i].Queue == queue)
                continue;

            _logger.LogError("All messages in a batch must target the same queue. Expected: {ExpectedQueue}, Found: {FoundQueue} at index {Index}",
                queue, messages[i].Queue, i);
            return false;
        }
        return true;
    }

    private void LogPayload(ServiceBusMessage message)
    {
        if (_transmitterLogging && _logger.IsEnabled(LogLevel.Information))
            _logPayload(_logger, message.Body.ToString(), null);
    }
}
