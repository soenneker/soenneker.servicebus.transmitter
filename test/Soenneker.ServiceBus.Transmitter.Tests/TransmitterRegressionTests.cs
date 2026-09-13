using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Audit;

public class TransmitterRegressionTests
{
    private static void Check(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task BatchSplitsWithoutLosingOrDuplicatingMessages(bool queued)
    {
        var f = new Fixture();
        var input = Enumerable.Range(0, 7).Select(i => Payload.Create(i.ToString())).ToArray();
        await f.Transmitter.SendMessages(input, queued);
        if (queued) await f.DrainOne();
        Check(f.Sender.BatchSends == 4, "Expected four batches");
        Check(f.Sender.Sent.Count == 7 && f.Builder.Calls == 7, "Every message must be built and sent once");
        for (int i = 0; i < 7; i++)
            Check(f.Sender.Sent[i].Body.ToString().Contains($"\"content\":\"{i}\""), "Message order changed");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OversizedBatchEnvelopeOnlyFallsBackForThatMessage(bool queued)
    {
        var f = new Fixture();
        var input = Enumerable.Range(0, 6).Select(i => Payload.Create(i.ToString())).ToArray();
        input[2].Id = "oversize";
        await f.Transmitter.SendMessages(input, queued);
        if (queued) await f.DrainOne();
        Check(f.Sender.SingleSends == 1, "Only oversized message should use individual send");
        Check(f.Sender.Sent.Count == 6 && f.Builder.Calls == 6, "No duplicate serialization or send");
        Check(f.Sender.BatchSends == 3, "Later messages must remain batched");
    }

    [Test]
    public async Task RejectedMessagesDoNotTriggerAzureAccess()
    {
        var f = new Fixture(); f.Builder.RejectAll = true;
        await f.Transmitter.SendMessage(Payload.Create(), false);
        await f.Transmitter.SendMessages(new[] { Payload.Create(), Payload.Create() }, false);
        Check(f.Senders.Gets == 0 && f.Sender.CreatedBatches == 0, "Rejected bodies should not create clients or batches");
    }

    [Test]
    public async Task MixedQueuesAreRejectedBeforeSerialization()
    {
        var f = new Fixture(); var one = Payload.Create(); var two = Payload.Create(); two.Queue = "other";
        await f.Transmitter.SendMessages(new[] { one, two }, false);
        await f.Transmitter.SendMessages(new[] { one, two }, true);
        Check(f.Builder.Calls == 0 && f.Senders.Gets == 0, "Mixed queues must never be sent");
    }

    [Test]
    public async Task QueuedPayloadIsSnapshotAndRejectedSlotsAreNotSent()
    {
        var f = new Fixture(true);
        var one = Payload.Create("before"); var rejected = Payload.Create(); rejected.Id = "reject";
        await f.Transmitter.SendMessages(new[] { one, rejected }, true);
        one.Content = "after";
        await f.DrainOne();
        Check(f.Sender.Sent.Count == 1 && f.Sender.SingleSends == 1, "Only valid message should be sent");
        Check(f.Sender.Sent[0].Body.ToString().Contains("before"), "Queued payload retained mutable application data");
    }

    [Test]
    public async Task CancellationAvoidsSerializationAndBatchRetry()
    {
        var f = new Fixture(); using var canceled = new CancellationTokenSource(); canceled.Cancel();
        try { await f.Transmitter.SendMessage(Payload.Create(), true, canceled.Token); }
        catch (OperationCanceledException) { }
        Check(f.Builder.Calls == 0, "Canceled queued request still serialized");
        f.Sender.CancelCreation = true;
        await f.Transmitter.SendMessages(new[] { Payload.Create(), Payload.Create() });
        await f.DrainOne();
        Check(f.Sender.CreatedBatches == 1, "Cancellation must not be retried");
    }

    [Test]
    public async Task FailedReplacementFallsBackWithoutResendingCompletedBatch()
    {
        var f = new Fixture(); f.Sender.FailReplacement = true;
        await f.Transmitter.SendMessages(Enumerable.Range(0, 5).Select(i => Payload.Create(i.ToString())).ToArray());
        await f.DrainOne();
        Check(f.Sender.CreatedBatches == 4 && f.Sender.BatchSends == 1 && f.Sender.SingleSends == 3, "Retry or fallback count changed");
        Check(f.Sender.Sent.Count == 5, "Fallback lost or duplicated messages");
    }
}
