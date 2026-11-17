using System.Text.Json;
using DataHub.Contracts;
using DataHub.Platform;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Configuration;

namespace DataHub.Regression;

internal static class ResourceChecks {
  private static IConfiguration Config(params (string Key, string Value)[] values) => new ConfigurationBuilder()
      .AddInMemoryCollection(values.Select(v => new KeyValuePair<string, string?>(v.Key, v.Value))).Build();
  private static void Check(bool condition, string message) {
    if (!condition) {
      throw new Exception(message);
    }

    Console.WriteLine("PASS " + message);
  }
  private static T Rejects<T>(Action action) where T : Exception {
    try {
      action();
    } catch (T error) { return error; }
    throw new Exception("Expected " + typeof(T).Name);
  }
  private static async Task<T> RejectsAsync<T>(Func<Task> action) where T : Exception {
    try {
      await action();
    } catch (T error) { return error; }
    throw new Exception("Expected " + typeof(T).Name);
  }
  public static async Task Run() {
    await RpcBoundary();
    await Registrations();
    await PendingReads();
    await Payloads();
  }
  private static Task<Empty> ThroughRpcBoundary(Exception error) =>
      new RpcErrorInterceptor().UnaryServerHandler<Empty, Empty>(
          new(), null!, (_, _) => Task.FromException<Empty>(error));

  private static async Task RpcBoundary() {
    (Exception Error, StatusCode Code)[] cases = [
      (new RegistrationConflictException(), StatusCode.AlreadyExists),
      (new KeyNotFoundException(), StatusCode.NotFound),
      (new ArgumentException(), StatusCode.InvalidArgument),
      (new InvalidOperationException(), StatusCode.FailedPrecondition),
      (new OperationCanceledException(), StatusCode.Cancelled)
    ];
    foreach (var (error, code) in cases) {
      var mapped = await RejectsAsync<RpcException>(() => ThroughRpcBoundary(error));
      Check(mapped.StatusCode == code, "gRPC boundary maps " + error.GetType().Name);
    }
    var payload = await RejectsAsync<RpcException>(() => ThroughRpcBoundary(
        new ResourceLimitException(ResourceLimitKind.Payload, "too large")));
    Check(payload.StatusCode == StatusCode.ResourceExhausted &&
        payload.Trailers.GetValue(ResourceLimitErrors.Trailer) == "payload",
        "gRPC boundary preserves the resource-limit classification");
    var capacity = new ResourceLimitException(ResourceLimitKind.Capacity, "queue full")
        .ToRpcException();
    Check(capacity.StatusCode == StatusCode.ResourceExhausted &&
        capacity.Trailers.GetValue(ResourceLimitErrors.Trailer) == "capacity" &&
        ResourceLimitErrors.HttpStatus(capacity) == 429,
        "provider queue capacity remains classified as gRPC ResourceExhausted and REST 429");
    var upstream = new RpcException(new(StatusCode.ResourceExhausted, "upstream"),
        new Metadata { { ResourceLimitErrors.Trailer, "capacity" } });
    Check(ReferenceEquals(upstream,
        await RejectsAsync<RpcException>(() => ThroughRpcBoundary(upstream))),
        "upstream gRPC errors and trailers pass through unchanged");
    var unexpected = new NullReferenceException("programming error");
    Check(ReferenceEquals(unexpected,
        await RejectsAsync<NullReferenceException>(() => ThroughRpcBoundary(unexpected))),
        "unexpected failures are not disguised as retryable unavailability");
  }
  private static async Task Registrations() {
    var clock = new ManualClock();
    var state = new HubState(Config(("LeaseSeconds", "10")), new(), new(), clock);
    var first = new SourceDescriptor("log", "First", new ProviderAddress.External("first"), DataProtocol.ApiVersion);
    var second = first with {
      Target = new ProviderAddress.External("second")
    };
    state.Register(first);
    clock.Advance(9);
    state.Register(first with {
      DisplayName = "Renewed"
    });
    clock.Advance(2);
    Rejects<RegistrationConflictException>(() => state.Register(second));
    Check(state.GetSource("log").Descriptor.Target == first.Target && state.GetSource("log").Descriptor.DisplayName == "Renewed",
        "same-provider renewal extends the lease while a different provider cannot replace it");
    clock.Advance(9);
    state.Register(second);
    Check(state.GetSource("log").Descriptor.Target == second.Target, "another provider can claim the ID after lease expiry");
    var snapshot = state.List();
    clock.Advance(10);
    Check(state.List().Count == 0 && snapshot.Single().Descriptor.Target == second.Target,
        "registry cleanup expires leases without changing previously returned snapshots");
    Rejects<KeyNotFoundException>(() => state.GetSource("log"));

    var race = new HubState(Config(), new(), new());
    var winners = await Task.WhenAll(Enumerable.Range(0, 32).Select(i => Task.Run(() => {
      try {
        race.Register(first with {
          Target = new ProviderAddress.External("node-" + i)
        });
        return 1;
      } catch (RegistrationConflictException) { return 0; }
    })));
    Check(winners.Sum() == 1, "concurrent claims for the same source ID admit exactly one provider");
  }
  private static async Task PendingReads() {
    using var limiter = new ReadLimiter(Config(("MaxPendingReads", "1"), ("MinIntervalMs", "100")), new NoLoad());
    var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var first = limiter.RunAsync(async ct => { entered.SetResult(); await release.Task.WaitAsync(ct); return 1; }, default);
    await entered.Task;
    using var cancel = new CancellationTokenSource();
    var queuedCalls = 0;
    var queued = limiter.RunAsync(ct => { queuedCalls++; return Task.FromResult(2); }, cancel.Token);
    var overflow = await RejectsAsync<ResourceLimitException>(() => limiter.RunAsync(ct => Task.FromResult(3), default));
    Check(overflow.Kind == ResourceLimitKind.Capacity && queuedCalls == 0, "full provider queue rejects excess requests before source I/O");
    cancel.Cancel();
    await RejectsAsync<OperationCanceledException>(() => queued);
    var replacement = limiter.RunAsync(ct => Task.FromResult(4), default);
    release.SetResult();
    Check(await first == 1 && await replacement == 4 && queuedCalls == 0, "cancelled waiters release admission capacity without releasing the active gate");
    await RejectsAsync<IOException>(() => limiter.RunAsync<int>(ct => throw new IOException("source failed"), default));
    Check(await limiter.RunAsync(ct => Task.FromResult(5), default) == 5, "source failure releases execution and admission slots");
  }
  private static async Task Payloads() {
    var options = new ReadPayloadLimits { MaxRecordBytes = 1024, MaxResponseBytes = 4096 };
    var records = new[] { new LogRecord { Message = "日本語\"\\\n" }, new LogRecord { Message = "normal" } };
    var reply = await options.CreateReplyAsync(Stream(records), "trace", default);
    Check(reply.Records.Count == 2 && reply.CalculateSize() <= options.MaxResponseBytes,
        "valid payload fits the protobuf response budget");
    var tooLarge = await RejectsAsync<ResourceLimitException>(() => options.CreateReplyAsync(Stream([new() { Message = new string('x', 1025) }]), "trace", default));
    Check(tooLarge.Kind == ResourceLimitKind.Payload, "single oversized record is rejected");
    var escapedRecord = new LogRecord { Message = new string('日', 180) };
    var escaped = await options.CreateReplyAsync(Stream([escapedRecord]), "trace", default);
    var escapedJsonBytes = JsonSerializer.SerializeToUtf8Bytes(
        escapedRecord, JsonSerializerOptions.Web).Length;
    Check(escaped.Records.Count == 1 && escapedRecord.CalculateSize() <= options.MaxRecordBytes &&
        escapedJsonBytes > options.MaxRecordBytes,
        "JSON escaping does not reject a record within the protobuf limit");

    var single = await options.CreateReplyAsync(Stream([new() { Message = new string('x', 300) }]), "trace", default);
    var exact = single.CalculateSize();
    var recordSize = single.Records[0].CalculateSize();
    var boundary = new ReadPayloadLimits { MaxRecordBytes = recordSize, MaxResponseBytes = exact };
    await boundary.CreateReplyAsync(Stream([new() { Message = new string('x', 300) }]), "trace", default);
    var below = new ReadPayloadLimits { MaxRecordBytes = recordSize, MaxResponseBytes = exact - 1 };
    Check((await RejectsAsync<ResourceLimitException>(() => below.CreateReplyAsync(Stream([new() { Message = new string('x', 300) }]), "trace", default))).Kind == ResourceLimitKind.Payload,
        "protobuf response budget includes its envelope and accepts the exact byte boundary");
    below = new ReadPayloadLimits { MaxRecordBytes = recordSize - 1, MaxResponseBytes = exact };
    Check((await RejectsAsync<ResourceLimitException>(() => below.CreateReplyAsync(
        Stream([new() { Message = new string('x', 300) }]), "trace", default))).Kind ==
        ResourceLimitKind.Payload,
        "protobuf record budget accepts the exact boundary and rejects one byte over");

    foreach (int length in new[] {
        0, 124, 125, 126, 127, 128, 16379, 16380, 16381, 16382, 16383, 16384
    }) {
      var data = new[] { new LogRecord { Message = new string('x', 300) },
          new LogRecord { Message = new string('x', length) } };
      var expected = new ReadReply { CorrelationId = "trace" };
      expected.Records.AddRange(data);
      var sizeLimit = new ReadPayloadLimits {
        MaxRecordBytes = data.Max(record => record.CalculateSize()),
        MaxResponseBytes = expected.CalculateSize()
      };
      var actual = await sizeLimit.CreateReplyAsync(Stream(data), "trace", default);
      Check(actual.CalculateSize() == expected.CalculateSize(),
          "protobuf response size includes length prefixes at message length " + length);
      sizeLimit.MaxResponseBytes--;
      await RejectsAsync<ResourceLimitException>(
          () => sizeLimit.CreateReplyAsync(Stream(data), "trace", default));
    }

    var disposed = false;
    var consumed = 0;
    async IAsyncEnumerable<LogRecord> Many() {
      try {
        for (int i = 0; i < 100; i++) {
          consumed++;
          yield return new() {
            Message = new string('x', 600)
          };
        }
        await Task.CompletedTask;
      } finally { disposed = true; }
    }
    await RejectsAsync<ResourceLimitException>(() => options.CreateReplyAsync(Many(), "trace", default));
    Check(disposed && consumed < 100, "response overflow stops enumeration and disposes the source without returning partial success");
    Rejects<InvalidOperationException>(() => new ReadPayloadLimits { MaxResponseBytes = DataProtocol.MaxGrpcMessageBytes + 1 }.Validate());
    Console.WriteLine("PASS response budget cannot exceed the shared gRPC transport limit");
  }
  private static async IAsyncEnumerable<LogRecord> Stream(IEnumerable<LogRecord> records) {
    foreach (var record in records) {
      yield return record;
    }

    await Task.CompletedTask;
  }
  private sealed class ManualClock : TimeProvider {
    private DateTimeOffset _now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");
    public override DateTimeOffset GetUtcNow() => _now;
    public void Advance(int seconds) => _now = _now.AddSeconds(seconds);
  }
  private sealed class NoLoad : ILoadMonitor {
    public Task<LoadSample?> SampleAsync(CancellationToken ct) => Task.FromResult<LoadSample?>(null);
  }
}
