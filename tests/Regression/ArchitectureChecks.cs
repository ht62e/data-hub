using DataHub.Contracts;
using DataHub.Platform;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;

namespace DataHub.Regression;

internal static class ArchitectureChecks {
  public static async Task Run() {
    CheckTopology();
    CheckCatalogMerge();
    CheckSourceBoundary();
    await CheckVerifiedTopology();
    await CheckCoordinator();
  }

  private static void Check(bool condition, string description) {
    if (!condition)
      throw new Exception(description);
    Console.WriteLine("PASS " + description);
  }

  private static void Rejects<T>(Action action, string description) where T : Exception {
    try {
      action();
    } catch (T) { Console.WriteLine("PASS " + description); return; }
    throw new Exception(description);
  }

  private static async Task RejectsAsync<T>(Func<Task> action, string description) where T : Exception {
    try {
      await action();
    } catch (T) { Console.WriteLine("PASS " + description); return; }
    throw new Exception(description);
  }

  private static FederationNode Node(string id, params string[] children) {
    var node = new FederationNode { HubId = id, InstanceId = Guid.NewGuid().ToString("N") };
    node.DownstreamHubs.AddRange(children);
    return node;
  }

  private static void CheckTopology() {
    var a = Node("a", "b", "d");
    var b = Node("b", "c");
    var c = Node("c");
    var d = Node("d", "c");
    var diamond = FederationTopologyValidator.MergeAndValidate(a, [d, c, b, c.Clone()]);
    Check(diamond.Nodes.Select(n => n.HubId).SequenceEqual(["a", "b", "c", "d"]),
        "DAG accepts diamond joins and merges repeated hub identities deterministically");
    var cycle = c.Clone();
    cycle.DownstreamHubs.Add("a");
    Rejects<InvalidOperationException>(() => FederationTopologyValidator.MergeAndValidate(a, [b, cycle, d]), "DAG rejects a directed cycle");
    Rejects<InvalidOperationException>(() => FederationTopologyValidator.MergeAndValidate(a, [b, d]), "DAG rejects missing descendants");
    Rejects<InvalidOperationException>(() => FederationTopologyValidator.MergeAndValidate(a, [b, c, d, Node("orphan")]), "DAG rejects unreachable nodes");
    Rejects<InvalidOperationException>(() => FederationTopologyValidator.MergeAndValidate(a, [b, c, d, Node("c")]), "DAG rejects conflicting hub instances");
    Rejects<InvalidOperationException>(() => FederationTopologyValidator.MergeAndValidate(a, [b, c, d, cycle]), "DAG rejects conflicting downstream configurations");
  }

  private static DiscoveredSource Entry(string owner, string id, params string[] path) => new(owner, new("platform-" + owner, "data-hub"),
      new(id, "Source " + id, new ProviderAddress.Platform(new("platform-" + owner, "provider-" + id)), DataProtocol.ApiVersion), [.. path]);

  private static void CheckCatalogMerge() {
    var local = new SourceCatalog("a", [Entry("a", "log", "a")], []);
    var viaB = new SourceCatalog("b", [Entry("c", "log", "b", "c")], ["offline"]);
    var viaD = new SourceCatalog("d", [Entry("c", "log", "d", "c")], ["offline", "missing"], true);
    var merged = FederationCatalogMerger.Merge(local, [viaD, viaB]);
    var reverse = FederationCatalogMerger.Merge(local, [viaB, viaD]);
    Check(merged.Sources.Select(s => s.QualifiedId).SequenceEqual(["a/log", "c/log"]) &&
        merged.Sources[1].HubPath.SequenceEqual(["a", "b", "c"]) &&
        merged.Sources[1].HubPath.SequenceEqual(reverse.Sources[1].HubPath),
        "catalog keeps different owners distinct and breaks equal-length path ties independently of reply order");
    var direct = new SourceCatalog("c", [Entry("c", "log", "c")], []);
    Check(FederationCatalogMerger.Merge(local, [viaB, direct]).Sources[1].HubPath.SequenceEqual(["a", "c"]),
        "catalog chooses the shortest discovery path");
    Check(merged.Truncated && merged.UnavailableHubs.SequenceEqual(["missing", "offline"]),
        "catalog preserves partial results and deduplicates failure reports");
    Check(viaB.Sources[0].HubPath.SequenceEqual(["b", "c"]) && local.Sources.Length == 1,
        "catalog merge does not mutate input snapshots");
    var full = new SourceCatalog("a", [.. Enumerable.Range(0, FederationCatalogMerger.MaxSources)
            .Select(i => Entry("a", "log-" + i.ToString("D4"), "a"))], []);
    var limited = FederationCatalogMerger.Merge(full, [direct]);
    Check(limited.Truncated && limited.Sources.Length == FederationCatalogMerger.MaxSources && limited.Sources[^1].QualifiedId == "a/log-0999",
        "catalog enforces its size limit after deterministic deduplication and ordering");
    Check(!FederationCatalogMerger.Merge(full, []).Truncated, "exactly full catalog is not reported as truncated");
  }

  private static void CheckSourceBoundary() {
    var original = new SourceCatalog("b", [Entry("c", "log", "b", "c")], ["offline"], true);
    var wire = SourceMappings.ToFederationCatalog(original);
    var converted = SourceMappings.FromFederationCatalog(wire, "b", ["a"], 2);
    wire.Sources[0].Target.PlatformService.PlatformId = "changed";
    wire.Sources[0].HubPath.Clear();
    Check(converted.Sources[0].Descriptor == original.Sources[0].Descriptor &&
        converted.Sources[0].Owner == original.Sources[0].Owner && converted.Sources[0].HubPath.SequenceEqual(["b", "c"]) &&
        converted.Truncated && converted.UnavailableHubs.SequenceEqual(["offline"]),
        "protobuf conversion preserves metadata without sharing mutable message state");
    var invalidId = SourceMappings.ToFederationCatalog(original);
    invalidId.Sources[0].Source.Id = "log";
    Rejects<InvalidDataException>(() => SourceMappings.FromFederationCatalog(invalidId, "b", ["a"], 2),
        "unqualified wire source IDs are rejected");
    Rejects<InvalidDataException>(() => SourceMappings.FromFederationCatalog(SourceMappings.ToFederationCatalog(original), "b", ["c"], 2),
        "catalog boundary rejects a source path that revisits an ancestor");
    Rejects<InvalidDataException>(() => SourceMappings.FromFederationCatalog(SourceMappings.ToFederationCatalog(original), "b", ["a"], 1),
        "catalog boundary rejects paths longer than the remaining traversal budget");
    var noTarget = SourceMappings.ToFederationCatalog(original);
    noTarget.Sources[0].Target = null;
    Rejects<InvalidDataException>(() => SourceMappings.FromFederationCatalog(noTarget, "b", ["a"], 2),
        "catalog boundary rejects missing provider identity");
    var noOwner = SourceMappings.ToFederationCatalog(original);
    noOwner.Sources[0].Owner = null;
    Rejects<InvalidDataException>(() => SourceMappings.FromFederationCatalog(noOwner, "b", ["a"], 2),
        "catalog boundary rejects a missing owning-hub address");
    var external = Entry("c", "outside", "b", "c");
    external = external with {
      Descriptor = external.Descriptor with {
        Target = new ProviderAddress.External("node-c")
      }
    };
    var externalCatalog = new SourceCatalog("b", [external], []);
    Check(SourceMappings.FromFederationCatalog(SourceMappings.ToFederationCatalog(externalCatalog), "b", ["a"], 2).Sources[0].Descriptor == external.Descriptor,
        "external node identity survives federation without publishing an endpoint");
    Check(SourceMappings.ToSource(Entry("a", "log", "a")).Id == "a/log", "all public source IDs are qualified");
    Rejects<ArgumentException>(() => DataProtocol.ParseSourceId("log"), "local aliases are not accepted");
    var oldRead = ReadRequest.Parser.ParseFrom(new byte[] { 10, 5, 97, 47, 108, 111, 103 });
    Check(oldRead.SourceId.Length == 0, "legacy client IDs cannot be interpreted as source IDs");
    var registry = new HubState(new ConfigurationBuilder().Build(), new FederationOptions(), new PlatformOptions());
    foreach (var invalid in new ProviderAddress[] {
            new ProviderAddress.Platform(new("", "provider")), new ProviderAddress.Platform(null!), new ProviderAddress.External("bad/node")
        }) {
      Rejects<ArgumentException>(() => registry.Register(new("log", "Log", invalid, DataProtocol.ApiVersion)),
          "registry validates internal provider addresses directly: " + invalid.GetType().Name);
    }

    var identity = new ProviderAddress.External("node-c");
    var request = new ProviderReadRequest {
      ApiVersion = DataProtocol.ApiVersion,
      ExpectedSourceId = "log",
      ExpectedTarget = ProviderTargets.ToContract(identity),
      CorrelationId = "check"
    };
    Check(ProviderReadValidation.Validate(request, "log", identity) == "check", "external provider validates its node identity");
    request.ExpectedTarget = ProviderTargets.ToContract(new ProviderAddress.External("wrong-node"));
    Rejects<Grpc.Core.RpcException>(() => ProviderReadValidation.Validate(request, "log", identity), "wrong external node identity is rejected");
    request.ExpectedTarget = ProviderTargets.ToContract(new ProviderAddress.Platform(new("c", "provider")));
    Rejects<Grpc.Core.RpcException>(() => ProviderReadValidation.Validate(request, "log", identity), "platform identity cannot match an external provider");
  }

  private static async Task CheckCoordinator() {
    var config = new ConfigurationBuilder().Build();
    var options = new FederationOptions { HubId = "a" };
    var platformOptions = new PlatformOptions { Id = "platform-a" };
    var local = new HubState(config, options, platformOptions);
    var source = new SourceDescriptor("log", "Log", new ProviderAddress.Platform(new("p", "provider")), DataProtocol.ApiVersion);
    var outside = new SourceDescriptor("external", "External", new ProviderAddress.External("node-a"), DataProtocol.ApiVersion);
    local.Register(source);
    local.Register(outside);
    using var platform = new StaticPlatformGrpcClientFactory(platformOptions);
    var network = new FederationNetwork(options, local, platform, NullLogger<FederationNetwork>.Instance);
    var reader = new RecordingReader();
    var policy = new RecordingPolicy();
    var audit = new RecordingAudit();
    var readOptions = new ReadOptions { TimeoutSeconds = 2 };
    var hub = new HubCoordinator(local, options, network, readOptions, reader, policy, audit);
    using var cancel = new CancellationTokenSource();
    var deadline = DateTime.UtcNow.AddSeconds(1);
    var reply = await hub.Read("a/log", 7, false, cancel.Token, deadline, "trace-1");
    Check(reader.Calls.Count == 1 && reader.Calls[0] == new ReadCall(local.GetSource("log"), 7, "trace-1", deadline, cancel.Token) && reply.Records.Count == 1,
        "coordinator passes source, limit, correlation, deadline and cancellation through IProviderReader");
    Check(policy.Requests[^1] == new ReadAccessRequest("a/log", DataProtocol.ApiVersion, 7, "trace-1", "grpc") &&
        audit.Events.Select(e => e.Phase).SequenceEqual(["Prepared", "Completed"]) && audit.Events[^1].RecordCount == 1,
        "successful transfer retains policy evaluation and audit correlation");

    policy.Reject = true;
    await RejectsAsync<InvalidOperationException>(() => hub.Read("a/log", 7, false, default, correlationId: "denied"), "policy rejection blocks provider transfer");
    Check(reader.Calls.Count == 1 && audit.Events.Count == 2, "rejected requests produce no provider calls or completed audits");
    policy.Reject = false;
    await RejectsAsync<ArgumentException>(() => hub.Read("log", 7, false, default),
        "unqualified source IDs are rejected before transfer");
    await RejectsAsync<ArgumentException>(() => hub.Read("a/log", -1, false, default),
        "negative read limits are rejected before transfer");
    Check(reader.Calls.Count == 1, "invalid requests do not reach the provider");
    var relay = new RelayReadRequest { SourceId = "other/external", ApiVersion = DataProtocol.ApiVersion, Limit = 1, CorrelationId = "relay" };
    await RejectsAsync<InvalidOperationException>(() => hub.ReadLocalSource(relay, default), "relay refuses another hub's source instead of forwarding");
    relay.SourceId = "a/external";
    relay.ApiVersion = "unsupported";
    await RejectsAsync<InvalidOperationException>(() => hub.ReadLocalSource(relay, default), "relay checks API version before transfer");
    Check(reader.Calls.Count == 1, "invalid relay requests do not reach a provider");
    relay.ApiVersion = DataProtocol.ApiVersion;
    var relayDeadline = DateTime.UtcNow.AddSeconds(1);
    await hub.ReadLocalSource(relay, default, relayDeadline);
    Check(reader.Calls[^1].Source.Descriptor == outside && policy.Requests[^1].ClientKind == "relay" && reader.Calls[^1].Deadline == relayDeadline,
        "owner relay resolves local registration, runs its policy and preserves the shorter incoming deadline");

    reader.Fail = true;
    audit.Events.Clear();
    await RejectsAsync<IOException>(() => hub.Read("a/external", 7, false, default, correlationId: "trace-failure"), "provider transfer errors propagate to the API boundary");
    Check(audit.Events.Select(e => e.Phase).SequenceEqual(["Prepared", "Failed"]) && audit.Events.All(e => e.CorrelationId == "trace-failure"),
        "failed transfers retain correlated failure audits");
    cancel.Cancel();
    await RejectsAsync<OperationCanceledException>(() => hub.Read("a/external", 7, false, cancel.Token), "cancelled requests stop before provider transfer");
    Check(reader.Calls.Count == 3, "cancellation does not initiate another transfer");
    reader.Fail = false;
    var before = DateTime.UtcNow;
    await hub.Read("a/external", 1, false, default, DateTime.UtcNow.AddMinutes(1));
    var after = DateTime.UtcNow;
    Check(reader.Calls[^1].Deadline >= before.AddSeconds(2) && reader.Calls[^1].Deadline <= after.AddSeconds(2),
        "read timeout is capped by read configuration independently of federation settings");
    Rejects<InvalidOperationException>(() => new ReadOptions { TimeoutSeconds = 0 }.Validate(), "invalid read timeout is rejected at startup");
  }

  private static async Task CheckVerifiedTopology() {
    var options = new FederationOptions { HubId = "solo" };
    var platformOptions = new PlatformOptions();
    var local = new HubState(new ConfigurationBuilder().Build(), options, platformOptions);
    using var platform = new StaticPlatformGrpcClientFactory(platformOptions);
    var network = new FederationNetwork(options, local, platform, NullLogger<FederationNetwork>.Instance);
    network.Rejected(new Grpc.Core.RpcException(new(Grpc.Core.StatusCode.Unavailable, "offline")));
    Check(!network.Status.Ready && network.Status.State == "Unavailable", "no topology is admitted before initial verification");
    var topology = await network.Negotiate([], 4, null, default);
    network.Negotiated(topology);
    var validatedAt = network.Status.LastValidatedAt;
    network.Rejected(new Grpc.Core.RpcException(new(Grpc.Core.StatusCode.Unavailable, "offline")));
    Check(network.Status.Ready && network.Status.State == "Degraded" && network.Status.LastValidatedAt == validatedAt,
        "revalidation failure retains the previously verified topology and its validation time");
    await RejectsAsync<Grpc.Core.RpcException>(() => network.Discover([], 0, null, default, Guid.NewGuid().ToString("N")),
        "discovery refuses an unverified peer instance");
    await RejectsAsync<ArgumentException>(() => network.Discover([], 0, null, default, ""), "remote discovery requires an instance pin");
    await network.Discover([], 0, null, default, topology.Nodes.Single().InstanceId);
    Console.WriteLine("PASS verified peer instance remains usable during revalidation failure");
    network.Negotiated(topology);
    Check(network.Status.Ready && network.Status.State == "Ready" && network.Status.Error is null, "successful revalidation clears degraded state");
  }

  private sealed record ReadCall(DiscoveredSource Source, int Limit, string CorrelationId, DateTime Deadline, CancellationToken CancellationToken);
  private sealed class RecordingReader : IProviderReader {
    public List<ReadCall> Calls { get; } = [];
    public bool Fail {
      get; set;
    }
    public Task<ReadReply> ReadAsync(DiscoveredSource source, int limit, string correlationId, DateTime deadline, CancellationToken ct) {
      Calls.Add(new(source, limit, correlationId, deadline, ct));
      if (Fail)
        throw new IOException("Source unavailable");
      var reply = new ReadReply { CorrelationId = correlationId };
      reply.Records.Add(new LogRecord { Message = "sample" });
      return Task.FromResult(reply);
    }
  }
  private sealed class RecordingPolicy : IReadRequestPolicy {
    public List<ReadAccessRequest> Requests { get; } = [];
    public bool Reject {
      get; set;
    }
    public Task ValidateAsync(ReadAccessRequest request, CancellationToken ct) {
      ct.ThrowIfCancellationRequested();
      Requests.Add(request);
      if (Reject)
        throw new InvalidOperationException("Request denied");
      return Task.CompletedTask;
    }
  }
  private sealed class RecordingAudit : IReadAuditSink {
    public List<ReadAuditEvent> Events { get; } = [];
    public void Record(ReadAuditEvent entry) => Events.Add(entry);
  }
}
