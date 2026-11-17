using System.Collections.Immutable;
using DataHub.Contracts;
using DataHub.Platform;
using Grpc.Core;

public sealed class FederationNetwork(
    FederationOptions options,
    HubState local,
    IPlatformGrpcClientFactory platform,
    ILogger<FederationNetwork> logger) {
  private sealed record VerifiedView(
      FederationStatus Status,
      ImmutableDictionary<string, string> Instances);
  private readonly string _instanceId = Guid.NewGuid().ToString("N");
  private VerifiedView _view = new(
      new(false, "Starting", null, "Initial topology negotiation has not completed"),
      ImmutableDictionary<string, string>.Empty);
  public FederationStatus Status => Volatile.Read(ref _view).Status;
  public void Negotiated(FederationTopology topology) {
    // Only the background negotiator publishes fully validated graphs. Never persist a partial
    // graph.
    var instances = topology.Nodes.ToImmutableDictionary(
        node => node.HubId, node => node.InstanceId, StringComparer.Ordinal);
    Volatile.Write(ref _view, new(new(true, "Ready", DateTimeOffset.UtcNow, null), instances));
  }
  public void Rejected(Exception e) {
    var current = Volatile.Read(ref _view);
    var state = current.Status.Ready ? "Degraded" :
        e is RpcException { StatusCode: StatusCode.FailedPrecondition } or InvalidOperationException
            ? "Rejected"
            : "Unavailable";
    // Static edges remain verified for the same process instances. Discovery pins every peer to
    // this view, so a restart/configuration change cannot silently activate an unverified edge.
    Volatile.Write(ref _view, current with {
      Status = new(current.Status.Ready, state, current.Status.LastValidatedAt, e.Message)
    });
  }
  private FederationApi.FederationApiClient Client(FederationPeer peer) =>
      new(platform.Create(new(peer.PlatformId, peer.ServiceId)));

  private static DateTime Earlier(DateTime configured, DateTime? incoming) =>
      incoming is not null && incoming < configured ? incoming.Value : configured;
  private Metadata Headers(FederationPeer peer) => new() {
    { "x-federation-peer", options.HubId },
    { "x-federation-key", peer.ApiKey }
  };

  // Called during startup and periodically. A new graph is admitted only after complete DAG
  // validation. Incoming negotiation does not depend on the receiver's background worker, so
  // concurrent startup is safe.
  public async Task<FederationTopology> Negotiate(
      IReadOnlyList<string> visited,
      int remaining,
      DateTime? incomingDeadline,
      CancellationToken ct) {
    if (visited.Contains(options.HubId, StringComparer.Ordinal)) {
      throw new InvalidOperationException(
          "Directed cycle rejected: " + string.Join(" -> ", visited.Append(options.HubId)));
    }

    ValidateTraversal(visited, remaining);
    if (remaining == 0 && options.Peers.Count > 0) {
      throw new InvalidOperationException(
          "Topology exceeds four outgoing hops; cannot verify the entire graph safely");
    }

    var deadline = Earlier(
        DateTime.UtcNow.AddSeconds(options.DiscoveryTimeoutSeconds), incomingDeadline);
    var request = new DiscoverRequest { RemainingHops = remaining - 1 };
    request.VisitedHubs.AddRange(visited.Append(options.HubId));
    var replies = await Task.WhenAll(options.Peers.Select(async peer => {
      var reply = await Client(peer).NegotiateAsync(request, Headers(peer), deadline, ct);
      if (reply.HubId != peer.HubId) {
        throw new InvalidOperationException(
            "Negotiated peer identity does not match configuration");
      }

      return reply;
    }));
    var self = new FederationNode { HubId = options.HubId, InstanceId = _instanceId };
    self.DownstreamHubs.AddRange(options.Peers.Select(p => p.HubId).Order(StringComparer.Ordinal));
    return FederationTopologyValidator.MergeAndValidate(self, replies.SelectMany(r => r.Nodes));
  }

  public async Task<SourceCatalog> Discover(
      IReadOnlyList<string> visited,
      int remaining,
      DateTime? incomingDeadline,
      CancellationToken ct,
      string? expectedInstanceId = null) {
    if (expectedInstanceId is not null) {
      if (!Guid.TryParse(expectedInstanceId, out _)) {
        throw new ArgumentException("Discovery requires a negotiated peer instance ID");
      }

      if (expectedInstanceId != _instanceId) {
        throw new RpcException(new Status(
            StatusCode.FailedPrecondition,
            "Peer restarted; topology renegotiation is required"));
      }
    }
    ValidateTraversal(visited, remaining);
    ct.ThrowIfCancellationRequested();
    var current = Volatile.Read(ref _view);
    // Reserve response time at each hop so a child's timeout can return a partial catalog upstream.
    var deadline = Earlier(
        DateTime.UtcNow.AddSeconds(options.DiscoveryTimeoutSeconds),
        incomingDeadline?.AddMilliseconds(-100));
    remaining = Math.Min(remaining, options.MaxHops);
    var localSources = local.List();
    var result = new SourceCatalog(options.HubId,
        [.. localSources.Take(FederationCatalogMerger.MaxSources)], [],
        localSources.Count > FederationCatalogMerger.MaxSources);
    if (options.Peers.Count == 0) {
      return result;
    }
    if (!current.Status.Ready) {
      return result with {
        UnavailableHubs = [.. options.Peers.Select(p => p.HubId)]
      };
    }

    var path = visited.Append(options.HubId).ToArray();
    if (options.Peers.Any(p => path.Contains(p.HubId, StringComparer.Ordinal))) {
      throw new InvalidOperationException(
          "Directed cycle detected after negotiation; federation forwarding rejected");
    }

    if (remaining == 0) {
      return result with {
        Truncated = true
      };
    }

    var responses = await Task.WhenAll(options.Peers.Select(async peer => {
      try {
        var request = new DiscoverRequest {
          RemainingHops = remaining - 1,
          ExpectedInstanceId = current.Instances[peer.HubId]
        };
        request.VisitedHubs.AddRange(path);
        var reply = await Client(peer).DiscoverAsync(request, Headers(peer), deadline, ct);
        return SourceMappings.FromFederationCatalog(reply, peer.HubId, path, remaining);
      } catch (Exception e) when (
          !ct.IsCancellationRequested && e is RpcException or InvalidDataException) {
        logger.LogWarning(
            "Federation discovery failed for {Peer}: {Reason}", peer.HubId, e.Message);
        return new SourceCatalog(peer.HubId, [], [peer.HubId]);
      }
    }));
    ct.ThrowIfCancellationRequested();
    return FederationCatalogMerger.Merge(result, responses);
  }
  private void ValidateTraversal(IReadOnlyList<string> visited, int remaining) {
    if (remaining is < 0 or > 4 || visited.Count + remaining > 4 ||
        visited.Any(id => !FederationOptions.IsId(id)) ||
        visited.Distinct(StringComparer.Ordinal).Count() != visited.Count ||
        visited.Contains(options.HubId, StringComparer.Ordinal)) {
      throw new ArgumentException("Invalid, cyclic or excessive federation discovery path");
    }
  }
}
