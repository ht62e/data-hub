using DataHub.Contracts;
using Grpc.Core;

public sealed class HubCoordinator(
    HubState local,
    FederationOptions options,
    FederationNetwork federation,
    ReadOptions reads,
    IProviderReader reader,
    IReadRequestPolicy policy,
    IReadAuditSink audit) {
  public async Task<SourceList> List(CancellationToken ct, DateTime? deadline = null) {
    var catalog = await federation.Discover([], options.MaxHops, deadline, ct);
    var result = new SourceList { Truncated = catalog.Truncated };
    result.UnavailableHubs.AddRange(catalog.UnavailableHubs);
    result.Sources.AddRange(catalog.Sources.Select(SourceMappings.ToSource));
    return result;
  }
  private async Task<DiscoveredSource> Resolve(
      string id,
      CancellationToken ct,
      DateTime? deadline) {
    var (owner, localId) = DataProtocol.ParseSourceId(id);
    if (owner == options.HubId) {
      return local.GetSource(localId);
    }

    var catalog = await federation.Discover([], options.MaxHops, deadline, ct);
    var found = catalog.Sources.FirstOrDefault(e => e.QualifiedId == id);
    if (found is not null) {
      return found;
    }

    if (catalog.UnavailableHubs.Length > 0 || catalog.Truncated) {
      throw new RpcException(new Status(
          StatusCode.Unavailable,
          "Source not resolved: federation discovery is incomplete or hop/catalog limit was " +
          "reached"));
    }

    throw new KeyNotFoundException("Source is unavailable or its lease expired");
  }
  private async Task<string> BeginRead(
      DiscoveredSource source,
      int limit,
      string correlationId,
      string clientKind,
      CancellationToken ct) {
    if (limit < 0) {
      throw new ArgumentException("limit must be non-negative");
    }

    if (string.IsNullOrWhiteSpace(correlationId)) {
      correlationId = Guid.NewGuid().ToString("N");
    }

    if (!DataProtocol.IsIdentifier(correlationId)) {
      throw new ArgumentException("Invalid correlation ID");
    }

    await policy.ValidateAsync(
        new(source.QualifiedId, source.Descriptor.ApiVersion, limit, correlationId, clientKind),
        ct);
    audit.Record(new("Prepared", source.QualifiedId, correlationId));
    return correlationId;
  }
  public async Task<ReadReply> Read(string sourceId, int limit, bool rest, CancellationToken ct,
      DateTime? incomingDeadline = null, string correlationId = "") {
    var deadline = reads.Deadline(incomingDeadline);
    var source = await Resolve(sourceId, ct, deadline);
    correlationId = await BeginRead(source, limit, correlationId, rest ? "rest" : "grpc", ct);
    return await Transfer(source, limit, correlationId, deadline, ct);
  }
  // A relay is bound to an owning hub and a source snapshot, never a client selection or another
  // relay.
  public async Task<ReadReply> ReadLocalSource(
      RelayReadRequest request,
      CancellationToken ct,
      DateTime? incomingDeadline = null) {
    var (owner, localId) = DataProtocol.ParseSourceId(request.SourceId);
    if (owner != options.HubId) {
      throw new InvalidOperationException("Relay destination is not the owning hub");
    }

    var source = local.GetSource(localId);
    if (request.ApiVersion != source.Descriptor.ApiVersion) {
      throw new InvalidOperationException("Unsupported data API version");
    }

    var correlation = await BeginRead(source, request.Limit, request.CorrelationId, "relay", ct);
    return await Transfer(source, request.Limit, correlation, reads.Deadline(incomingDeadline), ct);
  }
  private async Task<ReadReply> Transfer(
      DiscoveredSource source,
      int limit,
      string correlationId,
      DateTime deadline,
      CancellationToken ct) {
    try {
      var result = await reader.ReadAsync(source, limit, correlationId, deadline, ct);
      audit.Record(new("Completed", source.QualifiedId, correlationId, result.Records.Count));
      return result;
    } catch {
      audit.Record(new("Failed", source.QualifiedId, correlationId));
      throw;
    }
  }
}
