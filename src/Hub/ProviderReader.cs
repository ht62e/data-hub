using DataHub.Contracts;
using DataHub.Platform;

public interface IProviderReader {
  Task<ReadReply> ReadAsync(
      DiscoveredSource source,
      int limit,
      string correlationId,
      DateTime deadline,
      CancellationToken ct);
}

public sealed class GrpcProviderReader(
    FederationOptions options,
    IPlatformGrpcClientFactory platform,
    ExternalNodeClients external) : IProviderReader {
  public async Task<ReadReply> ReadAsync(
      DiscoveredSource source,
      int limit,
      string correlationId,
      DateTime deadline,
      CancellationToken ct) {
    // An external node is reachable only from its owning hub/platform. Reach that hub through MSA,
    // regardless of which DAG path discovered it. The owning hub never forwards this RPC
    // further.
    if (source.Descriptor.Target is ProviderAddress.External &&
        source.OriginHubId != options.HubId) {
      using var relay = new HubRelayApi.HubRelayApiClient(
          platform.Create(source.Owner)).ReadSourceAsync(
          new RelayReadRequest {
            SourceId = source.QualifiedId,
            ApiVersion = source.Descriptor.ApiVersion,
            Limit = limit,
            CorrelationId = correlationId
          },
          deadline: deadline, cancellationToken: ct);
      return await relay.ResponseAsync;
    }
    var invoker = source.Descriptor.Target switch {
      ProviderAddress.Platform p => platform.Create(p.Service),
      ProviderAddress.External e => external.Create(e.NodeId),
      _ => throw new InvalidOperationException("Unknown provider location")
    };
    using var call = new ProviderApi.ProviderApiClient(invoker).ReadAsync(new ProviderReadRequest {
      Limit = limit,
      ApiVersion = source.Descriptor.ApiVersion,
      CorrelationId = correlationId,
      ExpectedSourceId = source.Descriptor.Id,
      ExpectedTarget = ProviderTargets.ToContract(source.Descriptor.Target)
    }, deadline: deadline, cancellationToken: ct);
    return await call.ResponseAsync;
  }
}
