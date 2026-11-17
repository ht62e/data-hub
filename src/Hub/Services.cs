using DataHub.Contracts;
using Grpc.Core;

public sealed class RegistryService(HubState state) : Registry.RegistryBase {
  public override Task<RegisterReply> Register(RegisterRequest request, ServerCallContext context) {
    state.Register(SourceMappings.FromRegistration(request));
    return Task.FromResult(new RegisterReply { LeaseSeconds = state.LeaseSeconds });
  }
}
public sealed class HubService(HubCoordinator coordinator) : HubApi.HubApiBase {
  public override Task<SourceList> ListSources(Empty request, ServerCallContext context) =>
      coordinator.List(context.CancellationToken, context.Deadline);

  public override Task<ReadReply> Read(ReadRequest request, ServerCallContext context) =>
      coordinator.Read(
          request.SourceId,
          request.Limit,
          false,
          context.CancellationToken,
          context.Deadline,
          request.CorrelationId);
}
public sealed class HubRelayService(HubCoordinator coordinator) : HubRelayApi.HubRelayApiBase {
  public override Task<ReadReply> ReadSource(
      RelayReadRequest request,
      ServerCallContext context) => coordinator.ReadLocalSource(
          request, context.CancellationToken, context.Deadline);
}
public sealed class FederationService(FederationNetwork network) : FederationApi.FederationApiBase {
  public override Task<FederationTopology> Negotiate(
      DiscoverRequest request,
      ServerCallContext context) {
    ValidateCaller(request.VisitedHubs, context);
    return network.Negotiate(
        request.VisitedHubs,
        request.RemainingHops,
        context.Deadline,
        context.CancellationToken);
  }
  private static void ValidateCaller(IReadOnlyList<string> visited, ServerCallContext context) {
    var caller = context.RequestHeaders.GetValue("x-federation-peer");
    if (visited.Count == 0 || visited[^1] != caller) {
      throw new ArgumentException("Federation path must end with the authenticated caller");
    }
  }
  public override async Task<FederationCatalog> Discover(
      DiscoverRequest request,
      ServerCallContext context) {
    ValidateCaller(request.VisitedHubs, context);
    var status = network.Status;
    if (!status.Ready) {
      throw new RpcException(new Status(
          StatusCode.Unavailable, "Federation topology is not verified: " + status.State));
    }
    var catalog = await network.Discover(
        request.VisitedHubs,
        request.RemainingHops,
        context.Deadline,
        context.CancellationToken,
        request.ExpectedInstanceId);
    return SourceMappings.ToFederationCatalog(catalog);
  }
}
