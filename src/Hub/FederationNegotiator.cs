using Grpc.Core;

public sealed record FederationStatus(
    bool Ready,
    string State,
    DateTimeOffset? LastValidatedAt,
    string? Error);

public sealed class FederationNegotiator(
    FederationNetwork network,
    FederationOptions options,
    ILogger<FederationNegotiator> logger) : BackgroundService {
  protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
    while (!stoppingToken.IsCancellationRequested) {
      int delay;
      try {
        var topology = await network.Negotiate([], 4, null, stoppingToken);
        network.Negotiated(topology);
        logger.LogInformation("Federation DAG verified for {HubId}", options.HubId);
        delay = options.NegotiationIntervalSeconds;
      } catch (Exception e) when (e is RpcException or InvalidOperationException) {
        stoppingToken.ThrowIfCancellationRequested();
        network.Rejected(e);
        logger.LogWarning(
            "Federation negotiation {State}: {Reason}", network.Status.State, e.Message);
        delay = 2;
      }
      await Task.Delay(TimeSpan.FromSeconds(delay), stoppingToken);
    }
  }
}
