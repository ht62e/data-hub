using DataHub.Contracts;
using DataHub.Platform;
using Grpc.Core;

public sealed record ProviderRegistration(
    string SourceId,
    string DisplayName,
    ProviderAddress Target);

public sealed class RegistrationWorker(
    Registry.RegistryClient registry,
    ProviderRegistration registration,
    ILogger<RegistrationWorker> logger) : BackgroundService {
  protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
    var request = new RegisterRequest {
      SourceId = registration.SourceId,
      DisplayName = registration.DisplayName,
      Target = ProviderTargets.ToContract(registration.Target),
      ApiVersion = DataProtocol.ApiVersion
    };
    var retry = 2;
    while (!stoppingToken.IsCancellationRequested) {
      int delay;
      try {
        var reply = await registry.RegisterAsync(
            request, deadline: DateTime.UtcNow.AddSeconds(5), cancellationToken: stoppingToken);
        retry = 2;
        delay = Math.Max(1, reply.LeaseSeconds / 3);
        logger.LogInformation(
            "Registered source {SourceId}; lease {LeaseSeconds}s",
            request.SourceId, reply.LeaseSeconds);
      } catch (RpcException e) {
        stoppingToken.ThrowIfCancellationRequested();
        logger.LogWarning(e, "Registration failed; retrying in {Seconds}s", retry);
        delay = retry;
        retry = Math.Min(retry * 2, 30);
      }
      await Task.Delay(TimeSpan.FromSeconds(delay), stoppingToken);
    }
  }
}
