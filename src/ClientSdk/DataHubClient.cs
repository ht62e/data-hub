using DataHub.Contracts;
using DataHub.Platform;

namespace DataHub.Client;

// Each SDK instance keeps its own selection. All reads go through the entry hub.
public sealed class DataHubClient(
    IPlatformGrpcClientFactory platform,
    ServiceAddress hubAddress) {
  private volatile string? _selectedSourceId;
  private HubApi.HubApiClient _hub => new(platform.Create(hubAddress));

  public async Task<SourceList> ListSourcesAsync(CancellationToken ct = default) =>
      await _hub.ListSourcesAsync(
          new Empty(), deadline: DateTime.UtcNow.AddSeconds(10), cancellationToken: ct);

  public Task SelectSourceAsync(string sourceId, CancellationToken ct = default) {
    ct.ThrowIfCancellationRequested();
    DataProtocol.ParseSourceId(sourceId);
    _selectedSourceId = sourceId;
    return Task.CompletedTask;
  }

  public async Task<ReadReply> ReadAsync(int limit = 0, CancellationToken ct = default) {
    var sourceId = _selectedSourceId ?? throw new InvalidOperationException(
        "Select a source before reading");
    return await _hub.ReadAsync(
        new ReadRequest { SourceId = sourceId, Limit = limit },
        deadline: DateTime.UtcNow.AddSeconds(30), cancellationToken: ct);
  }
}
