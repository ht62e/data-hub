using DataHub.Contracts;
using DataHub.Platform;

public sealed class RegistrationConflictException() :
    Exception("Source ID is already leased to a different provider");

// The registry owns only this hub's providers; remote discoveries are never written here.
public sealed class HubState(
    IConfiguration config,
    FederationOptions federation,
    PlatformOptions platform,
    TimeProvider? clock = null) {
  private sealed record Registration(DiscoveredSource Source, DateTimeOffset Expires);
  private readonly Dictionary<string, Registration> _sources = new();
  private readonly object _sync = new();
  private readonly TimeProvider _time = clock ?? TimeProvider.System;
  public int LeaseSeconds { get; } = Math.Clamp(config.GetValue("LeaseSeconds", 45), 10, 3600);
  public void Register(SourceDescriptor source) {
    var invalidSource = !DataProtocol.IsIdentifier(source.Id) ||
        string.IsNullOrWhiteSpace(source.DisplayName) || source.DisplayName.Length > 200;
    if (invalidSource) {
      throw new ArgumentException(
          "Source ID must contain 1-100 ASCII letters, digits, dots, underscores or hyphens; " +
          "display name is required (max 200)");
    }
    if (source.Target is not { IsValid: true } || source.ApiVersion != DataProtocol.ApiVersion) {
      throw new ArgumentException(
          "A valid provider identity and supported ApiVersion (1) are required");
    }
    var entry = new DiscoveredSource(
        federation.HubId,
        new(platform.Id, platform.ServiceId),
        source,
        [federation.HubId]);
    lock (_sync) {
      var now = _time.GetUtcNow();
      if (_sources.TryGetValue(source.Id, out var previous) && previous.Expires > now &&
          previous.Source.Descriptor.Target != source.Target) {
        throw new RegistrationConflictException();
      }
      _sources[source.Id] = new(entry, now.AddSeconds(LeaseSeconds));
    }
  }
  public DiscoveredSource GetSource(string id) {
    lock (_sync) {
      return _sources.TryGetValue(id, out var source) && source.Expires > _time.GetUtcNow()
          ? source.Source
          : throw new KeyNotFoundException("Source is unavailable or its lease expired");
    }
  }
  public IReadOnlyList<DiscoveredSource> List() {
    lock (_sync) {
      var now = _time.GetUtcNow();
      foreach (var id in _sources.Where(pair => pair.Value.Expires <= now)
          .Select(pair => pair.Key).ToArray()) {
        _sources.Remove(id);
      }
      return _sources.Values.Select(s => s.Source)
          .OrderBy(s => s.QualifiedId, StringComparer.Ordinal).ToArray();
    }
  }
}
