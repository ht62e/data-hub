using DataHub.Contracts;
using DataHub.Platform;

// Wire validation stays at the boundary; the registry and merger use immutable source metadata.
public static class SourceMappings {
  public static SourceDescriptor FromRegistration(RegisterRequest request) =>
      new(
          request.SourceId,
          request.DisplayName,
          ProviderTargets.FromContract(request.Target),
          request.ApiVersion);

  public static Source ToSource(DiscoveredSource entry) => new() {
    Id = entry.QualifiedId,
    DisplayName = entry.Descriptor.DisplayName
  };

  public static FederationCatalog ToFederationCatalog(SourceCatalog catalog) {
    var result = new FederationCatalog { HubId = catalog.HubId, Truncated = catalog.Truncated };
    result.UnavailableHubs.AddRange(catalog.UnavailableHubs);
    foreach (var entry in catalog.Sources) {
      var message = new FederationSource {
        Source = ToSource(entry),
        ApiVersion = entry.Descriptor.ApiVersion,
        Target = ProviderTargets.ToContract(entry.Descriptor.Target),
        Owner = ProviderTargets.ToService(entry.Owner)
      };
      message.HubPath.AddRange(entry.HubPath);
      result.Sources.Add(message);
    }
    return result;
  }

  public static SourceCatalog FromFederationCatalog(
      FederationCatalog reply,
      string peerId,
      IReadOnlyList<string> visited,
      int maxPathLength) {
    var invalidCatalog = reply.HubId != peerId ||
        reply.Sources.Count > FederationCatalogMerger.MaxSources ||
        reply.UnavailableHubs.Count > FederationCatalogMerger.MaxSources ||
        reply.UnavailableHubs.Any(id => !FederationOptions.IsId(id));
    if (invalidCatalog) {
      throw new InvalidDataException("Unexpected peer identity or oversized/invalid catalog");
    }
    var sources = new List<DiscoveredSource>();
    foreach (var entry in reply.Sources) {
      var source = entry.Source ?? throw new InvalidDataException(
          "Invalid federation source identity or API version");
      var invalidSource = !DataProtocol.IsSourceId(source.Id) ||
          string.IsNullOrWhiteSpace(source.DisplayName) || source.DisplayName.Length > 200 ||
          !DataProtocol.IsTarget(entry.Target) || !DataProtocol.IsService(entry.Owner) ||
          entry.ApiVersion != DataProtocol.ApiVersion;
      if (invalidSource) {
        throw new InvalidDataException("Invalid federation source identity or API version");
      }
      var (ownerId, localId) = DataProtocol.ParseSourceId(source.Id);
      var invalidPath = entry.HubPath.Count == 0 || entry.HubPath.Count > maxPathLength ||
          entry.HubPath[0] != peerId || entry.HubPath[^1] != ownerId ||
          entry.HubPath.Any(id => !FederationOptions.IsId(id) ||
              visited.Contains(id, StringComparer.Ordinal)) ||
          entry.HubPath.Distinct(StringComparer.Ordinal).Count() != entry.HubPath.Count;
      if (invalidPath) {
        throw new InvalidDataException("Invalid federation source path");
      }
      var descriptor = new SourceDescriptor(
          localId,
          source.DisplayName,
          ProviderTargets.FromContract(entry.Target),
          entry.ApiVersion);
      sources.Add(new(
          ownerId,
          new(entry.Owner.PlatformId, entry.Owner.ServiceId),
          descriptor,
          [.. entry.HubPath]));
    }
    return new(reply.HubId, [.. sources], [.. reply.UnavailableHubs], reply.Truncated);
  }
}
