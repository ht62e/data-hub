using System.Collections.Immutable;
using DataHub.Platform;

public sealed record SourceDescriptor(
    string Id,
    string DisplayName,
    ProviderAddress Target,
    string ApiVersion);

public sealed record DiscoveredSource(
    string OriginHubId,
    ServiceAddress Owner,
    SourceDescriptor Descriptor,
    ImmutableArray<string> HubPath) {
  public string QualifiedId => OriginHubId + "/" + Descriptor.Id;
}

public sealed record SourceCatalog(
    string HubId,
    ImmutableArray<DiscoveredSource> Sources,
    ImmutableArray<string> UnavailableHubs,
    bool Truncated = false);
