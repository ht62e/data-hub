public static class FederationCatalogMerger {
  public const int MaxSources = 1000;

  // Choose a deterministic shortest simple path without modifying either input catalog.
  public static SourceCatalog Merge(SourceCatalog local, IEnumerable<SourceCatalog> downstream) {
    var candidates = local.Sources.ToList();
    var unavailable = local.UnavailableHubs.ToList();
    var truncated = local.Truncated;
    foreach (var catalog in downstream) {
      truncated |= catalog.Truncated;
      unavailable.AddRange(catalog.UnavailableHubs);
      candidates.AddRange(catalog.Sources.Select(
          entry => entry with { HubPath = entry.HubPath.Insert(0, local.HubId) }));
    }
    var merged = candidates.GroupBy(e => e.QualifiedId, StringComparer.Ordinal)
        .Select(g => g.OrderBy(e => e.HubPath.Length)
            .ThenBy(e => string.Join("/", e.HubPath), StringComparer.Ordinal).First())
        .OrderBy(e => e.QualifiedId, StringComparer.Ordinal).ToArray();
    return new(
        local.HubId,
        [.. merged.Take(MaxSources)],
        [.. unavailable.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)],
        truncated || merged.Length > MaxSources);
  }
}
