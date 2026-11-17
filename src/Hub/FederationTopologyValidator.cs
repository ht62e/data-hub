using DataHub.Contracts;

// Pure graph validation: no RPC, registry writes, clocks or readiness updates.
public static class FederationTopologyValidator {
  public static FederationTopology MergeAndValidate(
      FederationNode self,
      IEnumerable<FederationNode> discovered) {
    var nodes = new Dictionary<string, FederationNode>(StringComparer.Ordinal);
    foreach (var node in discovered.Prepend(self)) {
      var invalidNode = !FederationOptions.IsId(node.HubId) ||
          !Guid.TryParse(node.InstanceId, out _) || node.DownstreamHubs.Count > 8 ||
          node.DownstreamHubs.Any(id => !FederationOptions.IsId(id)) ||
          node.DownstreamHubs.Distinct().Count() != node.DownstreamHubs.Count;
      if (invalidNode) {
        throw new InvalidOperationException("Invalid topology node");
      }
      var conflictingNode = nodes.TryGetValue(node.HubId, out var previous) &&
          (previous.InstanceId != node.InstanceId ||
          !previous.DownstreamHubs.Order(StringComparer.Ordinal).SequenceEqual(
              node.DownstreamHubs.Order(StringComparer.Ordinal)));
      if (conflictingNode) {
        throw new InvalidOperationException(
            "Conflicting hub identity or topology for " + node.HubId);
      }
      nodes[node.HubId] = node;
    }
    if (nodes.Count > 128)
      throw new InvalidOperationException("Topology exceeds 128 hubs");
    var active = new HashSet<string>(StringComparer.Ordinal);
    var complete = new HashSet<string>(StringComparer.Ordinal);
    void Verify(string id) {
      if (active.Contains(id))
        throw new InvalidOperationException(
            "Directed cycle rejected in negotiated topology at " + id);
      if (complete.Contains(id))
        return;
      if (!nodes.TryGetValue(id, out var node))
        throw new InvalidOperationException("Incomplete negotiated topology: " + id);
      active.Add(id);
      foreach (var child in node.DownstreamHubs)
        Verify(child);
      active.Remove(id);
      complete.Add(id);
    }
    Verify(self.HubId);
    if (complete.Count != nodes.Count)
      throw new InvalidOperationException("Negotiation contains unreachable topology nodes");
    var result = new FederationTopology { HubId = self.HubId };
    result.Nodes.AddRange(nodes.Values.OrderBy(n => n.HubId, StringComparer.Ordinal));
    return result;
  }
}
