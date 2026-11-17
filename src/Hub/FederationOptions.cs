using DataHub.Contracts;

public sealed class FederationPeer {
  public string HubId { get; set; } = "";
  public string ApiKey { get; set; } = "";
  public string PlatformId { get; set; } = "";
  public string ServiceId { get; set; } = "data-hub";
}
public sealed class TrustedHub {
  public string HubId { get; set; } = "";
  public string ApiKey { get; set; } = "";
}
public sealed class FederationOptions {
  public string HubId { get; set; } = "hub-local";
  public int MaxHops { get; set; } = 3;
  public int DiscoveryTimeoutSeconds { get; set; } = 3;
  public int NegotiationIntervalSeconds { get; set; } = 10;
  public List<FederationPeer> Peers { get; set; } = [];
  // Accepting an RPC does not create a graph edge. Only Peers creates outgoing edges.
  public List<TrustedHub> TrustedHubs { get; set; } = [];
  public static bool IsId(string? value) => DataProtocol.IsIdentifier(value);
  public void Validate(string clientKey) {
    var invalidSettings = !IsId(HubId) || MaxHops is < 0 or > 4 ||
        DiscoveryTimeoutSeconds is < 1 or > 10 || Peers.Count > 8 ||
        NegotiationIntervalSeconds is < 2 or > 60;
    if (invalidSettings) {
      throw new InvalidOperationException(
          "Invalid Federation settings: valid HubId, MaxHops 0-4, " +
          "DiscoveryTimeoutSeconds 1-10, at most 8 peers required");
    }
    if (Peers.Select(p => p.HubId).Distinct(StringComparer.Ordinal).Count() != Peers.Count) {
      throw new InvalidOperationException("Federation peer HubIds must be unique");
    }
    foreach (var peer in Peers) {
      if (string.IsNullOrEmpty(peer.PlatformId))
        peer.PlatformId = peer.HubId;
      var invalidPeer = !IsId(peer.HubId) || peer.HubId == HubId || !IsId(peer.PlatformId) ||
          !IsId(peer.ServiceId) || string.IsNullOrWhiteSpace(peer.ApiKey) ||
          peer.ApiKey == clientKey;
      if (invalidPeer) {
        throw new InvalidOperationException(
            "Federation peers require distinct valid identities and a dedicated outbound key " +
            "different from the client key");
      }
    }
    var invalidTrustedHub = TrustedHubs.Count > 32 ||
        TrustedHubs.Any(p => !IsId(p.HubId) || p.HubId == HubId ||
            string.IsNullOrWhiteSpace(p.ApiKey) || p.ApiKey == clientKey) ||
        TrustedHubs.Select(p => p.HubId).Distinct(StringComparer.Ordinal).Count() !=
            TrustedHubs.Count ||
        TrustedHubs.Select(p => p.ApiKey).Distinct(StringComparer.Ordinal).Count() !=
            TrustedHubs.Count;
    if (invalidTrustedHub) {
      throw new InvalidOperationException(
          "TrustedHubs requires unique identities and dedicated per-caller keys (at most 32)");
    }
  }
}
