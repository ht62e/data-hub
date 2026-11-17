using DataHub.Contracts;

namespace DataHub.Platform;

public abstract record ProviderAddress {
  public sealed record Platform(ServiceAddress Service) : ProviderAddress;
  public sealed record External(string NodeId) : ProviderAddress;

  public bool IsValid => this switch {
    Platform { Service: { } service } =>
        DataProtocol.IsIdentifier(service.PlatformId) &&
            DataProtocol.IsIdentifier(service.ServiceId),
    External external => DataProtocol.IsIdentifier(external.NodeId),
    _ => false
  };
}

public static class ProviderTargets {
  public static ProviderAddress FromContract(ProviderTarget target) {
    if (!DataProtocol.IsTarget(target))
      throw new ArgumentException("A platform service or external node identity is required");
    return target.LocationCase == ProviderTarget.LocationOneofCase.PlatformService
        ? new ProviderAddress.Platform(
            new(target.PlatformService.PlatformId, target.PlatformService.ServiceId))
        : new ProviderAddress.External(target.ExternalNodeId);
  }
  public static ProviderTarget ToContract(ProviderAddress address) => address switch {
    ProviderAddress.Platform p => new() { PlatformService = ToService(p.Service) },
    ProviderAddress.External e => new() { ExternalNodeId = e.NodeId },
    _ => throw new ArgumentException("Unknown provider location")
  };
  public static ServiceTarget ToService(ServiceAddress address) => new() {
    PlatformId = address.PlatformId,
    ServiceId = address.ServiceId
  };
}
