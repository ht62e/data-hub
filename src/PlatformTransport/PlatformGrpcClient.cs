using DataHub.Contracts;
using Grpc.Core;

namespace DataHub.Platform;

public sealed record ServiceAddress(string PlatformId, string ServiceId);
public sealed class StaticService {
  public string PlatformId { get; set; } = "";
  public string ServiceId { get; set; } = "";
  public string Endpoint { get; set; } = "";
  public string ApiKey { get; set; } = "";
}
public sealed class PlatformOptions {
  public string Id { get; set; } = "platform-local";
  public string ServiceId { get; set; } = "data-hub";
  public List<StaticService> Services { get; set; } = [];
}
// Replace with the MSA platform's common client. Gateways and physical addresses stay behind this
// boundary.
public interface IPlatformGrpcClientFactory {
  CallInvoker Create(ServiceAddress address);
}
public sealed class StaticPlatformGrpcClientFactory : IPlatformGrpcClientFactory, IDisposable {
  private readonly Dictionary<ServiceAddress, StaticService> _directory;
  private readonly GrpcConnections _connections = new();
  public StaticPlatformGrpcClientFactory(PlatformOptions options) {
    _directory = options.Services.ToDictionary(s => new ServiceAddress(s.PlatformId, s.ServiceId));
    foreach (var service in _directory.Values) {
      if (!DataProtocol.IsIdentifier(service.PlatformId) ||
          !DataProtocol.IsIdentifier(service.ServiceId))
        throw new ArgumentException("Invalid platform service identity");
      GrpcConnections.ValidateEndpoint(service.Endpoint);
    }
  }
  public CallInvoker Create(ServiceAddress address) {
    if (!_directory.TryGetValue(address, out var service))
      throw new RpcException(new Status(
          StatusCode.Unavailable,
          $"No platform service mapping: {address.PlatformId}/{address.ServiceId}"));
    return _connections.Create(service.Endpoint, service.ApiKey);
  }
  public void Dispose() => _connections.Dispose();
}
