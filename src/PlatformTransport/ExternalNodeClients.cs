using DataHub.Contracts;
using Grpc.Core;

namespace DataHub.Platform;

public sealed class ExternalNode {
  public string NodeId { get; set; } = "";
  public string Endpoint { get; set; } = "";
  public string ApiKey { get; set; } = "";
}

// Only the owning hub needs these mappings or credentials.
public sealed class ExternalNodeClients : IDisposable {
  private readonly Dictionary<string, ExternalNode> _nodes;
  private readonly GrpcConnections _connections = new();
  public ExternalNodeClients(IEnumerable<ExternalNode> configuredNodes) {
    _nodes = configuredNodes.ToDictionary(n => n.NodeId, StringComparer.Ordinal);
    foreach (var node in _nodes.Values) {
      if (!DataProtocol.IsIdentifier(node.NodeId))
        throw new ArgumentException("Invalid external node ID");
      GrpcConnections.ValidateEndpoint(node.Endpoint);
    }
  }
  public CallInvoker Create(string nodeId) {
    if (!_nodes.TryGetValue(nodeId, out var node))
      throw new RpcException(new Status(
          StatusCode.Unavailable,
          "No external node mapping at owning hub: " + nodeId));
    return _connections.Create(node.Endpoint, node.ApiKey);
  }
  public void Dispose() => _connections.Dispose();
}
