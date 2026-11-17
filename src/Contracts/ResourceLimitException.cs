namespace DataHub.Contracts;

public enum ResourceLimitKind {
  Capacity,
  Payload
}

public sealed class ResourceLimitException(
    ResourceLimitKind kind,
    string message) : Exception(message) {
  public ResourceLimitKind Kind { get; } = kind;
}
