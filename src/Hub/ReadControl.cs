using DataHub.Contracts;

public sealed record ReadAccessRequest(
    string SourceId,
    string ApiVersion,
    int Limit,
    string CorrelationId,
    string ClientKind);
public interface IReadRequestPolicy {
  Task ValidateAsync(ReadAccessRequest request, CancellationToken ct);
}
// Skeleton only: shared-key authentication still happens in middleware. This does NOT authorize
// an end user.
public sealed class DevelopmentReadRequestPolicy : IReadRequestPolicy {
  public Task ValidateAsync(ReadAccessRequest request, CancellationToken ct) {
    ct.ThrowIfCancellationRequested();
    if (request.ApiVersion != DataProtocol.ApiVersion)
      throw new InvalidOperationException("Unsupported data API version");
    if (request.Limit < 0)
      throw new ArgumentException("limit must be non-negative");
    return Task.CompletedTask;
  }
}
public sealed record ReadAuditEvent(
    string Phase,
    string SourceId,
    string CorrelationId,
    int? RecordCount = null);
public interface IReadAuditSink {
  void Record(ReadAuditEvent entry);
}
// Replace with a durable audit destination. Correlation is not a credential or verified user
// identity.
public sealed class LoggingReadAuditSink(ILogger<LoggingReadAuditSink> logger) : IReadAuditSink {
  public void Record(ReadAuditEvent entry) => logger.LogInformation(
      "Read {Phase} source={SourceId} correlation={CorrelationId} records={RecordCount}",
      entry.Phase,
      entry.SourceId,
      entry.CorrelationId,
      entry.RecordCount);
}
