using DataHub.Contracts;
using DataHub.Platform;
using Grpc.Core;

public static class ProviderReadValidation {
  // This is a consistency check, not a credential or authorization decision.
  public static string Validate(
      ProviderReadRequest request,
      string sourceId,
      ProviderAddress identity) {
    if (request.ApiVersion != DataProtocol.ApiVersion)
      throw new RpcException(new Status(
          StatusCode.FailedPrecondition,
          "Unsupported data API version"));
    var correlation = string.IsNullOrEmpty(request.CorrelationId)
        ? Guid.NewGuid().ToString("N")
        : request.CorrelationId;
    var invalidRequest = request.Limit < 0 || !DataProtocol.IsIdentifier(correlation) ||
        !DataProtocol.IsIdentifier(request.ExpectedSourceId) ||
        !DataProtocol.IsTarget(request.ExpectedTarget);
    if (invalidRequest) {
      throw new RpcException(new Status(
          StatusCode.InvalidArgument,
          "Valid limit, correlation ID and expected provider/source identity are required"));
    }
    if (request.ExpectedSourceId != sourceId ||
        ProviderTargets.FromContract(request.ExpectedTarget) != identity) {
      throw new RpcException(new Status(
          StatusCode.FailedPrecondition,
          "Resolved provider does not match the requested source; check connection mapping and " +
          "registration"));
    }
    return correlation;
  }
}
