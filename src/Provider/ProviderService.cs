using System.Data.Common;
using System.Text.Json;
using DataHub.Contracts;
using DataHub.Platform;
using Grpc.Core;

public sealed class LogReader(
    ILogSource source,
    ReadLimiter limiter,
    IConfiguration config,
    ReadPayloadLimits limits) {
  public Task<ReadReply> ReadAsync(int limit, string correlationId, CancellationToken ct) {
    if (limit < 0) {
      throw new ArgumentException("limit must be non-negative");
    }

    var max = Math.Clamp(config.GetValue("MaxRows", 100), 1, 1000);
    var effectiveLimit = limit == 0 ? max : Math.Min(limit, max);
    return limiter.RunAsync(
        token => limits.CreateReplyAsync(
            source.ReadAsync(effectiveLimit, token), correlationId, token),
        ct);
  }
}
public sealed class ProviderService(
    LogReader reader,
    ProviderRegistration registration,
    ILogger<ProviderService> logger) : ProviderApi.ProviderApiBase {
  public override async Task<ReadReply> Read(
      ProviderReadRequest request,
      ServerCallContext context) {
    var correlation = ProviderReadValidation.Validate(
        request, registration.SourceId, registration.Target);
    try {
      var result = await reader.ReadAsync(request.Limit, correlation, context.CancellationToken);
      logger.LogInformation(
          "Read Completed correlation={CorrelationId} records={RecordCount}",
          correlation,
          result.Records.Count);
      return result;
    } catch (ResourceLimitException e) {
      throw e.ToRpcException();
    } catch (Exception e) when (e is IOException or InvalidDataException or
        UnauthorizedAccessException or JsonException or DbException or TimeoutException) {
      logger.LogError(e, "Source read failed correlation={CorrelationId}", correlation);
      throw new RpcException(new Status(StatusCode.Unavailable, "Source read failed"));
    }
  }
}
