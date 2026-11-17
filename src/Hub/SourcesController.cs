using DataHub.Contracts;
using DataHub.Platform;
using Grpc.Core;
using Microsoft.AspNetCore.Mvc;
using GrpcStatus = Grpc.Core.StatusCode;

[ApiController]
[Route("api")]
public sealed class SourcesController(
    HubCoordinator coordinator,
    FederationNetwork federation,
    FederationOptions options) : ControllerBase {
  [HttpGet("federation")]
  public IActionResult Federation() => Ok(new { options.HubId, status = federation.Status });

  [HttpGet("federation/ready")]
  public IActionResult FederationReady() {
    var status = federation.Status;
    return StatusCode(status.Ready ? 200 : 503, status);
  }
  [HttpGet("sources")]
  public Task<IActionResult> Sources() =>
      Handle(() => coordinator.List(HttpContext.RequestAborted));

  [HttpGet("records")]
  public Task<IActionResult> Read([FromQuery] string sourceId, [FromQuery] int limit = 0) =>
      Handle(() => coordinator.Read(
          sourceId,
          limit,
          true,
          HttpContext.RequestAborted,
          correlationId: Request.Headers["X-Correlation-ID"].ToString()));
  private async Task<IActionResult> Handle<T>(Func<Task<T>> action) {
    try {
      return Ok(await action());
    } catch (Exception e) when (e is KeyNotFoundException or ArgumentException or
        InvalidOperationException or ResourceLimitException) {
      int status = e switch {
        KeyNotFoundException => 404,
        ArgumentException => 400,
        ResourceLimitException limit => ResourceLimitErrors.HttpStatus(limit.Kind),
        _ => 409
      };
      return StatusCode(status, new {
        error = e.Message
      });
    } catch (RpcException e) {
      return StatusCode(e.StatusCode switch {
        GrpcStatus.NotFound => 404,
        GrpcStatus.InvalidArgument => 400,
        GrpcStatus.FailedPrecondition => 409,
        GrpcStatus.DeadlineExceeded => 504,
        GrpcStatus.Unavailable => 503,
        GrpcStatus.ResourceExhausted => ResourceLimitErrors.HttpStatus(e),
        _ => 502
      }, new {
        error = "Upstream or federation request failed",
        detail = e.Status.Detail
      });
    }
  }
}
