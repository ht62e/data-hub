using DataHub.Contracts;
using Grpc.Core;

namespace DataHub.Platform;

public static class ResourceLimitErrors {
  public const string Trailer = "datahub-limit";
  public static RpcException ToRpcException(this ResourceLimitException error) => new(
      new Status(StatusCode.ResourceExhausted, error.Message),
      new Metadata {
        { Trailer, error.Kind == ResourceLimitKind.Payload ? "payload" : "capacity" }
      });

  public static int HttpStatus(ResourceLimitKind kind) =>
      kind == ResourceLimitKind.Payload ? 422 : 429;
  public static int HttpStatus(RpcException error) => error.Trailers.GetValue(Trailer) switch {
    "payload" => 422,
    "capacity" => 429,
    _ => 502
  };
}
