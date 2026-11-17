using DataHub.Contracts;
using DataHub.Platform;
using Grpc.Core;
using Grpc.Core.Interceptors;

// Translate expected application errors once, at the gRPC boundary. Upstream RpcExceptions
// retain their status/trailers; unexpected failures use gRPC's normal logging and error response.
public sealed class RpcErrorInterceptor : Interceptor {
  public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
      TRequest request,
      ServerCallContext context,
      UnaryServerMethod<TRequest, TResponse> continuation) {
    try {
      return await continuation(request, context);
    } catch (ResourceLimitException e) {
      throw e.ToRpcException();
    } catch (Exception e) when (e is RegistrationConflictException or KeyNotFoundException or
        ArgumentException or InvalidOperationException or OperationCanceledException) {
      var code = e switch {
        RegistrationConflictException => StatusCode.AlreadyExists,
        KeyNotFoundException => StatusCode.NotFound,
        ArgumentException => StatusCode.InvalidArgument,
        InvalidOperationException => StatusCode.FailedPrecondition,
        _ => StatusCode.Cancelled
      };
      throw new RpcException(new Status(code, e.Message));
    }
  }
}
