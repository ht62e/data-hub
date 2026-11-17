using DataHub.Contracts;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;

namespace DataHub.Platform;

// Physical connections are made only from trusted configuration, never catalog-supplied URLs.
public sealed class GrpcConnections : IDisposable {
  private sealed class DestinationCredential(string apiKey) : Interceptor {
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation) {
      var headers = new Metadata();
      foreach (var entry in context.Options.Headers ?? new Metadata()) {
        if (entry.Key != "x-api-key") {
          headers.Add(entry);
        }
      }
      headers.Add("x-api-key", apiKey);
      return continuation(
          request,
          new(context.Method, context.Host, context.Options.WithHeaders(headers)));
    }
  }
  private readonly Dictionary<string, GrpcChannel> _channels =
      new(StringComparer.Ordinal);
  private readonly object _sync = new();
  public CallInvoker Create(string endpoint, string apiKey) {
    GrpcChannel channel;
    lock (_sync) {
      if (!_channels.TryGetValue(endpoint, out channel!)) {
        ValidateEndpoint(endpoint);
        channel = GrpcChannel.ForAddress(endpoint, new GrpcChannelOptions {
          MaxReceiveMessageSize = DataProtocol.MaxGrpcMessageBytes,
          HttpHandler = new SocketsHttpHandler { AllowAutoRedirect = false }
        });
        _channels.Add(endpoint, channel);
      }
    }
    var invoker = channel.CreateCallInvoker();
    return string.IsNullOrEmpty(apiKey)
        ? invoker
        : invoker.Intercept(new DestinationCredential(apiKey));
  }
  public static void ValidateEndpoint(string endpoint) {
    if (endpoint.Length > 2048 ||
        !Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) ||
        uri.Scheme is not ("http" or "https") || uri.UserInfo.Length != 0 ||
        uri.AbsolutePath != "/" || uri.Query.Length != 0 || uri.Fragment.Length != 0) {
      throw new ArgumentException("Configured gRPC endpoint must be an HTTP(S) origin");
    }
  }
  public void Dispose() {
    lock (_sync) {
      foreach (var channel in _channels.Values) {
        channel.Dispose();
      }
    }
  }
}
