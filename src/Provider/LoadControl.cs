using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.RateLimiting;
using DataHub.Contracts;

public record LoadSample(
    [property: JsonRequired] double CpuPercent,
    [property: JsonRequired] double DiskPercent);
public interface ILoadMonitor {
  Task<LoadSample?> SampleAsync(CancellationToken ct);
}
public sealed class HttpLoadMonitor(
    HttpClient client,
    IConfiguration config,
    ILogger<HttpLoadMonitor> logger) : ILoadMonitor {
  public async Task<LoadSample?> SampleAsync(CancellationToken ct) {
    var url = config["MetricsUrl"];
    if (string.IsNullOrWhiteSpace(url)) {
      return null;
    }

    ct.ThrowIfCancellationRequested();
    if (!Uri.TryCreate(client.BaseAddress, url, out var endpoint) ||
        endpoint.Scheme is not ("http" or "https")) {
      logger.LogWarning("Invalid MetricsUrl; using maximum interval");
      return new LoadSample(100, 100);
    }
    try {
      var sample = await client.GetFromJsonAsync<LoadSample>(endpoint, ct);
      if (sample is { CpuPercent: >= 0 and <= 100, DiskPercent: >= 0 and <= 100 }) {
        return sample;
      }
      logger.LogWarning("Invalid load metrics; using maximum interval");
    } catch (Exception e) when (!ct.IsCancellationRequested &&
        e is HttpRequestException or IOException or JsonException or OperationCanceledException) {
      logger.LogWarning(e, "Load metrics unavailable; using maximum interval");
    }
    return new LoadSample(100, 100);
  }
}
public sealed class ReadLimiter(IConfiguration config, ILoadMonitor monitor) : IDisposable {
  private readonly ConcurrencyLimiter _queue = new(new ConcurrencyLimiterOptions {
    PermitLimit = 1,
    QueueLimit = Math.Clamp(config.GetValue("MaxPendingReads", 32), 0, 1024),
    QueueProcessingOrder = QueueProcessingOrder.OldestFirst
  });
  private long? _lastStart;
  public async Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> read, CancellationToken ct) {
    using var lease = await _queue.AcquireAsync(1, ct);
    if (!lease.IsAcquired) {
      throw new ResourceLimitException(ResourceLimitKind.Capacity, "Provider read queue is full");
    }

    int min = Math.Clamp(config.GetValue("MinIntervalMs", 1000), 100, 60000);
    int max = Math.Clamp(config.GetValue("MaxIntervalMs", 10000), min, 300000);
    var load = await monitor.SampleAsync(ct);
    var utilization = load is null ? 0 : Math.Max(load.CpuPercent, load.DiskPercent);
    var interval = min + (max - min) * Math.Clamp((utilization - 50) / 40, 0, 1);
    if (_lastStart is long previous) {
      var remaining = TimeSpan.FromMilliseconds(interval) - Stopwatch.GetElapsedTime(previous);
      if (remaining > TimeSpan.Zero) {
        await Task.Delay(remaining, ct);
      }
    }
    _lastStart = Stopwatch.GetTimestamp();
    return await read(ct);
  }
  public void Dispose() => _queue.Dispose();
}
