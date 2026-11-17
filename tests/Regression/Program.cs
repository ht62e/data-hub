using System.Net;
using System.Text;
using DataHub.Contracts;
using DataHub.Platform;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;

namespace DataHub.Regression;

internal static class Program {
  private static IConfiguration Config(params (string Key, string Value)[] values) => new ConfigurationBuilder()
      .AddInMemoryCollection(values.Select(v => new KeyValuePair<string, string?>(v.Key, v.Value))).Build();
  private static void Check(bool value, string description) {
    if (!value)
      throw new Exception(description);
    Console.WriteLine("PASS " + description);
  }
  public static async Task Main() {
    var config = Config(("MetricsUrl", "http://test.invalid/load"));
    foreach (var body in new[] { "{}", "{\"cpuPercent\":10}", "{\"diskPercent\":20}", "{\"cpuPercent\":null,\"diskPercent\":10}", "{\"cpuPercent\":101,\"diskPercent\":10}" }) {
      using var http = new HttpClient(new Handler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) {
        Content = new StringContent(body, Encoding.UTF8, "application/json")
      })));
      var sample = await new HttpLoadMonitor(http, config, NullLogger<HttpLoadMonitor>.Instance).SampleAsync(default);
      Check(sample is { CpuPercent: 100, DiskPercent: 100 }, "invalid or incomplete load metrics use the maximum interval: " + body);
    }
    using (var http = new HttpClient(new Handler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) {
      Content = new StringContent("{\"cpuPercent\":0,\"diskPercent\":25}", Encoding.UTF8, "application/json")
    })))) {
      var sample = await new HttpLoadMonitor(http, config, NullLogger<HttpLoadMonitor>.Instance).SampleAsync(default);
      Check(sample is { CpuPercent: 0, DiskPercent: 25 }, "valid zero utilization is accepted");
    }

    foreach (var failure in new Exception[] {
        new HttpRequestException("metrics offline"), new TaskCanceledException("metrics timeout"),
        new IOException("metrics response interrupted")
    }) {
      using var http = new HttpClient(new Handler(
          (_, _) => Task.FromException<HttpResponseMessage>(failure)));
      var sample = await new HttpLoadMonitor(
          http, config, NullLogger<HttpLoadMonitor>.Instance).SampleAsync(default);
      Check(sample is { CpuPercent: 100, DiskPercent: 100 },
          "metrics request failure uses the maximum interval: " + failure.GetType().Name);
    }
    using (var http = new HttpClient(new Handler(
        (_, _) => throw new Exception("Invalid metrics URL reached the network")))) {
      var sample = await new HttpLoadMonitor(
          http, Config(("MetricsUrl", "/no-base-address")), NullLogger<HttpLoadMonitor>.Instance)
          .SampleAsync(default);
      Check(sample is { CpuPercent: 100, DiskPercent: 100 },
          "an unusable metrics URL uses the maximum interval");
    }
    using (var cancel = new CancellationTokenSource()) {
      cancel.Cancel();
      using var http = new HttpClient(new Handler(
          (_, ct) => Task.FromCanceled<HttpResponseMessage>(ct)));
      try {
        await new HttpLoadMonitor(http, config, NullLogger<HttpLoadMonitor>.Instance)
            .SampleAsync(cancel.Token);
        throw new Exception("Metrics cancellation ignored");
      } catch (OperationCanceledException) {
        Console.WriteLine("PASS caller cancellation is not replaced with a load fallback");
      }
    }

    using var limiter = new ReadLimiter(Config(("MinIntervalMs", "100")), new NoLoad());
    var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var first = limiter.RunAsync(async ct => { entered.SetResult(); await release.Task.WaitAsync(ct); return 1; }, default);
    await entered.Task;
    using (var cancel = new CancellationTokenSource()) {
      var queued = limiter.RunAsync(ct => Task.FromResult(2), cancel.Token);
      cancel.Cancel();
      try {
        await queued;
        throw new Exception("Queued cancellation ignored");
      } catch (OperationCanceledException) { Console.WriteLine("PASS cancelled waiter does not enter the source"); }
    }
    release.SetResult();
    await first;
    Check(await limiter.RunAsync(ct => Task.FromResult(3), default).WaitAsync(TimeSpan.FromSeconds(2)) == 3, "cancelling a waiter does not leak the limiter gate");

    var local = new HubState(Config(), new FederationOptions { HubId = "hub" }, new PlatformOptions());
    var registration = new RegisterRequest {
      SourceId = "logical-only",
      DisplayName = "Logical only",
      ApiVersion = DataProtocol.ApiVersion,
      Target = new ProviderTarget { PlatformService = new ServiceTarget { PlatformId = "p", ServiceId = "provider" } }
    };
    local.Register(SourceMappings.FromRegistration(registration));
    registration.Target.PlatformService.PlatformId = "changed";
    var entry = local.GetSource("logical-only");
    Check(entry.QualifiedId == "hub/logical-only" && entry.Descriptor.Target is ProviderAddress.Platform { Service.PlatformId: "p" },
        "provider registration does not retain mutable wire messages");
    var options = new FederationOptions { HubId = "a", Peers = [new FederationPeer { HubId = "b", ApiKey = "peer-only" }] };
    options.Validate("client-only");
    Check(options.Peers[0].PlatformId == "b", "DAG peers use platform name resolution");
    await ArchitectureChecks.Run();
    await ResourceChecks.Run();
  }
  private sealed class Handler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> action) : HttpMessageHandler {
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => action(request, cancellationToken);
  }
  private sealed class NoLoad : ILoadMonitor {
    public Task<LoadSample?> SampleAsync(CancellationToken ct) => Task.FromResult<LoadSample?>(null);
  }
}
