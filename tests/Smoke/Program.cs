using System.Diagnostics;
using System.Net;
using System.Text.Json;
using DataHub.Contracts;
using Grpc.Core;
using Grpc.Net.Client;
using DataHub.Client;
using DataHub.Platform;

var root = Directory.GetCurrentDirectory();
var processes = new List<Process>();
var externalFile = Path.Combine(Path.GetTempPath(), "datahub-smoke-" + Guid.NewGuid().ToString("N") + ".json");
Process Start(string project, Dictionary<string, string>? environment = null) {
  var info = new ProcessStartInfo("dotnet", $"bin/Debug/net10.0/{project}.dll") {
    WorkingDirectory = Path.Combine(root, "src", project),
    UseShellExecute = false,
    CreateNoWindow = true
  };
  info.Environment["Logging__LogLevel__Default"] = "Error";
  foreach (var (key, value) in environment ?? [])
    info.Environment[key] = value;
  var process = Process.Start(info) ?? throw new Exception("Failed to start " + project);
  processes.Add(process);
  return process;
}
static void Check(bool condition, string message) { if (!condition) throw new Exception(message); Console.WriteLine("PASS " + message); }
static async Task Expect(StatusCode code, Func<Task> action, string message, string? limitKind = null) {
  try {
    await action();
  } catch (RpcException e) when (e.StatusCode == code && (limitKind is null || e.Trailers.GetValue(ResourceLimitErrors.Trailer) == limitKind)) {
    Console.WriteLine("PASS " + message);
    return;
  }
  throw new Exception("Expected " + code + ": " + message);
}
const string hubKey = "development-only-change-me";
try {
  File.Copy(Path.Combine(root, "tests", "Smoke", "external-logs.json"), externalFile);
  Start("Hub", new() {
    ["LeaseSeconds"] = "10",
    ["ExternalNodes__0__NodeId"] = "outside-node",
    ["ExternalNodes__0__Endpoint"] = "http://localhost:5005",
    ["ExternalNodes__0__ApiKey"] = "external-provider-only"
  });
  var provider = Start("Provider");
  var external = Start("Provider", new() {
    ["ApiKey"] = "external-provider-only",
    ["ExternalNodeId"] = "outside-node",
    // This provider must start even when the unused MSA directory is invalid.
    ["Platform__Services__0__Endpoint"] = "unused-by-external-node",
    ["HubEndpoint"] = "http://localhost:5002",
    ["HubApiKey"] = hubKey,
    ["SourceId"] = "outside-log",
    ["DisplayName"] = "External log",
    ["Kestrel__Endpoints__Grpc__Url"] = "http://localhost:5005",
    ["MinIntervalMs"] = "100",
    ["FilePath"] = externalFile
  });
  using var http = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false }) { BaseAddress = new("http://localhost:5000"), Timeout = TimeSpan.FromSeconds(8) };
  using var channel = GrpcChannel.ForAddress("http://localhost:5002");
  var api = new HubApi.HubApiClient(channel);
  var headers = new Metadata { { "x-api-key", hubKey } };
  SourceList? sources = null;
  for (int i = 0; i < 40; i++) {
    try {
      sources = await api.ListSourcesAsync(new Empty(), headers, deadline: DateTime.UtcNow.AddSeconds(1));
      if (sources.Sources.Count == 2)
        break;
    } catch (RpcException) { }
    await Task.Delay(300);
  }
  Check(sources?.Sources.Select(s => s.Id).Order().SequenceEqual(["hub-local/file-press-01", "hub-local/outside-log"]) == true,
      "single hub registers platform and external providers with qualified source IDs");
  var registry = new Registry.RegistryClient(channel);
  await Expect(StatusCode.AlreadyExists, async () => await registry.RegisterAsync(new RegisterRequest {
    SourceId = "file-press-01",
    DisplayName = "Conflicting provider",
    ApiVersion = DataProtocol.ApiVersion,
    Target = ProviderTargets.ToContract(new ProviderAddress.External("outside-node"))
  }, headers), "different provider cannot overwrite a live source registration");
  Check((await http.GetAsync("/api/sources")).StatusCode == HttpStatusCode.Unauthorized, "unauthenticated request rejected");
  http.DefaultRequestHeaders.Add("x-api-key", hubKey);
  using var catalog = JsonDocument.Parse(await http.GetStringAsync("/api/sources"));
  Check(catalog.RootElement.GetProperty("sources").GetArrayLength() == 2 &&
      catalog.RootElement.GetProperty("unavailableHubs").GetArrayLength() == 0 && !catalog.RootElement.GetProperty("truncated").GetBoolean(),
      "one REST sources endpoint returns both sources and discovery status");
  Check((await http.GetAsync("/api/catalog")).StatusCode == HttpStatusCode.NotFound, "duplicate catalog endpoint removed");
  Check((await http.GetAsync("/api/clients/rest/source")).StatusCode == HttpStatusCode.NotFound,
      "the server-side selection endpoint has been removed");
  Check((await http.GetAsync("/api/records")).StatusCode == HttpStatusCode.BadRequest &&
      (await http.GetAsync("/api/records?sourceId=invalid")).StatusCode == HttpStatusCode.BadRequest,
      "REST requires a qualified source ID for each read");

  using var transport = new StaticPlatformGrpcClientFactory(new PlatformOptions {
    Services = [
      new StaticService { PlatformId = "platform-local", ServiceId = "data-hub", Endpoint = "http://localhost:5002", ApiKey = hubKey }
  ]
  });
  var recording = new RecordingFactory(transport);
  var sdk = new DataHubClient(recording, new("platform-local", "data-hub"));
  var otherSdk = new DataHubClient(recording, new("platform-local", "data-hub"));
  try {
    await sdk.ReadAsync(1);
    throw new Exception("An SDK without a selected source accepted a read");
  } catch (InvalidOperationException) {
    Console.WriteLine("PASS an SDK requires a local selection before reading");
  }
  await sdk.SelectSourceAsync("hub-local/file-press-01");
  await otherSdk.SelectSourceAsync("hub-local/outside-log");
  Check(recording.Calls.Count == 0, "SDK source selection is local and makes no network call");
  recording.Calls.Clear();
  var firstRead = sdk.ReadAsync(1);
  await sdk.SelectSourceAsync("hub-local/outside-log");
  Check((await firstRead).Records.Single().EquipmentId == "press-01" &&
      recording.Calls.SequenceEqual([new ServiceAddress("platform-local", "data-hub")]),
      "(1) SDK reads the selected MSA source through the entry hub using a per-read snapshot");
  await sdk.SelectSourceAsync("hub-local/file-press-01");
  var watch = Stopwatch.StartNew();
  Check((await sdk.ReadAsync(1)).Records.Count == 1 && watch.ElapsedMilliseconds >= 700, "successive reads respect provider rate limiting");
  recording.Calls.Clear();
  Check((await otherSdk.ReadAsync(1)).Records.Single().EquipmentId == "outside-press" &&
      recording.Calls.SequenceEqual([new ServiceAddress("platform-local", "data-hub")]),
      "(2) SDK instances keep separate selections and use only the entry hub");
  await sdk.SelectSourceAsync("hub-local/outside-log");

  Check((await http.GetStringAsync("/api/records?sourceId=hub-local/file-press-01&limit=1")).Contains("press-01"),
      "(3) external REST client reads MSA provider through entrance hub");
  Check((await http.GetStringAsync("/api/records?sourceId=hub-local/outside-log&limit=1")).Contains("outside-press"),
      "(4) external REST client reads external provider through its owning entrance hub");
  Check((await api.ReadAsync(new ReadRequest { SourceId = "hub-local/file-press-01", Limit = 1 }, headers))
      .Records.Count == 1, "gRPC reads with an explicit source ID");
  var oldRequest = ReadRequest.Parser.ParseFrom(new byte[] { 10, 5, 97, 47, 108, 111, 103 });
  await Expect(StatusCode.InvalidArgument, async () => await api.ReadAsync(oldRequest, headers),
      "legacy client ID fields cannot be mistaken for a source ID");

  var escapedMessage = new string('\u0001', 130000);
  await File.WriteAllTextAsync(externalFile, JsonSerializer.Serialize(Enumerable.Range(0, 3)
      .Select(_ => new { equipmentId = "outside-press", message = escapedMessage })));
  var escapedReply = await sdk.ReadAsync(3);
  Check(escapedReply.Records.Count == 3 && escapedReply.Records[0].Message == escapedMessage &&
      escapedReply.CalculateSize() < 2 * 1024 * 1024,
      "SDK accepts escaped data within the protobuf limits");
  using (var response = await http.GetAsync("/api/records?sourceId=hub-local/outside-log&limit=3")) {
    var content = await response.Content.ReadAsByteArrayAsync();
    using var json = JsonDocument.Parse(content);
    Check(response.IsSuccessStatusCode && content.Length > 2 * 1024 * 1024 &&
        json.RootElement.GetProperty("records").GetArrayLength() == 3 &&
        json.RootElement.GetProperty("records")[0].GetProperty("message").GetString() ==
            escapedMessage,
        "REST can return JSON larger than the protobuf response budget");
  }

  await File.WriteAllTextAsync(externalFile, JsonSerializer.Serialize(new[] { new { equipmentId = "outside-press", message = new string('x', 300000) } }));
  await Expect(StatusCode.ResourceExhausted, async () => await sdk.ReadAsync(1), "oversized record returns an explicit SDK payload error", "payload");
  Check((await http.GetAsync("/api/records?sourceId=hub-local/outside-log&limit=1")).StatusCode == HttpStatusCode.UnprocessableEntity,
      "REST returns 422 for an oversized record");
  await File.WriteAllTextAsync(externalFile, JsonSerializer.Serialize(Enumerable.Range(0, 12).Select(i => new { equipmentId = "outside-press", message = new string('x', 200000) })));
  await Expect(StatusCode.ResourceExhausted, async () => await sdk.ReadAsync(12), "aggregate response budget is enforced before the transport size limit", "payload");
  Check((await http.GetAsync("/api/records?sourceId=hub-local/outside-log&limit=12")).StatusCode == HttpStatusCode.UnprocessableEntity,
      "REST rejects oversized aggregate responses without partial success");
  File.Copy(Path.Combine(root, "tests", "Smoke", "external-logs.json"), externalFile, true);
  Check((await sdk.ReadAsync(1)).Records.Single().EquipmentId == "outside-press", "provider recovers its limiter slots after payload rejection");
  foreach (var invalid in new[] { "[invalid-json", "[null]" }) {
    await File.WriteAllTextAsync(externalFile, invalid);
    await Expect(StatusCode.Unavailable, async () => await sdk.ReadAsync(1),
        "malformed JSON and null records both report a source failure");
    using var response = await http.GetAsync("/api/records?sourceId=hub-local/outside-log&limit=1");
    Check(response.StatusCode == HttpStatusCode.ServiceUnavailable,
        "REST preserves source failures as 503");
  }
  File.Copy(Path.Combine(root, "tests", "Smoke", "external-logs.json"), externalFile, true);
  Check((await sdk.ReadAsync(1)).Records.Single().EquipmentId == "outside-press",
      "provider recovers after invalid source data is repaired");
  provider.Kill(true);
  await provider.WaitForExitAsync();
  external.Kill(true);
  await external.WaitForExitAsync();
  await Task.Delay(TimeSpan.FromSeconds(12));
  Check((await api.ListSourcesAsync(new Empty(), headers)).Sources.Count == 0, "expired registrations disappear for both placements");
  await Expect(StatusCode.NotFound, async () => await sdk.ReadAsync(1),
      "an SDK selection is resolved again after the source lease expires");
} finally {
  foreach (var process in processes) {
    if (!process.HasExited) {
      process.Kill(true);
      await process.WaitForExitAsync();
    }
    process.Dispose();
  }
  if (File.Exists(externalFile))
    File.Delete(externalFile);
}
sealed class RecordingFactory(IPlatformGrpcClientFactory inner) : IPlatformGrpcClientFactory {
  public List<ServiceAddress> Calls { get; } = [];
  public CallInvoker Create(ServiceAddress address) {
    Calls.Add(address);
    return inner.Create(address);
  }
}
