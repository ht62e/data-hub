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
var externalFile = Path.Combine(Path.GetTempPath(), "datahub-federation-" + Guid.NewGuid().ToString("N") + ".json");
var hubs = new Dictionary<string, (int Port, string[] Peers)> {
  ["a"] = (6100, ["b", "d"]),
  ["b"] = (6200, ["c"]),
  ["c"] = (6300, []),
  ["d"] = (6400, ["c"]),
  ["e"] = (6500, ["f"]),
  ["f"] = (6600, ["e"])
};
string Key(string id) => $"client-{id}-only";
Process Start(string project, Dictionary<string, string> environment) {
  var info = new ProcessStartInfo("dotnet", $"bin/Debug/net10.0/{project}.dll") {
    WorkingDirectory = Path.Combine(root, "src", project),
    UseShellExecute = false,
    CreateNoWindow = true
  };
  foreach (var (name, value) in environment)
    info.Environment[name] = value;
  info.Environment["Logging__LogLevel__Default"] = "Error";
  var process = Process.Start(info) ?? throw new Exception("Cannot start " + project);
  processes.Add(process);
  return process;
}
void Mapping(Dictionary<string, string> env, int index, string platform, string service, int port, string key) {
  env[$"Platform__Services__{index}__PlatformId"] = platform;
  env[$"Platform__Services__{index}__ServiceId"] = service;
  env[$"Platform__Services__{index}__Endpoint"] = $"http://localhost:{port}";
  env[$"Platform__Services__{index}__ApiKey"] = key;
}
Process StartHub(string id, string[]? configuredPeers = null) {
  var (port, defaultPeers) = hubs[id];
  var peers = configuredPeers ?? defaultPeers;
  var env = new Dictionary<string, string> {
    ["ApiKey"] = Key(id),
    ["Federation__HubId"] = id,
    ["LeaseSeconds"] = "10",
    ["Platform__Id"] = id,
    ["Kestrel__Endpoints__Rest__Url"] = $"http://localhost:{port}",
    ["Kestrel__Endpoints__Grpc__Url"] = $"http://localhost:{port + 2}",
    ["Federation__DiscoveryTimeoutSeconds"] = "2",
    ["Federation__NegotiationIntervalSeconds"] = "2"
  };
  Mapping(env, 0, "a", "provider-shared", 6113, Key("a"));
  Mapping(env, 1, "c", "provider-shared", 6313, Key("c"));
  var index = 2;
  foreach (var hub in hubs)
    Mapping(env, index++, hub.Key, "data-hub", hub.Value.Port + 2, Key(hub.Key));
  // Deliberately absent at all other hubs: only c can resolve/connect to its external node.
  if (id == "c") {
    env["ExternalNodes__0__NodeId"] = "outside-c";
    env["ExternalNodes__0__Endpoint"] = "http://localhost:6333";
    env["ExternalNodes__0__ApiKey"] = "external-c-only";
  }
  for (int i = 0; i < peers.Length; i++) {
    env[$"Federation__Peers__{i}__HubId"] = peers[i];
    env[$"Federation__Peers__{i}__ApiKey"] = $"federation-{id}-{peers[i]}-secret";
  }
  var parents = hubs.Where(h => h.Value.Peers.Contains(id)).Select(h => h.Key).ToList();
  if (id == "a") {
    parents.Add("test-runner");
    parents.Add("b");
  }
  for (int i = 0; i < parents.Count; i++) {
    env[$"Federation__TrustedHubs__{i}__HubId"] = parents[i];
    env[$"Federation__TrustedHubs__{i}__ApiKey"] = $"federation-{parents[i]}-{id}-secret";
  }
  return Start("Hub", env);
}
Process StartProvider(string owner, int port, bool external = false) {
  var env = new Dictionary<string, string> {
    ["ApiKey"] = Key(owner),
    ["SourceId"] = "shared-log",
    ["DisplayName"] = owner + " owned source",
    ["SourceType"] = "File",
    ["Platform__Id"] = owner,
    ["Platform__ServiceId"] = "provider-shared",
    ["Kestrel__Endpoints__Grpc__Url"] = $"http://localhost:{port}",
    ["MinIntervalMs"] = "100"
  };
  Mapping(env, 0, owner, "data-hub", hubs[owner].Port + 2, Key(owner));
  if (owner == "c")
    env["FilePath"] = Path.Combine(root, "tests", "FederationSmoke", "remote-logs.json");
  if (external) {
    env["ApiKey"] = "external-c-only";
    env["ExternalNodeId"] = "outside-c";
    env["SourceId"] = "external-log";
    env["HubEndpoint"] = $"http://localhost:{hubs[owner].Port + 2}";
    env["HubApiKey"] = Key(owner);
    env["FilePath"] = externalFile;
  }
  return Start("Provider", env);
}
static void Check(bool condition, string message) { if (!condition) throw new Exception(message); Console.WriteLine("PASS " + message); }
static async Task Expect(StatusCode code, Func<Task> action, string message) {
  try {
    await action();
  } catch (RpcException e) when (e.StatusCode == code) { Console.WriteLine("PASS " + message); return; }
  throw new Exception("Expected " + code + ": " + message);
}
try {
  File.Copy(Path.Combine(root, "tests", "FederationSmoke", "external-logs.json"), externalFile);
  StartHub("a");
  var b = StartHub("b");
  StartHub("c");
  StartHub("d");
  StartProvider("a", 6113);
  var remoteProvider = StartProvider("c", 6313);
  var externalProvider = StartProvider("c", 6333, true);
  using var channel = GrpcChannel.ForAddress("http://localhost:6102");
  using var cChannel = GrpcChannel.ForAddress("http://localhost:6302");
  var api = new HubApi.HubApiClient(channel);
  var cApi = new HubApi.HubApiClient(cChannel);
  var federationApi = new FederationApi.FederationApiClient(channel);
  var headers = new Metadata { { "x-api-key", Key("a") } };
  var cHeaders = new Metadata { { "x-api-key", Key("c") } };
  var federationHeaders = new Metadata { { "x-federation-peer", "test-runner" }, { "x-federation-key", "federation-test-runner-a-secret" } };
  SourceList? catalog = null;
  for (int i = 0; i < 50; i++) {
    try {
      catalog = await api.ListSourcesAsync(new Empty(), headers, deadline: DateTime.UtcNow.AddSeconds(3));
      if (catalog.Sources.Count == 3 && catalog.UnavailableHubs.Count == 0)
        break;
    } catch (RpcException) { }
    await Task.Delay(300);
  }
  Check(catalog is { Truncated: false } && catalog.UnavailableHubs.Count == 0 && catalog.Sources.Count == 3,
      "startup negotiation accepts diamond DAG and deduplicates both provider placements");
  var cList = await cApi.ListSourcesAsync(new Empty(), cHeaders);
  Check(cList.Sources.Count == 2 && cList.Sources.All(s => s.Id.StartsWith("c/")), "inbound trust does not create a reverse discovery edge");
  Check(catalog!.Sources.Any(s => s.Id == "a/shared-log") && catalog.Sources.Any(s => s.Id == "c/shared-log"), "qualified IDs distinguish same-named local sources");
  await Expect(StatusCode.InvalidArgument, async () => await api.ReadAsync(
      new ReadRequest { SourceId = "shared-log", Limit = 1 }, headers), "source aliases are rejected");

  using var transport = new StaticPlatformGrpcClientFactory(new PlatformOptions {
    Services = [
      new StaticService { PlatformId = "a", ServiceId = "data-hub", Endpoint = "http://localhost:6102", ApiKey = Key("a") }
  ]
  });
  var recording = new RecordingPlatformFactory(transport);
  var sdk = new DataHubClient(recording, new("a", "data-hub"));
  await sdk.SelectSourceAsync("c/shared-log");
  recording.Calls.Clear();
  Check((await sdk.ReadAsync(1)).Records.Single().EquipmentId == "remote-press" &&
      recording.Calls.SequenceEqual([new ServiceAddress("a", "data-hub")]),
      "(1) SDK reads a remote MSA provider through the entry hub only");
  await Expect(StatusCode.InvalidArgument, async () => await sdk.ReadAsync(-1), "SDK cannot skip hub input validation");
  await sdk.SelectSourceAsync("c/external-log");
  recording.Calls.Clear();
  Check((await sdk.ReadAsync(1)).Records.Single().EquipmentId == "outside-dag-press" &&
      recording.Calls.SequenceEqual([new ServiceAddress("a", "data-hub")]),
      "(2) SDK reads a remote external provider through the entry and owning hubs");
  await sdk.SelectSourceAsync("c/missing");
  await Expect(StatusCode.NotFound, async () => await sdk.ReadAsync(1),
      "a locally selected missing source is rejected during the read");
  await sdk.SelectSourceAsync("c/external-log");

  using var http = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false }) { BaseAddress = new("http://localhost:6100"), Timeout = TimeSpan.FromSeconds(8) };
  http.DefaultRequestHeaders.Add("x-api-key", Key("a"));
  Check((await http.GetStringAsync("/api/records?sourceId=c/shared-log&limit=1")).Contains("remote-press"),
      "(3) external REST client reads federated MSA provider through entrance hub");
  Check((await http.GetStringAsync("/api/records?sourceId=c/external-log&limit=1")).Contains("outside-dag-press"),
      "(4) entrance hub uses owning hub for external data without an external-node mapping or credential");
  await File.WriteAllTextAsync(externalFile, JsonSerializer.Serialize(new[] { new { equipmentId = "outside-dag-press", message = new string('x', 300000) } }));
  try {
    await sdk.ReadAsync(1);
    throw new Exception("Oversized federated payload was accepted");
  } catch (RpcException e) when (e.StatusCode == StatusCode.ResourceExhausted && e.Trailers.GetValue(ResourceLimitErrors.Trailer) == "payload") {
    Console.WriteLine("PASS owner relay preserves explicit payload-limit errors to SDK");
  }
  Check((await http.GetAsync("/api/records?sourceId=c/external-log&limit=1")).StatusCode == HttpStatusCode.UnprocessableEntity,
      "REST payload-limit classification survives both entrance and owner hubs");
  File.Copy(Path.Combine(root, "tests", "FederationSmoke", "external-logs.json"), externalFile, true);
  using var restCatalog = JsonDocument.Parse(await http.GetStringAsync("/api/sources"));
  Check(restCatalog.RootElement.GetProperty("sources").GetArrayLength() == 3 &&
      restCatalog.RootElement.GetProperty("unavailableHubs").GetArrayLength() == 0, "REST catalog and discovery status share one response");
  Check((await api.ReadAsync(new ReadRequest { SourceId = "a/shared-log", Limit = 1 }, headers))
      .Records.Single().EquipmentId == "press-01" &&
      (await http.GetStringAsync("/api/records?sourceId=c/external-log&limit=1")).Contains("outside-dag-press"),
      "REST and gRPC requests choose sources independently");

  using var wrongProviderChannel = GrpcChannel.ForAddress("http://localhost:6113");
  await Expect(StatusCode.FailedPrecondition, async () => await new ProviderApi.ProviderApiClient(
      wrongProviderChannel).ReadAsync(new ProviderReadRequest {
        ExpectedSourceId = "shared-log",
        ExpectedTarget = ProviderTargets.ToContract(
            new ProviderAddress.Platform(new("c", "provider-shared"))),
        ApiVersion = DataProtocol.ApiVersion, Limit = 1
      }, headers), "a wrong provider cannot return another provider's same-named source");
  await Expect(StatusCode.FailedPrecondition, async () =>
      await new HubRelayApi.HubRelayApiClient(channel).ReadSourceAsync(new RelayReadRequest {
        SourceId = "c/external-log", ApiVersion = DataProtocol.ApiVersion, Limit = 1
      }, headers), "a wrong owning hub rejects the source without another relay");

  var pendingRead = sdk.ReadAsync(1);
  await sdk.SelectSourceAsync("a/shared-log");
  Check((await pendingRead).Records.Single().EquipmentId == "outside-dag-press",
      "switching the local selection cannot change an in-flight read");
  var ownerReply = await new HubRelayApi.HubRelayApiClient(cChannel).ReadSourceAsync(new RelayReadRequest {
    SourceId = "c/external-log",
    ApiVersion = DataProtocol.ApiVersion,
    Limit = 1,
    CorrelationId = "relay-trace"
  }, cHeaders);
  Check(ownerReply.Records.Single().EquipmentId == "outside-dag-press" && ownerReply.CorrelationId == "relay-trace",
      "owner relay resolves its local source and preserves correlation");
  var tracedReply = await api.ReadAsync(new ReadRequest {
    SourceId = "c/external-log", Limit = 1, CorrelationId = "entry-trace"
  }, headers);
  Check(tracedReply.CorrelationId == "entry-trace",
      "entry and owner hubs preserve the caller's correlation ID");
  using var missing = new StaticPlatformGrpcClientFactory(new PlatformOptions());
  await Expect(StatusCode.Unavailable, () => Task.Run(() => missing.Create(new("c", "provider-shared"))), "missing platform mapping has no URL fallback");

  Check((await http.GetAsync("/api/federation/ready")).IsSuccessStatusCode, "readiness requires successful topology negotiation");
  var topologyRequest = new DiscoverRequest { RemainingHops = 3 };
  topologyRequest.VisitedHubs.Add("test-runner");
  var initialTopology = await federationApi.NegotiateAsync(topologyRequest, federationHeaders);
  var entryInstance = initialTopology.Nodes.Single(n => n.HubId == "a").InstanceId;
  var oldBInstance = initialTopology.Nodes.Single(n => n.HubId == "b").InstanceId;
  var discovery = new DiscoverRequest { RemainingHops = 0, ExpectedInstanceId = entryInstance };
  discovery.VisitedHubs.Add("test-runner");
  var bounded = await federationApi.DiscoverAsync(discovery, federationHeaders);
  Check(bounded.Truncated && bounded.Sources.Count == 1, "hop budget limits exploration and signals truncation");
  await Expect(StatusCode.Unauthenticated, async () => await federationApi.DiscoverAsync(discovery, headers), "client key cannot call private federation RPCs");
  await Expect(StatusCode.Unauthenticated, async () => await api.ListSourcesAsync(new Empty(), federationHeaders), "federation key cannot call public APIs");
  await Expect(StatusCode.Unauthenticated, async () => await new HubRelayApi.HubRelayApiClient(channel).ReadSourceAsync(
      new RelayReadRequest { SourceId = "a/shared-log", ApiVersion = "1" }, federationHeaders), "federation key cannot authorize data relay");
  var excessive = new DiscoverRequest { RemainingHops = 4, ExpectedInstanceId = entryInstance };
  excessive.VisitedHubs.Add("test-runner");
  await Expect(StatusCode.InvalidArgument, async () => await federationApi.DiscoverAsync(excessive, federationHeaders), "excessive discovery path rejected");

  b.Kill(true);
  await b.WaitForExitAsync();
  for (int i = 0; i < 30; i++) {
    if ((await http.GetStringAsync("/api/federation")).Contains("Degraded"))
      break;
    await Task.Delay(300);
  }
  Check((await http.GetStringAsync("/api/federation")).Contains("Degraded") && (await http.GetAsync("/api/federation/ready")).IsSuccessStatusCode,
      "peer outage retains verified topology and marks federation degraded");
  catalog = await api.ListSourcesAsync(new Empty(), headers);
  Check(catalog.Sources.Count == 3 && catalog.UnavailableHubs.Contains("b"), "healthy diamond branch still discovers both remote provider placements");
  await sdk.SelectSourceAsync("c/external-log");
  Check((await sdk.ReadAsync(1)).Records.Single().EquipmentId == "outside-dag-press" &&
      (await http.GetStringAsync("/api/records?sourceId=c/external-log&limit=1")).Contains("outside-dag-press"),
      "SDK and REST remote reads continue through healthy branch during peer outage");
  await Expect(StatusCode.Unavailable, async () => await api.ReadAsync(new ReadRequest { SourceId = "b/missing", Limit = 1 }, headers),
      "source hidden by failed branch reports unavailable rather than not found");
  Check((await api.ReadAsync(new ReadRequest { SourceId = "a/shared-log", Limit = 1 }, headers)).Records.Count == 1, "local reads survive federation isolation");
  b = StartHub("b", ["a"]);
  using var bHttp = new HttpClient { BaseAddress = new("http://localhost:6200") };
  bHttp.DefaultRequestHeaders.Add("x-api-key", Key("b"));
  string changedState = "";
  for (int i = 0; i < 40; i++) {
    try {
      changedState = await bHttp.GetStringAsync("/api/federation");
      if (changedState.Contains("Directed cycle rejected"))
        break;
    } catch (HttpRequestException) { }
    await Task.Delay(300);
  }
  catalog = await api.ListSourcesAsync(new Empty(), headers);
  Check(changedState.Contains("Directed cycle rejected") && catalog.Sources.Count == 3 && catalog.UnavailableHubs.Contains("b"),
      "restarted peer with a cyclic configuration is excluded while the verified healthy branch remains available");
  b.Kill(true);
  await b.WaitForExitAsync();
  StartHub("b");
  for (int i = 0; i < 40; i++) {
    catalog = await api.ListSourcesAsync(new Empty(), headers);
    if (catalog.Sources.Count == 3 && catalog.UnavailableHubs.Count == 0)
      break;
    await Task.Delay(300);
  }
  Check(catalog.Sources.Count == 3 && catalog.UnavailableHubs.Count == 0, "successful renegotiation admits restarted peer and restores full discovery");
  using var bChannel = GrpcChannel.ForAddress("http://localhost:6202");
  var bDiscovery = new DiscoverRequest { RemainingHops = 1, ExpectedInstanceId = oldBInstance };
  bDiscovery.VisitedHubs.Add("a");
  await Expect(StatusCode.FailedPrecondition, async () => await new FederationApi.FederationApiClient(bChannel).DiscoverAsync(bDiscovery,
      new Metadata { { "x-federation-peer", "a" }, { "x-federation-key", "federation-a-b-secret" } }),
      "old instance pin cannot discover a restarted peer even after it becomes ready");
  StartHub("e");
  StartHub("f");
  using var eHttp = new HttpClient { BaseAddress = new("http://localhost:6500") };
  eHttp.DefaultRequestHeaders.Add("x-api-key", Key("e"));
  string cycleState = "";
  for (int i = 0; i < 40; i++) {
    try {
      cycleState = await eHttp.GetStringAsync("/api/federation");
      if (cycleState.Contains("Directed cycle rejected"))
        break;
    } catch (HttpRequestException) { }
    await Task.Delay(300);
  }
  Check(cycleState.Contains("Directed cycle rejected") && (await eHttp.GetAsync("/api/federation/ready")).StatusCode == HttpStatusCode.ServiceUnavailable,
      "startup negotiation rejects directed cycles");
  await sdk.SelectSourceAsync("c/external-log");
  externalProvider.Kill(true);
  await externalProvider.WaitForExitAsync();
  await Expect(StatusCode.Unavailable, async () => await sdk.ReadAsync(1), "external-node failure propagates through the owning hub to SDK");
  Check((await http.GetAsync("/api/records?sourceId=c/external-log&limit=1")).StatusCode == HttpStatusCode.ServiceUnavailable,
      "external-node failure propagates as REST 503 without redirect or fallback");
  remoteProvider.Kill(true);
  await remoteProvider.WaitForExitAsync();
  await Task.Delay(TimeSpan.FromSeconds(12));
  catalog = await api.ListSourcesAsync(new Empty(), headers);
  Check(catalog.Sources.All(s => !s.Id.StartsWith("c/")), "both remote provider leases expire without stale catalog cache");
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
sealed class RecordingPlatformFactory(IPlatformGrpcClientFactory inner) : IPlatformGrpcClientFactory {
  public List<ServiceAddress> Calls { get; } = [];
  public CallInvoker Create(ServiceAddress address) {
    Calls.Add(address);
    return inner.Create(address);
  }
}
