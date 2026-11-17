using System.Text.RegularExpressions;

namespace DataHub.Contracts;

public static class DataProtocol {
  public const string ApiVersion = "1";
  public const int MaxGrpcMessageBytes = 4 * 1024 * 1024;
  public static bool IsIdentifier(string? value) =>
      value is not null && Regex.IsMatch(value, "\\A[A-Za-z0-9._-]{1,100}\\z");
  public static bool IsService(ServiceTarget? target) =>
      target is not null && IsIdentifier(target.PlatformId) && IsIdentifier(target.ServiceId);
  public static bool IsTarget(ProviderTarget? target) => target?.LocationCase switch {
    ProviderTarget.LocationOneofCase.PlatformService => IsService(target.PlatformService),
    ProviderTarget.LocationOneofCase.ExternalNodeId => IsIdentifier(target.ExternalNodeId),
    _ => false
  };
  public static bool IsSourceId(string? id) =>
      id is not null && id.Split('/') is [var hub, var source] && IsIdentifier(hub) &&
      IsIdentifier(source);
  public static (string HubId, string LocalId) ParseSourceId(string id) {
    if (!IsSourceId(id))
      throw new ArgumentException("Source ID must use owner-hub/local-source");
    var parts = id.Split('/');
    return (parts[0], parts[1]);
  }
}
