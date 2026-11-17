using System.Runtime.CompilerServices;
using System.Text.Json;
using DataHub.Contracts;
using Npgsql;

// Each source translates its native representation into the shared contract.
public interface ILogSource {
  IAsyncEnumerable<LogRecord> ReadAsync(int limit, CancellationToken ct);
}
public sealed class FileLogSource(IConfiguration config, IWebHostEnvironment env) : ILogSource {
  private sealed record FileRecord(
      string? Timestamp,
      string? EquipmentId,
      string? Level,
      string? Message);

  public async IAsyncEnumerable<LogRecord> ReadAsync(
      int limit,
      [EnumeratorCancellation] CancellationToken ct) {
    var path = Path.GetFullPath(config["FilePath"] ?? "sample-logs.json", env.ContentRootPath);
    await using var stream = new FileStream(
        path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, true);
    if (stream.Length > 10 * 1024 * 1024) {
      throw new InvalidDataException("Sample JSON file exceeds 10 MiB");
    }

    var count = 0;
    await foreach (var r in JsonSerializer.DeserializeAsyncEnumerable<FileRecord>(
        stream, JsonSerializerOptions.Web, ct)) {
      if (r is null) {
        throw new InvalidDataException("Log record cannot be null");
      }

      yield return new LogRecord {
        Timestamp = r.Timestamp ?? "",
        EquipmentId = r.EquipmentId ?? "",
        Level = r.Level ?? "",
        Message = r.Message ?? ""
      };
      if (++count >= limit) {
        yield break;
      }
    }
  }
}
public sealed class PostgresLogSource(
    IConfiguration config,
    ReadPayloadLimits limits) : ILogSource {
  public async IAsyncEnumerable<LogRecord> ReadAsync(
      int limit,
      [EnumeratorCancellation] CancellationToken ct) {
    await using var connection = new NpgsqlConnection(config["ConnectionString"]);
    await connection.OpenAsync(ct);
    // Fetch one character beyond the byte budget: a larger value necessarily fails validation.
    // This bounds allocations for oversized text columns without returning silently truncated
    // data.
    const string query = "SELECT occurred_at, LEFT(equipment_id, @maxChars), " +
        "LEFT(level, @maxChars), LEFT(message, @maxChars) " +
        "FROM equipment_logs ORDER BY occurred_at DESC, id DESC LIMIT @limit";
    await using var command = new NpgsqlCommand(query, connection) {
      CommandTimeout = 15
    };
    command.Parameters.AddWithValue("limit", limit);
    command.Parameters.AddWithValue("maxChars", limits.MaxRecordBytes + 1);
    await using var reader = await command.ExecuteReaderAsync(ct);
    while (await reader.ReadAsync(ct)) {
      yield return new LogRecord {
        Timestamp = reader.GetFieldValue<DateTime>(0).ToUniversalTime().ToString("O"),
        EquipmentId = reader.GetString(1),
        Level = reader.GetString(2),
        Message = reader.GetString(3)
      };
    }
  }
}
