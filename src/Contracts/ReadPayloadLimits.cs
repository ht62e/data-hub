using Google.Protobuf;

namespace DataHub.Contracts;

// Limits apply to protobuf-encoded sizes. Overflow never returns a partial reply.
public sealed class ReadPayloadLimits {
  public int MaxRecordBytes { get; set; } = 256 * 1024;
  public int MaxResponseBytes { get; set; } = 2 * 1024 * 1024;
  public void Validate() {
    if (MaxRecordBytes < 128 || MaxRecordBytes > MaxResponseBytes || MaxResponseBytes < 256 ||
        MaxResponseBytes > DataProtocol.MaxGrpcMessageBytes) {
      throw new InvalidOperationException(
          "DataLimits requires MaxRecordBytes >= 128, MaxResponseBytes >= 256, " +
          "record <= response <= 4 MiB");
    }
  }

  public async Task<ReadReply> CreateReplyAsync(
      IAsyncEnumerable<LogRecord> records,
      string correlationId,
      CancellationToken ct) {
    ct.ThrowIfCancellationRequested();
    var reply = new ReadReply { CorrelationId = correlationId };
    var protobufBytes = reply.CalculateSize();
    await foreach (var record in records.WithCancellation(ct)) {
      ct.ThrowIfCancellationRequested();
      var recordBytes = record.CalculateSize();
      if (recordBytes > MaxRecordBytes) {
        throw TooLarge("Log record exceeds MaxRecordBytes");
      }
      // Repeated messages include a field tag and a length prefix.
      protobufBytes += CodedOutputStream.ComputeTagSize(ReadReply.RecordsFieldNumber) +
          CodedOutputStream.ComputeLengthSize(recordBytes) + recordBytes;
      if (protobufBytes > MaxResponseBytes) {
        throw TooLarge("Read response exceeds MaxResponseBytes; request fewer records");
      }
      reply.Records.Add(record);
    }
    return reply;
  }
  private static ResourceLimitException TooLarge(string message) =>
      new(ResourceLimitKind.Payload, message);
}
