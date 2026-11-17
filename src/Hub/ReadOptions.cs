public sealed class ReadOptions {
  public int TimeoutSeconds { get; set; } = 30;
  public void Validate() {
    if (TimeoutSeconds is < 1 or > 120)
      throw new InvalidOperationException("Read.TimeoutSeconds must be 1-120");
  }
  public DateTime Deadline(DateTime? incoming = null) {
    var configured = DateTime.UtcNow.AddSeconds(TimeoutSeconds);
    return incoming is { } supplied && supplied < configured ? supplied : configured;
  }
}
