package com.helix.benchmark.benchmark;

public record BenchmarkResult(
        String queryName,
        String configurationId,
        long p50Nanos,
        long p95Nanos,
        long p99Nanos,
        double averageNanos,
        double throughputOpsPerSec,
        int iterationCount
) {
    public double p50Millis() {
        return p50Nanos / 1_000_000.0;
    }

    public double p95Millis() {
        return p95Nanos / 1_000_000.0;
    }

    public double p99Millis() {
        return p99Nanos / 1_000_000.0;
    }

    public double averageMillis() {
        return averageNanos / 1_000_000.0;
    }
}
