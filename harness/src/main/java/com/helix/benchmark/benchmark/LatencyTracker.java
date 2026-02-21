package com.helix.benchmark.benchmark;

import java.util.Arrays;

public class LatencyTracker {
    private final long[] latencies;
    private int count;

    public LatencyTracker(int capacity) {
        this.latencies = new long[capacity];
        this.count = 0;
    }

    public void record(long nanos) {
        latencies[count++] = nanos;
    }

    public int count() {
        return count;
    }

    public BenchmarkResult computeResult(String queryName, String configId, long totalElapsedMs) {
        if (count == 0) {
            throw new IllegalStateException("No latencies recorded");
        }
        long[] sorted = Arrays.copyOf(latencies, count);
        Arrays.sort(sorted);
        return new BenchmarkResult(
                queryName,
                configId,
                percentile(sorted, 0.50),
                percentile(sorted, 0.95),
                percentile(sorted, 0.99),
                average(sorted),
                count * 1000.0 / totalElapsedMs,
                count
        );
    }

    private long percentile(long[] sorted, double p) {
        int index = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, index)];
    }

    private double average(long[] sorted) {
        long sum = 0;
        for (long v : sorted) {
            sum += v;
        }
        return (double) sum / sorted.length;
    }
}
