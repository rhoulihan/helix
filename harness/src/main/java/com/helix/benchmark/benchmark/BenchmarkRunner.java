package com.helix.benchmark.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class BenchmarkRunner {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);

    private final int warmUpIterations;
    private final int measurementIterations;

    public BenchmarkRunner(int warmUpIterations, int measurementIterations) {
        this.warmUpIterations = warmUpIterations;
        this.measurementIterations = measurementIterations;
    }

    public BenchmarkResult run(String queryName, String configId, Supplier<Void> workload) {
        log.info("Starting benchmark: {} on {}", queryName, configId);

        // Probe first iteration to detect slow queries
        long probeStart = System.nanoTime();
        workload.get();
        long probeNanos = System.nanoTime() - probeStart;
        double probeMs = probeNanos / 1_000_000.0;

        // Reduce iterations for slow queries (> 500ms per iteration)
        int effectiveWarmUp = warmUpIterations;
        int effectiveMeasure = measurementIterations;
        if (probeMs > 500) {
            effectiveWarmUp = Math.min(5, warmUpIterations);
            effectiveMeasure = Math.min(20, measurementIterations);
            log.info("Slow query detected ({} ms), reducing to {} warmup + {} measurement iterations",
                    String.format("%.0f", probeMs), effectiveWarmUp, effectiveMeasure);
        }

        // Warm-up phase (minus 1 since probe counts as warm-up)
        for (int i = 1; i < effectiveWarmUp; i++) {
            workload.get();
        }
        log.debug("Completed {} warm-up iterations for {} on {}", effectiveWarmUp, queryName, configId);

        // Measurement phase
        LatencyTracker tracker = new LatencyTracker(effectiveMeasure);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < effectiveMeasure; i++) {
            long iterStart = System.nanoTime();
            workload.get();
            long iterEnd = System.nanoTime();
            tracker.record(iterEnd - iterStart);
        }

        long totalElapsedMs = System.currentTimeMillis() - startTime;
        if (totalElapsedMs == 0) totalElapsedMs = 1; // avoid division by zero

        BenchmarkResult result = tracker.computeResult(queryName, configId, totalElapsedMs);
        log.info("Completed benchmark: {} on {} - p50={}ms, p95={}ms, p99={}ms, throughput={} ops/sec",
                queryName, configId,
                String.format("%.2f", result.p50Millis()),
                String.format("%.2f", result.p95Millis()),
                String.format("%.2f", result.p99Millis()),
                String.format("%.1f", result.throughputOpsPerSec()));

        return result;
    }
}
