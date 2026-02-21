package com.helix.benchmark.benchmark;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class LatencyTrackerTest {

    @Test
    void shouldCreateWithCapacity() {
        LatencyTracker tracker = new LatencyTracker(100);
        assertThat(tracker.count()).isZero();
    }

    @Test
    void shouldRecordLatency() {
        LatencyTracker tracker = new LatencyTracker(100);
        tracker.record(1_000_000L); // 1ms in nanos
        assertThat(tracker.count()).isEqualTo(1);
    }

    @Test
    void shouldComputeP50ForSingleValue() {
        LatencyTracker tracker = new LatencyTracker(10);
        tracker.record(5_000_000L);
        BenchmarkResult result = tracker.computeResult("Q1", "CONFIG1", 5);
        assertThat(result.p50Nanos()).isEqualTo(5_000_000L);
    }

    @Test
    void shouldComputePercentilesForMultipleValues() {
        LatencyTracker tracker = new LatencyTracker(100);
        for (int i = 1; i <= 100; i++) {
            tracker.record(i * 1_000_000L); // 1ms, 2ms, ..., 100ms
        }
        BenchmarkResult result = tracker.computeResult("Q1", "CONFIG1", 100);
        assertThat(result.p50Nanos()).isEqualTo(50_000_000L);
        assertThat(result.p95Nanos()).isEqualTo(95_000_000L);
        assertThat(result.p99Nanos()).isEqualTo(99_000_000L);
    }

    @Test
    void shouldComputeAverage() {
        LatencyTracker tracker = new LatencyTracker(10);
        tracker.record(2_000_000L);
        tracker.record(4_000_000L);
        tracker.record(6_000_000L);
        BenchmarkResult result = tracker.computeResult("Q1", "CONFIG1", 3);
        assertThat(result.averageNanos()).isCloseTo(4_000_000.0, within(0.01));
    }

    @Test
    void shouldComputeThroughput() {
        LatencyTracker tracker = new LatencyTracker(10);
        tracker.record(1_000_000L);
        tracker.record(1_000_000L);
        BenchmarkResult result = tracker.computeResult("Q1", "CONFIG1", 50);
        // 2 ops in 50ms = 40 ops/sec
        assertThat(result.throughputOpsPerSec()).isCloseTo(40.0, within(0.01));
    }

    @Test
    void shouldThrowOnComputeWithNoData() {
        LatencyTracker tracker = new LatencyTracker(10);
        assertThatThrownBy(() -> tracker.computeResult("Q1", "CONFIG1", 10))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldPreserveQueryAndConfigInResult() {
        LatencyTracker tracker = new LatencyTracker(10);
        tracker.record(1_000_000L);
        BenchmarkResult result = tracker.computeResult("Q1", "MONGO_NATIVE_EMBEDDED", 10);
        assertThat(result.queryName()).isEqualTo("Q1");
        assertThat(result.configurationId()).isEqualTo("MONGO_NATIVE_EMBEDDED");
    }

    @Test
    void shouldTrackIterationCount() {
        LatencyTracker tracker = new LatencyTracker(100);
        tracker.record(1_000_000L);
        tracker.record(2_000_000L);
        tracker.record(3_000_000L);
        BenchmarkResult result = tracker.computeResult("Q1", "CONFIG1", 100);
        assertThat(result.iterationCount()).isEqualTo(3);
    }
}
