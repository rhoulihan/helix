package com.helix.benchmark.benchmark;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class BenchmarkRunnerTest {

    @Test
    void shouldRunWarmUpAndMeasurementIterations() {
        AtomicInteger callCount = new AtomicInteger(0);
        Supplier<Void> workload = () -> {
            callCount.incrementAndGet();
            return null;
        };
        BenchmarkRunner runner = new BenchmarkRunner(5, 10);
        BenchmarkResult result = runner.run("Q1", "CONFIG1", workload);

        // 5 warm-up + 10 measurement = 15 total calls
        assertThat(callCount.get()).isEqualTo(15);
        assertThat(result.iterationCount()).isEqualTo(10);
    }

    @Test
    void shouldProduceValidPercentiles() {
        Supplier<Void> workload = () -> {
            try { Thread.sleep(1); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return null;
        };
        BenchmarkRunner runner = new BenchmarkRunner(2, 10);
        BenchmarkResult result = runner.run("Q1", "CONFIG1", workload);

        assertThat(result.p50Nanos()).isGreaterThan(0);
        assertThat(result.p95Nanos()).isGreaterThanOrEqualTo(result.p50Nanos());
        assertThat(result.p99Nanos()).isGreaterThanOrEqualTo(result.p95Nanos());
    }

    @Test
    void shouldCalculateThroughput() {
        Supplier<Void> workload = () -> null;
        BenchmarkRunner runner = new BenchmarkRunner(0, 100);
        BenchmarkResult result = runner.run("Q1", "CONFIG1", workload);

        assertThat(result.throughputOpsPerSec()).isGreaterThan(0);
    }

    @Test
    void shouldPreserveQueryAndConfigNames() {
        Supplier<Void> workload = () -> null;
        BenchmarkRunner runner = new BenchmarkRunner(0, 5);
        BenchmarkResult result = runner.run("Q5", "ORACLE_JDBC_NORMALIZED", workload);

        assertThat(result.queryName()).isEqualTo("Q5");
        assertThat(result.configurationId()).isEqualTo("ORACLE_JDBC_NORMALIZED");
    }
}
