package com.helix.benchmark.benchmark;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class BenchmarkResultTest {

    @Test
    void shouldStoreAllFields() {
        BenchmarkResult result = new BenchmarkResult(
                "Q1", "MONGO_NATIVE_EMBEDDED",
                5_000_000L, 10_000_000L, 15_000_000L,
                7_500_000.0, 133.33, 200
        );

        assertThat(result.queryName()).isEqualTo("Q1");
        assertThat(result.configurationId()).isEqualTo("MONGO_NATIVE_EMBEDDED");
        assertThat(result.p50Nanos()).isEqualTo(5_000_000L);
        assertThat(result.p95Nanos()).isEqualTo(10_000_000L);
        assertThat(result.p99Nanos()).isEqualTo(15_000_000L);
        assertThat(result.averageNanos()).isCloseTo(7_500_000.0, within(0.01));
        assertThat(result.throughputOpsPerSec()).isCloseTo(133.33, within(0.01));
        assertThat(result.iterationCount()).isEqualTo(200);
    }

    @Test
    void shouldConvertNanosToMillis() {
        BenchmarkResult result = new BenchmarkResult(
                "Q1", "CONFIG1",
                5_000_000L, 10_000_000L, 15_000_000L,
                7_500_000.0, 100.0, 200
        );
        assertThat(result.p50Millis()).isCloseTo(5.0, within(0.01));
        assertThat(result.p95Millis()).isCloseTo(10.0, within(0.01));
        assertThat(result.p99Millis()).isCloseTo(15.0, within(0.01));
        assertThat(result.averageMillis()).isCloseTo(7.5, within(0.01));
    }
}
