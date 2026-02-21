package com.helix.benchmark.report;

import com.helix.benchmark.benchmark.BenchmarkResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HtmlReportGeneratorTest {

    @Test
    void shouldGenerateHtmlString() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).isNotBlank();
        assertThat(html).contains("<!DOCTYPE html>");
        assertThat(html).contains("</html>");
    }

    @Test
    void shouldIncludeChartJsCdn() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("chart.js");
    }

    @Test
    void shouldEmbedBenchmarkData() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("const DATA =");
    }

    @Test
    void shouldIncludeQueryNames() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("Q1");
        assertThat(html).contains("Q2");
    }

    @Test
    void shouldIncludeConfigurationIds() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("MONGO_NATIVE_EMBEDDED");
        assertThat(html).contains("ORACLE_JDBC_EMBEDDED");
    }

    @Test
    void shouldWriteToFile(@TempDir Path tempDir) throws Exception {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        Path output = tempDir.resolve("report.html");

        generator.generateToFile(results, output);

        assertThat(output).exists();
        String content = Files.readString(output);
        assertThat(content).contains("<!DOCTYPE html>");
    }

    @Test
    void shouldIncludeSummaryTable() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("<table");
        assertThat(html).contains("p50");
        assertThat(html).contains("p95");
        assertThat(html).contains("p99");
    }

    @Test
    void shouldHighlightFastestConfiguration() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        // Should contain styling for fastest (best) result
        assertThat(html).contains("fastest");
    }

    private List<BenchmarkResult> sampleResults() {
        return List.of(
                new BenchmarkResult("Q1", "MONGO_NATIVE_EMBEDDED",
                        5_000_000L, 10_000_000L, 15_000_000L, 7_000_000.0, 140.0, 200),
                new BenchmarkResult("Q1", "ORACLE_JDBC_EMBEDDED",
                        8_000_000L, 15_000_000L, 20_000_000L, 10_000_000.0, 100.0, 200),
                new BenchmarkResult("Q2", "MONGO_NATIVE_EMBEDDED",
                        3_000_000L, 6_000_000L, 9_000_000L, 4_000_000.0, 250.0, 200),
                new BenchmarkResult("Q2", "ORACLE_JDBC_EMBEDDED",
                        4_000_000L, 8_000_000L, 12_000_000L, 5_000_000.0, 200.0, 200)
        );
    }
}
