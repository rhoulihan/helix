package com.helix.benchmark.report;

import com.helix.benchmark.benchmark.BenchmarkResult;
import com.helix.benchmark.benchmark.QueryDetail;
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
    void shouldWriteToFileWithDetails(@TempDir Path tempDir) throws Exception {
        List<BenchmarkResult> results = sampleResults();
        List<QueryDetail> details = sampleDetails();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        Path output = tempDir.resolve("report.html");

        generator.generateToFile(results, details, output);

        assertThat(output).exists();
        String content = Files.readString(output);
        assertThat(content).contains("<!DOCTYPE html>");
        assertThat(content).contains("SELECT");
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
    void shouldOutputColorsAsQuotedJsStrings() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("'#58a6ff'");
        assertThat(html).contains("'#3fb950'");
        assertThat(html).doesNotContainPattern("COLORS = \\[#[0-9a-f]");
    }

    @Test
    void shouldHighlightFastestConfiguration() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("fastest");
    }

    // --- Tab tests ---

    @Test
    void shouldIncludeOverviewTab() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("id=\"tab-overview\"");
        assertThat(html).contains("data-tab=\"tab-overview\"");
    }

    @Test
    void shouldIncludeQueryTabs() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("id=\"tab-Q1\"");
        assertThat(html).contains("id=\"tab-Q2\"");
        assertThat(html).contains("data-tab=\"tab-Q1\"");
        assertThat(html).contains("data-tab=\"tab-Q2\"");
    }

    @Test
    void shouldIncludeQueryTextAndExplainPlan() {
        List<BenchmarkResult> results = sampleResults();
        List<QueryDetail> details = sampleDetails();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results, details);

        assertThat(html).contains("Query Text");
        assertThat(html).contains("Explain Plan");
        assertThat(html).contains("SELECT * FROM jdbc_book_role_investor");
        assertThat(html).contains("FULL TABLE SCAN");
    }

    @Test
    void shouldIncludeSqlMonitorLinkForJdbcConfigs() {
        List<BenchmarkResult> results = sampleResults();
        List<QueryDetail> details = sampleDetails();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results, details);

        assertThat(html).contains("Active SQL Monitor Report");
        assertThat(html).contains("sql-monitor-btn");
        assertThat(html).contains("/_/sql/abc123");
    }

    @Test
    void shouldNotIncludeSqlMonitorLinkForMongoConfigs() {
        List<BenchmarkResult> results = sampleResults();
        List<QueryDetail> details = List.of(
                new QueryDetail("Q1", "MONGO_NATIVE_EMBEDDED",
                        "[ { $match: ... } ]", "{ explain output }", null, null)
        );
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results, details);

        // The mongo detail should not have a SQL Monitor link
        // Check that the subtab for MONGO_NATIVE_EMBEDDED does not contain sql-monitor-btn
        int mongoSubtabStart = html.indexOf("id=\"subtab-Q1-MONGO_NATIVE_EMBEDDED\"");
        assertThat(mongoSubtabStart).isGreaterThan(-1);
        int mongoSubtabEnd = html.indexOf("</div>", mongoSubtabStart);
        String mongoSubtab = html.substring(mongoSubtabStart, mongoSubtabEnd);
        assertThat(mongoSubtab).doesNotContain("sql-monitor-btn");
    }

    @Test
    void shouldIncludeTabSwitchingJavascript() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("tab-btn");
        assertThat(html).contains("addEventListener");
        assertThat(html).contains("classList.remove('active')");
        assertThat(html).contains("classList.add('active')");
    }

    @Test
    void shouldIncludeSubtabs() {
        List<BenchmarkResult> results = sampleResults();
        List<QueryDetail> details = sampleDetails();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results, details);

        assertThat(html).contains("subtab-bar");
        assertThat(html).contains("subtab-btn");
        assertThat(html).contains("subtab-content");
    }

    @Test
    void shouldEscapeHtmlInQueryText() {
        assertThat(HtmlReportGenerator.escapeHtml("<script>alert('xss')</script>"))
                .doesNotContain("<script>")
                .contains("&lt;script&gt;");
    }

    @Test
    void shouldFormatConfigNames() {
        assertThat(HtmlReportGenerator.formatConfigName("MONGO_NATIVE_EMBEDDED"))
                .isEqualTo("MongoDB Native");
        assertThat(HtmlReportGenerator.formatConfigName("ORACLE_JDBC_EMBEDDED"))
                .isEqualTo("Oracle JSON (JDBC)");
        assertThat(HtmlReportGenerator.formatConfigName("ORACLE_MONGO_API_EMBEDDED"))
                .isEqualTo("Oracle Mongo API");
        assertThat(HtmlReportGenerator.formatConfigName("ORACLE_RELATIONAL_EMBEDDED"))
                .isEqualTo("Oracle Relational");
        assertThat(HtmlReportGenerator.formatConfigName("ORACLE_DUALITY_VIEW_EMBEDDED"))
                .isEqualTo("Oracle Duality View");
        assertThat(HtmlReportGenerator.formatConfigName("ORACLE_MONGO_API_DV_EMBEDDED"))
                .isEqualTo("Oracle Mongo API (DV)");
    }

    @Test
    void shouldIncludePerQueryDetailCharts() {
        List<BenchmarkResult> results = sampleResults();
        HtmlReportGenerator generator = new HtmlReportGenerator();
        String html = generator.generate(results);

        assertThat(html).contains("detail-chart-Q1");
        assertThat(html).contains("detail-chart-Q2");
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

    private List<QueryDetail> sampleDetails() {
        return List.of(
                new QueryDetail("Q1", "MONGO_NATIVE_EMBEDDED",
                        "[ { $match: { advisorId: 'ADV001' } } ]",
                        "{ queryPlanner: { ... } }", null, null),
                new QueryDetail("Q1", "ORACLE_JDBC_EMBEDDED",
                        "SELECT * FROM jdbc_book_role_investor b WHERE ...",
                        "FULL TABLE SCAN on jdbc_book_role_investor",
                        "abc123", "https://localhost:8443/ords/admin"),
                new QueryDetail("Q2", "MONGO_NATIVE_EMBEDDED",
                        "[ { $match: { name: /smith/i } } ]",
                        "{ queryPlanner: { ... } }", null, null),
                new QueryDetail("Q2", "ORACLE_JDBC_EMBEDDED",
                        "SELECT * FROM jdbc_book_role_investor b WHERE name LIKE '%smith%'",
                        "INDEX SCAN on IDX_NAME",
                        "def456", "https://localhost:8443/ords/admin")
        );
    }
}
