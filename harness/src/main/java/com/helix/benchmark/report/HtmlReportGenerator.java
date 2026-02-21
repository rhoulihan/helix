package com.helix.benchmark.report;

import com.helix.benchmark.benchmark.BenchmarkResult;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class HtmlReportGenerator {

    public String generate(List<BenchmarkResult> results) {
        StringBuilder html = new StringBuilder();
        html.append(header());
        html.append(bodyStart());
        html.append(summaryTable(results));
        html.append(chartSection(results));
        html.append(dataScript(results));
        html.append(chartScripts(results));
        html.append(bodyEnd());
        return html.toString();
    }

    public void generateToFile(List<BenchmarkResult> results, Path outputPath) throws IOException {
        String html = generate(results);
        Files.writeString(outputPath, html);
    }

    private String header() {
        return """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Helix Database Benchmark Report</title>
                <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
                <style>
                * { box-sizing: border-box; margin: 0; padding: 0; }
                body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                       background: #0d1117; color: #c9d1d9; padding: 20px; }
                h1 { color: #58a6ff; margin-bottom: 10px; }
                h2 { color: #58a6ff; margin: 30px 0 15px; border-bottom: 1px solid #30363d; padding-bottom: 8px; }
                h3 { color: #8b949e; margin: 20px 0 10px; }
                .meta { color: #8b949e; margin-bottom: 30px; font-size: 0.9em; }
                table { width: 100%%; border-collapse: collapse; margin: 15px 0; }
                th { background: #161b22; color: #58a6ff; padding: 12px 8px; text-align: left; border: 1px solid #30363d; }
                td { padding: 10px 8px; border: 1px solid #30363d; }
                tr:nth-child(even) { background: #161b22; }
                .fastest { background: #0d4429 !important; color: #3fb950; font-weight: bold; }
                .chart-container { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
                                   padding: 20px; margin: 15px 0; }
                .chart-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
                canvas { max-height: 400px; }
                details { margin: 10px 0; }
                summary { cursor: pointer; color: #58a6ff; padding: 8px; background: #161b22;
                          border: 1px solid #30363d; border-radius: 4px; }
                pre { background: #161b22; color: #c9d1d9; padding: 15px; overflow-x: auto;
                      border: 1px solid #30363d; border-radius: 4px; font-size: 0.85em; }
                </style>
                </head>
                """;
    }

    private String bodyStart() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        return """
                <body>
                <h1>Helix Database Benchmark Report</h1>
                <div class="meta">
                  Generated: %s | JVM: %s | OS: %s
                </div>
                """.formatted(timestamp, System.getProperty("java.version"),
                System.getProperty("os.name") + " " + System.getProperty("os.version"));
    }

    private String summaryTable(List<BenchmarkResult> results) {
        Set<String> queries = results.stream().map(BenchmarkResult::queryName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> configs = results.stream().map(BenchmarkResult::configurationId)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, Map<String, BenchmarkResult>> lookup = new HashMap<>();
        for (BenchmarkResult r : results) {
            lookup.computeIfAbsent(r.queryName(), k -> new HashMap<>()).put(r.configurationId(), r);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("<h2>Summary</h2>\n<table>\n<tr><th>Query</th>");
        for (String config : configs) {
            sb.append("<th>").append(config).append("<br>p50 / p95 / p99 (ms)</th>");
        }
        sb.append("</tr>\n");

        for (String query : queries) {
            Map<String, BenchmarkResult> row = lookup.getOrDefault(query, Map.of());
            // Find fastest p50
            String fastestConfig = row.entrySet().stream()
                    .min(Comparator.comparingLong(e -> e.getValue().p50Nanos()))
                    .map(Map.Entry::getKey).orElse("");

            sb.append("<tr><td><strong>").append(query).append("</strong></td>");
            for (String config : configs) {
                BenchmarkResult r = row.get(config);
                if (r != null) {
                    String cls = config.equals(fastestConfig) ? " class=\"fastest\"" : "";
                    sb.append("<td").append(cls).append(">")
                            .append(String.format("%.2f / %.2f / %.2f", r.p50Millis(), r.p95Millis(), r.p99Millis()))
                            .append("<br><small>").append(String.format("%.1f ops/sec", r.throughputOpsPerSec()))
                            .append("</small></td>");
                } else {
                    sb.append("<td>-</td>");
                }
            }
            sb.append("</tr>\n");
        }
        sb.append("</table>\n");
        return sb.toString();
    }

    private String chartSection(List<BenchmarkResult> results) {
        Set<String> queries = results.stream().map(BenchmarkResult::queryName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        StringBuilder sb = new StringBuilder();

        sb.append("<h2>Latency Charts (p50 / p95 / p99)</h2>\n");
        sb.append("<div class=\"chart-grid\">\n");
        for (String query : queries) {
            sb.append("<div class=\"chart-container\">\n");
            sb.append("  <h3>").append(query).append("</h3>\n");
            sb.append("  <canvas id=\"chart-").append(query).append("\"></canvas>\n");
            sb.append("</div>\n");
        }
        sb.append("</div>\n");

        sb.append("<h2>Throughput Comparison</h2>\n");
        sb.append("<div class=\"chart-container\">\n");
        sb.append("  <canvas id=\"chart-throughput\"></canvas>\n");
        sb.append("</div>\n");

        sb.append("<h2>Overall Performance Radar</h2>\n");
        sb.append("<div class=\"chart-container\">\n");
        sb.append("  <canvas id=\"chart-radar\"></canvas>\n");
        sb.append("</div>\n");

        return sb.toString();
    }

    private String dataScript(List<BenchmarkResult> results) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<Map<String, Object>> data = results.stream().map(r -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("query", r.queryName());
                m.put("config", r.configurationId());
                m.put("p50", r.p50Millis());
                m.put("p95", r.p95Millis());
                m.put("p99", r.p99Millis());
                m.put("avg", r.averageMillis());
                m.put("throughput", r.throughputOpsPerSec());
                m.put("iterations", r.iterationCount());
                return m;
            }).toList();
            return "<script>\nconst DATA = " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data) + ";\n</script>\n";
        } catch (Exception e) {
            return "<script>\nconst DATA = [];\n</script>\n";
        }
    }

    private String chartScripts(List<BenchmarkResult> results) {
        Set<String> queries = results.stream().map(BenchmarkResult::queryName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> configs = results.stream().map(BenchmarkResult::configurationId)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        String[] colors = {"#58a6ff", "#3fb950", "#d29922", "#f85149", "#bc8cff", "#79c0ff"};

        StringBuilder sb = new StringBuilder();
        sb.append("<script>\n");
        sb.append("const COLORS = ").append(Arrays.toString(colors)).append(";\n");
        sb.append("const CONFIGS = ").append(toJsArray(configs)).append(";\n");
        sb.append("const QUERIES = ").append(toJsArray(queries)).append(";\n\n");

        // Per-query bar charts
        sb.append("""
                QUERIES.forEach(q => {
                  const ctx = document.getElementById('chart-' + q);
                  if (!ctx) return;
                  const qData = DATA.filter(d => d.query === q);
                  new Chart(ctx, {
                    type: 'bar',
                    data: {
                      labels: qData.map(d => d.config),
                      datasets: [
                        { label: 'p50 (ms)', data: qData.map(d => d.p50), backgroundColor: '#58a6ff80' },
                        { label: 'p95 (ms)', data: qData.map(d => d.p95), backgroundColor: '#d2992280' },
                        { label: 'p99 (ms)', data: qData.map(d => d.p99), backgroundColor: '#f8514980' }
                      ]
                    },
                    options: {
                      responsive: true,
                      scales: { y: { beginAtZero: true, title: { display: true, text: 'Latency (ms)', color: '#8b949e' },
                                     grid: { color: '#30363d' }, ticks: { color: '#8b949e' } },
                                x: { ticks: { color: '#8b949e', maxRotation: 45 }, grid: { color: '#30363d' } } },
                      plugins: { legend: { labels: { color: '#c9d1d9' } } }
                    }
                  });
                });
                """);

        // Throughput chart
        sb.append("""
                {
                  const ctx = document.getElementById('chart-throughput');
                  const datasets = CONFIGS.map((cfg, i) => ({
                    label: cfg,
                    data: QUERIES.map(q => { const d = DATA.find(d => d.query === q && d.config === cfg); return d ? d.throughput : 0; }),
                    backgroundColor: COLORS[i % COLORS.length] + '80'
                  }));
                  new Chart(ctx, {
                    type: 'bar',
                    data: { labels: QUERIES, datasets },
                    options: {
                      indexAxis: 'y', responsive: true,
                      scales: { x: { title: { display: true, text: 'ops/sec', color: '#8b949e' },
                                     grid: { color: '#30363d' }, ticks: { color: '#8b949e' } },
                                y: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } } },
                      plugins: { legend: { labels: { color: '#c9d1d9' } } }
                    }
                  });
                }
                """);

        // Radar chart
        sb.append("""
                {
                  const ctx = document.getElementById('chart-radar');
                  const maxP50 = Math.max(...DATA.map(d => d.p50));
                  const datasets = CONFIGS.map((cfg, i) => ({
                    label: cfg,
                    data: QUERIES.map(q => {
                      const d = DATA.find(d => d.query === q && d.config === cfg);
                      return d ? (1 - d.p50 / maxP50) * 100 : 0;
                    }),
                    borderColor: COLORS[i % COLORS.length],
                    backgroundColor: COLORS[i % COLORS.length] + '20',
                    pointBackgroundColor: COLORS[i % COLORS.length]
                  }));
                  new Chart(ctx, {
                    type: 'radar',
                    data: { labels: QUERIES, datasets },
                    options: {
                      responsive: true,
                      scales: { r: { beginAtZero: true, grid: { color: '#30363d' },
                                     pointLabels: { color: '#c9d1d9' }, ticks: { color: '#8b949e' } } },
                      plugins: { legend: { labels: { color: '#c9d1d9' } } }
                    }
                  });
                }
                """);

        sb.append("</script>\n");
        return sb.toString();
    }

    private String bodyEnd() {
        return "</body>\n</html>";
    }

    private String toJsArray(Collection<String> items) {
        return "[" + items.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + "]";
    }
}
