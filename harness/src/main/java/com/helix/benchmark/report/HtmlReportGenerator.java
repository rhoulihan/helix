package com.helix.benchmark.report;

import com.helix.benchmark.benchmark.BenchmarkResult;
import com.helix.benchmark.benchmark.QueryDetail;
import com.helix.benchmark.query.QueryDefinition;
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
        return generate(results, List.of());
    }

    public String generate(List<BenchmarkResult> results, List<QueryDetail> details) {
        StringBuilder html = new StringBuilder();
        html.append(header());
        html.append(bodyStart());
        html.append(tabBar(results));
        html.append(overviewTab(results));
        html.append(queryTabs(results, details));
        html.append(dataScript(results, details));
        html.append(chartScripts(results));
        html.append(tabScript());
        html.append(bodyEnd());
        return html.toString();
    }

    public void generateToFile(List<BenchmarkResult> results, Path outputPath) throws IOException {
        generateToFile(results, List.of(), outputPath);
    }

    public void generateToFile(List<BenchmarkResult> results, List<QueryDetail> details, Path outputPath) throws IOException {
        String html = generate(results, details);
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
                .meta { color: #8b949e; margin-bottom: 20px; font-size: 0.9em; }

                /* Main tab bar */
                .tab-bar { display: flex; gap: 0; background: #161b22; border-bottom: 2px solid #30363d;
                           margin-bottom: 20px; overflow-x: auto; }
                .tab-btn { padding: 10px 20px; background: transparent; color: #8b949e; border: none;
                           border-bottom: 2px solid transparent; cursor: pointer; font-size: 0.95em;
                           white-space: nowrap; margin-bottom: -2px; }
                .tab-btn:hover { color: #c9d1d9; }
                .tab-btn.active { color: #58a6ff; border-bottom-color: #58a6ff; font-weight: bold; }
                .tab-content { display: none; }
                .tab-content.active { display: block; }

                /* Subtab bar */
                .subtab-bar { display: flex; gap: 0; background: #0d1117; border-bottom: 1px solid #30363d;
                              margin-bottom: 15px; overflow-x: auto; }
                .subtab-btn { padding: 7px 14px; background: transparent; color: #8b949e; border: none;
                              border-bottom: 2px solid transparent; cursor: pointer; font-size: 0.85em;
                              white-space: nowrap; margin-bottom: -1px; }
                .subtab-btn:hover { color: #c9d1d9; }
                .subtab-btn.active { color: #58a6ff; border-bottom-color: #58a6ff; }
                .subtab-content { display: none; }
                .subtab-content.active { display: block; }

                /* Tables */
                table { width: 100%; border-collapse: collapse; margin: 15px 0; }
                th { background: #161b22; color: #58a6ff; padding: 12px 8px; text-align: left;
                     border: 1px solid #30363d; }
                td { padding: 10px 8px; border: 1px solid #30363d; }
                tr:nth-child(even) { background: #161b22; }
                .fastest { background: #0d4429 !important; color: #3fb950; font-weight: bold; }

                /* Charts */
                .chart-container { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
                                   padding: 20px; margin: 15px 0; }
                .chart-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
                canvas { max-height: 400px; }

                /* Pre blocks for query text / explain plans */
                pre { background: #161b22; color: #c9d1d9; padding: 15px; overflow-x: auto;
                      overflow-y: auto; max-height: 500px; border: 1px solid #30363d;
                      border-radius: 4px; font-size: 0.85em; white-space: pre-wrap; word-wrap: break-word; }

                /* SQL Monitor button */
                .sql-monitor-btn { display: inline-block; padding: 8px 16px; background: #238636;
                                   color: #ffffff; border-radius: 6px; text-decoration: none;
                                   font-size: 0.9em; margin: 10px 0; }
                .sql-monitor-btn:hover { background: #2ea043; }

                /* Metrics grid */
                .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                                gap: 10px; margin: 15px 0; }
                .metric-card { background: #161b22; border: 1px solid #30363d; border-radius: 6px;
                               padding: 12px; text-align: center; }
                .metric-value { font-size: 1.4em; color: #58a6ff; font-weight: bold; }
                .metric-label { font-size: 0.8em; color: #8b949e; margin-top: 4px; }

                /* Query description */
                .query-desc { color: #8b949e; font-size: 0.95em; margin-bottom: 15px;
                              padding: 10px; background: #161b22; border-left: 3px solid #58a6ff;
                              border-radius: 0 4px 4px 0; }
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

    private String tabBar(List<BenchmarkResult> results) {
        Set<String> queries = results.stream().map(BenchmarkResult::queryName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        StringBuilder sb = new StringBuilder();
        sb.append("<div class=\"tab-bar\">\n");
        sb.append("  <button class=\"tab-btn active\" data-tab=\"tab-overview\">Overview</button>\n");
        for (String q : queries) {
            sb.append("  <button class=\"tab-btn\" data-tab=\"tab-").append(q).append("\">").append(q).append("</button>\n");
        }
        sb.append("</div>\n");
        return sb.toString();
    }

    private String overviewTab(List<BenchmarkResult> results) {
        StringBuilder sb = new StringBuilder();
        sb.append("<div id=\"tab-overview\" class=\"tab-content active\">\n");
        sb.append(summaryTable(results));
        sb.append(chartSection(results));
        sb.append("</div>\n");
        return sb.toString();
    }

    private String queryTabs(List<BenchmarkResult> results, List<QueryDetail> details) {
        Set<String> queries = results.stream().map(BenchmarkResult::queryName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> configs = results.stream().map(BenchmarkResult::configurationId)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, Map<String, BenchmarkResult>> lookup = buildLookup(results);
        Map<String, Map<String, QueryDetail>> detailLookup = buildDetailLookup(details);

        StringBuilder sb = new StringBuilder();
        for (String query : queries) {
            sb.append("<div id=\"tab-").append(query).append("\" class=\"tab-content\">\n");

            // Query description
            QueryDefinition qDef = findQueryDef(query);
            if (qDef != null) {
                sb.append("<div class=\"query-desc\">").append(escapeHtml(qDef.description())).append("</div>\n");
            }

            // Per-query summary table
            sb.append(querySummaryTable(query, configs, lookup));

            // Per-query bar chart
            sb.append("<div class=\"chart-container\">\n");
            sb.append("  <h3>Latency: p50 / p95 / p99</h3>\n");
            sb.append("  <canvas id=\"detail-chart-").append(query).append("\"></canvas>\n");
            sb.append("</div>\n");

            // Subtab bar for endpoints
            Map<String, BenchmarkResult> queryResults = lookup.getOrDefault(query, Map.of());
            Map<String, QueryDetail> queryDetails = detailLookup.getOrDefault(query, Map.of());
            List<String> activeConfigs = configs.stream()
                    .filter(c -> queryResults.containsKey(c) || queryDetails.containsKey(c))
                    .toList();

            if (!activeConfigs.isEmpty()) {
                sb.append("<div class=\"subtab-bar\" data-query=\"").append(query).append("\">\n");
                boolean first = true;
                for (String cfg : activeConfigs) {
                    String activeClass = first ? " active" : "";
                    sb.append("  <button class=\"subtab-btn").append(activeClass)
                            .append("\" data-subtab=\"subtab-").append(query).append("-").append(cfg)
                            .append("\">").append(formatConfigName(cfg)).append("</button>\n");
                    first = false;
                }
                sb.append("</div>\n");

                // Subtab content
                first = true;
                for (String cfg : activeConfigs) {
                    String activeClass = first ? " active" : "";
                    sb.append("<div id=\"subtab-").append(query).append("-").append(cfg)
                            .append("\" class=\"subtab-content").append(activeClass).append("\">\n");

                    // Performance metrics
                    BenchmarkResult r = queryResults.get(cfg);
                    if (r != null) {
                        sb.append(metricsGrid(r));
                    }

                    // Query text and explain plan
                    QueryDetail detail = queryDetails.get(cfg);
                    if (detail != null) {
                        sb.append("<h3>Query Text</h3>\n");
                        sb.append("<pre>").append(escapeHtml(detail.queryText())).append("</pre>\n");

                        sb.append("<h3>Explain Plan</h3>\n");
                        sb.append("<pre>").append(escapeHtml(detail.explainPlan())).append("</pre>\n");

                        // SQL Monitor link (Oracle JDBC only)
                        String monitorUrl = detail.activeSqlMonitorUrl();
                        if (monitorUrl != null) {
                            sb.append("<a class=\"sql-monitor-btn\" href=\"")
                                    .append(escapeHtml(monitorUrl))
                                    .append("\" target=\"_blank\">Active SQL Monitor Report</a>\n");
                        }
                    }

                    sb.append("</div>\n");
                    first = false;
                }
            }

            sb.append("</div>\n");
        }
        return sb.toString();
    }

    private String summaryTable(List<BenchmarkResult> results) {
        Set<String> queries = results.stream().map(BenchmarkResult::queryName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> configs = results.stream().map(BenchmarkResult::configurationId)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, Map<String, BenchmarkResult>> lookup = buildLookup(results);

        StringBuilder sb = new StringBuilder();
        sb.append("<h2>Summary</h2>\n<table>\n<tr><th>Query</th>");
        for (String config : configs) {
            sb.append("<th>").append(formatConfigName(config)).append("<br>p50 / p95 / p99 (ms)</th>");
        }
        sb.append("</tr>\n");

        for (String query : queries) {
            Map<String, BenchmarkResult> row = lookup.getOrDefault(query, Map.of());
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

    private String querySummaryTable(String query, Set<String> configs,
                                      Map<String, Map<String, BenchmarkResult>> lookup) {
        Map<String, BenchmarkResult> row = lookup.getOrDefault(query, Map.of());
        if (row.isEmpty()) return "";

        String fastestConfig = row.entrySet().stream()
                .min(Comparator.comparingLong(e -> e.getValue().p50Nanos()))
                .map(Map.Entry::getKey).orElse("");

        StringBuilder sb = new StringBuilder();
        sb.append("<table>\n<tr><th>Configuration</th><th>p50 (ms)</th><th>p95 (ms)</th><th>p99 (ms)</th>")
                .append("<th>Avg (ms)</th><th>Throughput</th><th>Iterations</th></tr>\n");
        for (String config : configs) {
            BenchmarkResult r = row.get(config);
            if (r != null) {
                String cls = config.equals(fastestConfig) ? " class=\"fastest\"" : "";
                sb.append("<tr").append(cls).append(">");
                sb.append("<td>").append(formatConfigName(config)).append("</td>");
                sb.append("<td>").append(String.format("%.2f", r.p50Millis())).append("</td>");
                sb.append("<td>").append(String.format("%.2f", r.p95Millis())).append("</td>");
                sb.append("<td>").append(String.format("%.2f", r.p99Millis())).append("</td>");
                sb.append("<td>").append(String.format("%.2f", r.averageMillis())).append("</td>");
                sb.append("<td>").append(String.format("%.1f ops/sec", r.throughputOpsPerSec())).append("</td>");
                sb.append("<td>").append(r.iterationCount()).append("</td>");
                sb.append("</tr>\n");
            }
        }
        sb.append("</table>\n");
        return sb.toString();
    }

    private String metricsGrid(BenchmarkResult r) {
        return """
                <div class="metrics-grid">
                  <div class="metric-card"><div class="metric-value">%.2f</div><div class="metric-label">p50 (ms)</div></div>
                  <div class="metric-card"><div class="metric-value">%.2f</div><div class="metric-label">p95 (ms)</div></div>
                  <div class="metric-card"><div class="metric-value">%.2f</div><div class="metric-label">p99 (ms)</div></div>
                  <div class="metric-card"><div class="metric-value">%.2f</div><div class="metric-label">avg (ms)</div></div>
                  <div class="metric-card"><div class="metric-value">%.1f</div><div class="metric-label">ops/sec</div></div>
                  <div class="metric-card"><div class="metric-value">%d</div><div class="metric-label">iterations</div></div>
                </div>
                """.formatted(r.p50Millis(), r.p95Millis(), r.p99Millis(),
                r.averageMillis(), r.throughputOpsPerSec(), r.iterationCount());
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

    private String dataScript(List<BenchmarkResult> results, List<QueryDetail> details) {
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
        sb.append("const COLORS = ").append(toJsArray(List.of(colors))).append(";\n");
        sb.append("const CONFIGS = ").append(toJsArray(configs)).append(";\n");
        sb.append("const QUERIES = ").append(toJsArray(queries)).append(";\n\n");

        // Per-query bar charts (overview)
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

        // Per-query detail charts (in Q# tabs)
        sb.append("""
                QUERIES.forEach(q => {
                  const ctx = document.getElementById('detail-chart-' + q);
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

    private String tabScript() {
        return """
                <script>
                // Main tab switching
                document.querySelectorAll('.tab-btn').forEach(btn => {
                  btn.addEventListener('click', () => {
                    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                    btn.classList.add('active');
                    const target = document.getElementById(btn.dataset.tab);
                    if (target) target.classList.add('active');
                  });
                });

                // Subtab switching (scoped within parent tab)
                document.querySelectorAll('.subtab-btn').forEach(btn => {
                  btn.addEventListener('click', () => {
                    const bar = btn.closest('.subtab-bar');
                    const query = bar.dataset.query;
                    bar.querySelectorAll('.subtab-btn').forEach(b => b.classList.remove('active'));
                    // Find all subtab-content siblings after this bar
                    let sibling = bar.nextElementSibling;
                    while (sibling && !sibling.classList.contains('subtab-bar') && !sibling.classList.contains('tab-content')) {
                      if (sibling.classList.contains('subtab-content')) {
                        sibling.classList.remove('active');
                      }
                      sibling = sibling.nextElementSibling;
                    }
                    btn.classList.add('active');
                    const target = document.getElementById(btn.dataset.subtab);
                    if (target) target.classList.add('active');
                  });
                });
                </script>
                """;
    }

    private String bodyEnd() {
        return "</body>\n</html>";
    }

    // --- Utility methods ---

    private Map<String, Map<String, BenchmarkResult>> buildLookup(List<BenchmarkResult> results) {
        Map<String, Map<String, BenchmarkResult>> lookup = new HashMap<>();
        for (BenchmarkResult r : results) {
            lookup.computeIfAbsent(r.queryName(), k -> new HashMap<>()).put(r.configurationId(), r);
        }
        return lookup;
    }

    private Map<String, Map<String, QueryDetail>> buildDetailLookup(List<QueryDetail> details) {
        Map<String, Map<String, QueryDetail>> lookup = new HashMap<>();
        for (QueryDetail d : details) {
            lookup.computeIfAbsent(d.queryName(), k -> new HashMap<>()).put(d.configurationId(), d);
        }
        return lookup;
    }

    private QueryDefinition findQueryDef(String queryName) {
        for (QueryDefinition qd : QueryDefinition.values()) {
            if (qd.queryName().equals(queryName)) return qd;
        }
        return null;
    }

    static String formatConfigName(String configId) {
        return switch (configId) {
            case "MONGO_NATIVE_EMBEDDED" -> "MongoDB Native";
            case "ORACLE_JDBC_EMBEDDED" -> "Oracle JSON (JDBC)";
            case "ORACLE_MONGO_API_EMBEDDED" -> "Oracle Mongo API";
            case "ORACLE_RELATIONAL_EMBEDDED" -> "Oracle Relational";
            case "ORACLE_DUALITY_VIEW_EMBEDDED" -> "Oracle Duality View";
            case "ORACLE_MONGO_API_DV_EMBEDDED" -> "Oracle Mongo API (DV)";
            default -> configId;
        };
    }

    static String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;");
    }

    private String toJsArray(Collection<String> items) {
        return "[" + items.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + "]";
    }
}
