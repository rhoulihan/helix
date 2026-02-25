package com.helix.benchmark.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.helix.benchmark.HelixBenchmarkMain;
import com.helix.benchmark.config.BenchmarkConfig;
import com.helix.benchmark.config.DatabaseTarget;
import com.helix.benchmark.config.SchemaModel;
import com.helix.benchmark.connection.ConnectionManager;
import com.helix.benchmark.datagen.ReferenceRegistry;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;

/**
 * Runs all 9 queries against all 6 database targets and generates an HTML page
 * with one tab per query showing query implementations and result sets per target.
 */
public class QueryResultsHtmlGenerator {

    private static final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    static final String[] TARGET_NAMES = {
        "MongoDB Native",
        "Oracle JDBC (SODA JSON)",
        "Oracle MongoDB API",
        "Oracle Relational"
    };

    record TargetResult(String targetName, List<String> docs, int count, long elapsedMs, String error) {}

    record QueryResult(
        QueryDefinition query,
        Map<String, Object> params,
        Map<String, String> queryTexts,    // targetName -> query text
        List<TargetResult> targetResults
    ) {}

    public static void main(String[] args) throws Exception {
        for (String p : new String[]{"docker/truststore.jks", "../docker/truststore.jks", "truststore.jks"}) {
            if (new java.io.File(p).exists()) {
                System.setProperty("javax.net.ssl.trustStore", new java.io.File(p).getAbsolutePath());
                System.setProperty("javax.net.ssl.trustStoreType", "JKS");
                break;
            }
        }

        BenchmarkConfig config = HelixBenchmarkMain.loadConfig(null);
        ConnectionManager connMgr = new ConnectionManager(config);

        ReferenceRegistry registry = new ReferenceRegistry(
                config.advisoryContextPoolSize(), config.partyRoleIdPoolSize(), config.finInstIdPoolSize());
        String mongoConnStr = connMgr.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
        String mongoDbName = connMgr.getDatabaseName(DatabaseTarget.MONGO_NATIVE);

        try (MongoClient client = MongoClients.create(mongoConnStr)) {
            MongoDatabase db = client.getDatabase(mongoDbName);
            for (Document doc : db.getCollection("advisor").find().projection(new Document("_id", 1)))
                registry.registerAdvisorId(doc.getString("_id"));
            for (Document doc : db.getCollection("bookRoleInvestor").find()
                    .projection(new Document("investorId", 1)).limit(10000)) {
                String id = doc.getString("investorId");
                if (id != null) registry.registerInvestorId(id);
            }
        }

        QueryParameterGenerator paramGen = new QueryParameterGenerator(registry);
        MongoQueryExecutor mongoExec = new MongoQueryExecutor();
        OracleJdbcQueryExecutor jdbcExec = new OracleJdbcQueryExecutor();
        OracleRelationalQueryExecutor relExec = new OracleRelationalQueryExecutor();

        try (MongoClient initClient = MongoClients.create(mongoConnStr)) {
            paramGen.initFromData(initClient.getDatabase(mongoDbName), 50);
        }

        Map<QueryDefinition, Map<String, Object>> targetedParams =
                buildTargetedParams(mongoConnStr, mongoDbName, paramGen);

        // Connection strings for Mongo API targets
        String oraMongoApiConnStr = connMgr.getMongoConnectionString(DatabaseTarget.ORACLE_MONGO_API);
        String oraMongoApiDbName = connMgr.getDatabaseName(DatabaseTarget.ORACLE_MONGO_API);

        HikariDataSource ds = createDs(connMgr);

        List<QueryResult> results = new ArrayList<>();

        for (QueryDefinition query : QueryDefinition.values()) {
            Map<String, Object> params = targetedParams.get(query);
            System.out.printf("%n=== %s: %s ===%n", query.queryName(), query.description());

            // Build query text for each target
            Map<String, String> queryTexts = new LinkedHashMap<>();

            // MongoDB Native query text
            try (MongoClient client = MongoClients.create(mongoConnStr)) {
                MongoCollection<Document> col = client.getDatabase(mongoDbName).getCollection(query.embeddedCollection());
                CodecRegistry cr = col.getCodecRegistry();
                if (query.isAggregation()) {
                    var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED, params, DatabaseTarget.MONGO_NATIVE);
                    queryTexts.put(TARGET_NAMES[0], "db." + query.embeddedCollection() + ".aggregate(\n" +
                            mongoExec.serializePipeline(pipeline, cr) + "\n)");
                } else {
                    var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params, DatabaseTarget.MONGO_NATIVE);
                    queryTexts.put(TARGET_NAMES[0], "db." + query.embeddedCollection() + ".find(\n" +
                            mongoExec.serializeFilter(filter, cr) + "\n)");
                }
            }

            // Oracle JDBC SQL
            var jdbcQ = jdbcExec.buildSql(query, SchemaModel.EMBEDDED, params);
            queryTexts.put(TARGET_NAMES[1], jdbcExec.buildDisplaySql(jdbcQ));

            // Oracle MongoDB API query text
            try (MongoClient client = MongoClients.create(oraMongoApiConnStr)) {
                String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API);
                MongoCollection<Document> col = client.getDatabase(oraMongoApiDbName).getCollection(collName);
                CodecRegistry cr = col.getCodecRegistry();
                if (query.isAggregation()) {
                    var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED, params, DatabaseTarget.ORACLE_MONGO_API);
                    queryTexts.put(TARGET_NAMES[2], "db." + collName + ".aggregate(\n" +
                            mongoExec.serializePipeline(pipeline, cr) + "\n)");
                } else {
                    var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params, DatabaseTarget.ORACLE_MONGO_API);
                    queryTexts.put(TARGET_NAMES[2], "db." + collName + ".find(\n" +
                            mongoExec.serializeFilter(filter, cr) + "\n)");
                }
            }

            // Oracle Relational SQL
            var relQ = relExec.buildSql(query, SchemaModel.EMBEDDED, params);
            queryTexts.put(TARGET_NAMES[3], jdbcExec.buildDisplaySql(relQ));

            // Execute against all 4 targets
            List<TargetResult> targetResults = new ArrayList<>();

            // 1. MongoDB Native
            targetResults.add(executeMongoTarget(TARGET_NAMES[0], mongoConnStr, mongoDbName,
                    query, mongoExec, params, DatabaseTarget.MONGO_NATIVE));

            // 2. Oracle JDBC (SODA JSON)
            targetResults.add(executeJdbcTarget(TARGET_NAMES[1], ds, jdbcExec, query, params));

            // 3. Oracle MongoDB API
            targetResults.add(executeMongoTarget(TARGET_NAMES[2], oraMongoApiConnStr, oraMongoApiDbName,
                    query, mongoExec, params, DatabaseTarget.ORACLE_MONGO_API));

            // 4. Oracle Relational
            targetResults.add(executeJdbcTarget(TARGET_NAMES[3], ds, relExec, query, params));

            results.add(new QueryResult(query, params, queryTexts, targetResults));
        }

        ds.close();

        String outputPath = "query-results.html";
        generateHtml(results, outputPath);
        System.out.printf("%nReport generated: %s%n", new java.io.File(outputPath).getAbsolutePath());
    }

    // --- Execute against a MongoDB-driver target ---
    private static TargetResult executeMongoTarget(String name, String connStr, String dbName,
            QueryDefinition query, MongoQueryExecutor mongoExec,
            Map<String, Object> params, DatabaseTarget target) {
        System.out.printf("  %s ...", name);
        try (MongoClient client = MongoClients.create(connStr)) {
            MongoDatabase db = client.getDatabase(dbName);
            String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, target);
            MongoCollection<Document> col = db.getCollection(collName);

            long t0 = System.nanoTime();
            List<Document> docs;
            if (query.isAggregation()) {
                var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED, params, target);
                docs = col.aggregate(pipeline).into(new ArrayList<>());
            } else {
                var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params, target);
                docs = col.find(filter).into(new ArrayList<>());
            }
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

            List<String> jsonDocs = new ArrayList<>();
            for (Document doc : docs) {
                JsonNode node = mapper.readTree(doc.toJson());
                jsonDocs.add(mapper.writeValueAsString(node));
            }
            System.out.printf(" %d docs, %d ms%n", docs.size(), elapsedMs);
            return new TargetResult(name, jsonDocs, docs.size(), elapsedMs, null);
        } catch (Exception e) {
            System.out.printf(" ERROR: %s%n", e.getMessage());
            return new TargetResult(name, List.of(), 0, 0, e.getMessage());
        }
    }

    // --- Execute against a JDBC target ---
    private static TargetResult executeJdbcTarget(String name, HikariDataSource ds,
            OracleJdbcQueryExecutor exec, QueryDefinition query, Map<String, Object> params) {
        System.out.printf("  %s ...", name);
        try {
            var sqlQuery = exec.buildSql(query, SchemaModel.EMBEDDED, params);
            List<String> jsonDocs = new ArrayList<>();
            long t0 = System.nanoTime();

            try (Connection conn = ds.getConnection();
                 var ps = conn.prepareStatement(sqlQuery.sql())) {
                for (int i = 0; i < sqlQuery.parameters().size(); i++)
                    ps.setObject(i + 1, sqlQuery.parameters().get(i));
                try (ResultSet rs = ps.executeQuery()) {
                    if (query.isAggregation()) {
                        // Q1-Q4: build JSON object from result columns (camelCase aliases match MongoDB)
                        while (rs.next()) {
                            var obj = mapper.createObjectNode();
                            var meta = rs.getMetaData();
                            for (int c = 1; c <= meta.getColumnCount(); c++) {
                                String colName = meta.getColumnLabel(c);
                                int type = meta.getColumnType(c);
                                if (type == java.sql.Types.NUMERIC || type == java.sql.Types.DOUBLE
                                        || type == java.sql.Types.FLOAT || type == java.sql.Types.DECIMAL
                                        || type == java.sql.Types.BIGINT || type == java.sql.Types.INTEGER) {
                                    double val = rs.getDouble(c);
                                    if (!rs.wasNull()) {
                                        if (val == Math.floor(val) && val < Long.MAX_VALUE)
                                            obj.put(colName, (long) val);
                                        else
                                            obj.put(colName, val);
                                    }
                                } else {
                                    String val = rs.getString(c);
                                    if (val != null) obj.put(colName, val);
                                    else obj.put(colName, "");
                                }
                            }
                            jsonDocs.add(mapper.writeValueAsString(obj));
                        }
                    } else {
                        // Q5-Q9: first column is full JSON document
                        while (rs.next()) {
                            String json = rs.getString(1);
                            if (json != null) {
                                JsonNode node = mapper.readTree(json);
                                jsonDocs.add(mapper.writeValueAsString(node));
                            }
                        }
                    }
                }
            }
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf(" %d docs, %d ms%n", jsonDocs.size(), elapsedMs);
            return new TargetResult(name, jsonDocs, jsonDocs.size(), elapsedMs, null);
        } catch (Exception e) {
            System.out.printf(" ERROR: %s%n", e.getMessage());
            return new TargetResult(name, List.of(), 0, 0, e.getMessage());
        }
    }

    // --- HTML Generation ---

    private static void generateHtml(List<QueryResult> results, String outputPath) throws Exception {
        try (PrintWriter out = new PrintWriter(new FileWriter(outputPath))) {
            out.println("<!DOCTYPE html>");
            out.println("<html lang=\"en\">");
            out.println("<head>");
            out.println("<meta charset=\"UTF-8\">");
            out.println("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">");
            out.println("<title>Helix Benchmark - Query Results</title>");
            out.println("<style>");
            out.println(CSS);
            out.println("</style>");
            out.println("</head>");
            out.println("<body>");

            out.println("<div class=\"header\">");
            out.println("  <h1>Helix Benchmark &mdash; Query Results</h1>");
            out.printf("  <p class=\"subtitle\">Generated %s &bull; %d queries &bull; 4 database targets</p>%n",
                    java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")),
                    results.size());
            out.println("</div>");

            // Query tabs
            out.println("<div class=\"tab-bar\">");
            for (int i = 0; i < results.size(); i++) {
                QueryResult qr = results.get(i);
                out.printf("  <button class=\"tab-btn%s\" onclick=\"showQuery(%d)\">%s</button>%n",
                        i == 0 ? " active" : "", i, qr.query.queryName());
            }
            out.println("</div>");

            for (int i = 0; i < results.size(); i++) {
                QueryResult qr = results.get(i);
                out.printf("<div class=\"query-panel\" id=\"qpanel-%d\" style=\"%s\">%n",
                        i, i == 0 ? "" : "display:none");

                // Header
                out.printf("  <div class=\"query-header\">%n");
                out.printf("    <h2>%s: %s</h2>%n", qr.query.queryName(), escHtml(qr.query.description()));
                out.printf("    <div class=\"query-meta\">%n");
                out.printf("      <span class=\"badge\">%s</span>%n", qr.query.isAggregation() ? "Aggregation" : "Find");
                out.printf("      <span class=\"badge\">Collection: %s</span>%n", qr.query.embeddedCollection());
                out.printf("    </div>%n");
                out.printf("  </div>%n");

                // Parameters
                out.printf("  <div class=\"params-box\">%n");
                out.printf("    <h3>Parameters</h3>%n");
                out.printf("    <table class=\"params-table\">%n");
                for (var entry : qr.params.entrySet()) {
                    String val = entry.getValue() instanceof Double d ? String.format("%,.2f", d)
                            : String.valueOf(entry.getValue());
                    out.printf("      <tr><td class=\"param-name\">%s</td><td class=\"param-value\">%s</td></tr>%n",
                            escHtml(entry.getKey()), escHtml(val));
                }
                out.printf("    </table>%n");
                out.printf("  </div>%n");

                // Query implementations sub-tabs
                out.printf("  <div class=\"sql-section\">%n");
                out.printf("    <h3>Query Implementations</h3>%n");
                out.printf("    <div class=\"sql-tab-bar\" id=\"sqltabs-%d\">%n", i);
                int t = 0;
                for (var entry : qr.queryTexts.entrySet()) {
                    out.printf("      <button class=\"sql-tab-btn%s\" onclick=\"showSubTab('sql',%d,%d,%d)\">%s</button>%n",
                            t == 0 ? " active" : "", i, t, qr.queryTexts.size(), escHtml(entry.getKey()));
                    t++;
                }
                out.printf("    </div>%n");
                t = 0;
                for (var entry : qr.queryTexts.entrySet()) {
                    out.printf("    <div class=\"sql-panel\" id=\"sql-%d-%d\"%s><pre class=\"code\">%s</pre></div>%n",
                            i, t, t == 0 ? "" : " style=\"display:none\"", escHtml(entry.getValue()));
                    t++;
                }
                out.printf("  </div>%n");

                // Result sets per target — sub-tabs
                out.printf("  <div class=\"results-section\">%n");
                out.printf("    <h3>Result Sets</h3>%n");

                // Target result tabs
                out.printf("    <div class=\"target-tab-bar\" id=\"restabs-%d\">%n", i);
                for (int ti = 0; ti < qr.targetResults.size(); ti++) {
                    TargetResult tr = qr.targetResults.get(ti);
                    String countBadge = tr.error != null ? "ERR" : String.valueOf(tr.count);
                    out.printf("      <button class=\"target-tab-btn%s\" onclick=\"showSubTab('res',%d,%d,%d)\">%s <span class=\"count-badge\">%s</span></button>%n",
                            ti == 0 ? " active" : "", i, ti, qr.targetResults.size(),
                            escHtml(tr.targetName), countBadge);
                    ti = ti; // no-op to keep index
                }
                out.printf("    </div>%n");

                // Target result panels
                for (int ti = 0; ti < qr.targetResults.size(); ti++) {
                    TargetResult tr = qr.targetResults.get(ti);
                    out.printf("    <div class=\"res-panel\" id=\"res-%d-%d\"%s>%n",
                            i, ti, ti == 0 ? "" : " style=\"display:none\"");

                    // Timing badge
                    out.printf("      <div class=\"target-meta\">");
                    if (tr.error != null) {
                        out.printf("<span class=\"badge err\">Error: %s</span>", escHtml(tr.error));
                    } else {
                        out.printf("<span class=\"badge\">%d documents</span> <span class=\"badge\">%d ms</span>",
                                tr.count, tr.elapsedMs);
                    }
                    out.printf("</div>%n");

                    // Documents
                    for (int d = 0; d < tr.docs.size(); d++) {
                        String docJson = tr.docs.get(d);
                        String docId = "Document " + (d + 1);
                        String summary = "";
                        try {
                            JsonNode node = mapper.readTree(docJson);
                            if (node.has("_id")) docId = node.get("_id").asText();
                            if (qr.query.isAggregation()) {
                                StringBuilder sb = new StringBuilder();
                                if (node.has("advisorId")) {
                                    sb.append("advisor=").append(node.get("advisorId").asText());
                                }
                                JsonNode mvNode = node.has("viewableMarketValue") ? node.get("viewableMarketValue") : null;
                                if (mvNode != null) {
                                    if (!sb.isEmpty()) sb.append(", ");
                                    sb.append("mv=").append(String.format("%,.2f", mvNode.asDouble()));
                                }
                                summary = sb.toString();
                            }
                        } catch (Exception ignored) {}
                        out.printf("      <details%s>%n", d == 0 ? " open" : "");
                        if (!summary.isEmpty()) {
                            out.printf("        <summary><span class=\"doc-num\">#%d</span> <span class=\"mono\">%s</span> <span class=\"doc-summary\">%s</span></summary>%n",
                                    d + 1, escHtml(docId), escHtml(summary));
                        } else {
                            out.printf("        <summary><span class=\"doc-num\">#%d</span> <span class=\"mono\">%s</span></summary>%n",
                                    d + 1, escHtml(docId));
                        }
                        out.printf("        <pre class=\"code json\">%s</pre>%n", escHtml(docJson));
                        out.printf("      </details>%n");
                    }
                    out.printf("    </div>%n");
                }
                out.printf("  </div>%n");
                out.printf("</div>%n");
            }

            out.println("<script>");
            out.println(JS);
            out.println("</script>");
            out.println("</body>");
            out.println("</html>");
        }
    }

    private static String escHtml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    private static final String CSS = """
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               background: #0f172a; color: #e2e8f0; line-height: 1.6; }
        .header { background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
                  padding: 2rem 2rem 1.5rem; border-bottom: 1px solid #334155; }
        .header h1 { font-size: 1.75rem; font-weight: 700; color: #f1f5f9; }
        .subtitle { color: #94a3b8; margin-top: 0.25rem; font-size: 0.9rem; }

        .tab-bar { display: flex; gap: 2px; padding: 0 2rem; background: #1e293b;
                   border-bottom: 2px solid #334155; overflow-x: auto; }
        .tab-btn { padding: 0.75rem 1.25rem; background: transparent; border: none;
                   color: #94a3b8; font-size: 0.95rem; font-weight: 600; cursor: pointer;
                   border-bottom: 3px solid transparent; transition: all 0.2s; white-space: nowrap; }
        .tab-btn:hover { color: #e2e8f0; }
        .tab-btn.active { color: #38bdf8; border-bottom-color: #38bdf8; }

        .query-panel { padding: 2rem; max-width: 1400px; margin: 0 auto; }
        .query-header { margin-bottom: 1.5rem; }
        .query-header h2 { font-size: 1.35rem; color: #f1f5f9; margin-bottom: 0.5rem; }
        .query-meta { display: flex; gap: 0.5rem; flex-wrap: wrap; }
        .badge { background: #334155; color: #94a3b8; padding: 0.2rem 0.75rem;
                 border-radius: 9999px; font-size: 0.8rem; font-weight: 500; }
        .badge.err { background: #7f1d1d; color: #fca5a5; }

        .params-box { background: #1e293b; border: 1px solid #334155; border-radius: 8px;
                      padding: 1rem 1.25rem; margin-bottom: 1.5rem; }
        .params-box h3, .sql-section h3, .results-section h3 {
            font-size: 0.9rem; color: #64748b; text-transform: uppercase;
            letter-spacing: 0.05em; margin-bottom: 0.75rem; }
        .params-table { width: auto; }
        .params-table td { padding: 0.25rem 0; }
        .param-name { color: #38bdf8; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.85rem; padding-right: 1.5rem; }
        .param-value { color: #fbbf24; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.85rem; }

        .sql-section { margin-bottom: 1.5rem; }
        .sql-tab-bar, .target-tab-bar { display: flex; gap: 2px; margin-bottom: 0; flex-wrap: wrap; }
        .sql-tab-btn, .target-tab-btn {
            padding: 0.5rem 1rem; background: #1e293b; border: 1px solid #334155;
            border-bottom: none; color: #94a3b8; font-size: 0.8rem; font-weight: 500;
            cursor: pointer; border-radius: 6px 6px 0 0; transition: all 0.2s; white-space: nowrap; }
        .sql-tab-btn:hover, .target-tab-btn:hover { color: #e2e8f0; }
        .sql-tab-btn.active, .target-tab-btn.active {
            background: #1e293b; color: #38bdf8; border-color: #38bdf8;
            border-bottom: 1px solid #1e293b; position: relative; z-index: 1; }
        .count-badge { background: #475569; color: #e2e8f0; padding: 0.1rem 0.4rem;
                       border-radius: 4px; font-size: 0.7rem; margin-left: 0.25rem; }

        .sql-panel, .res-panel { background: #1e293b; border: 1px solid #334155;
                                  border-radius: 0 8px 8px 8px; margin-top: -1px; }
        .res-panel { padding: 1rem; }
        .target-meta { margin-bottom: 0.75rem; display: flex; gap: 0.5rem; }
        pre.code { padding: 1.25rem; overflow-x: auto; font-family: 'SF Mono', 'Fira Code', monospace;
                   font-size: 0.8rem; line-height: 1.5; color: #e2e8f0; white-space: pre-wrap; word-break: break-word; }

        details { background: #0f172a; border: 1px solid #334155; border-radius: 8px; margin-bottom: 0.5rem; }
        details[open] { border-color: #475569; }
        summary { padding: 0.75rem 1rem; cursor: pointer; font-size: 0.9rem; color: #94a3b8; user-select: none; }
        summary:hover { color: #e2e8f0; }
        summary .mono { color: #38bdf8; }
        summary .doc-num { color: #64748b; font-weight: 600; margin-right: 0.25rem; }
        summary .doc-summary { color: #94a3b8; font-size: 0.8rem; margin-left: 0.75rem; }
        details pre.code { border-top: 1px solid #334155; margin: 0; border-radius: 0 0 8px 8px;
                           max-height: 500px; overflow-y: auto; }
        """;

    private static final String JS = """
        function showQuery(idx) {
            document.querySelectorAll('.query-panel').forEach(p => p.style.display = 'none');
            document.querySelectorAll('.tab-bar .tab-btn').forEach(b => b.classList.remove('active'));
            document.getElementById('qpanel-' + idx).style.display = 'block';
            document.querySelectorAll('.tab-bar .tab-btn')[idx].classList.add('active');
        }
        function showSubTab(prefix, queryIdx, tabIdx, count) {
            for (let i = 0; i < count; i++) {
                const panel = document.getElementById(prefix + '-' + queryIdx + '-' + i);
                if (panel) panel.style.display = i === tabIdx ? 'block' : 'none';
            }
            const barId = prefix === 'sql' ? 'sqltabs-' + queryIdx : 'restabs-' + queryIdx;
            const bar = document.getElementById(barId);
            if (bar) {
                const btnClass = prefix === 'sql' ? 'sql-tab-btn' : 'target-tab-btn';
                bar.querySelectorAll('.' + btnClass).forEach((b, i) => {
                    b.classList.toggle('active', i === tabIdx);
                });
            }
        }
        """;

    private static HikariDataSource createDs(ConnectionManager connMgr) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(connMgr.getJdbcUrl());
        hc.setUsername(connMgr.getJdbcUsername());
        hc.setPassword(connMgr.getJdbcPassword());
        hc.setMaximumPoolSize(2);
        hc.setAutoCommit(false);
        hc.addDataSourceProperty("oracle.jdbc.J2EE13Compliant", "true");
        String ts = findTruststore();
        if (ts != null) {
            hc.addDataSourceProperty("javax.net.ssl.trustStore", ts);
            hc.addDataSourceProperty("javax.net.ssl.trustStorePassword", connMgr.getJdbcPassword());
            hc.addDataSourceProperty("javax.net.ssl.trustStoreType", "JKS");
            hc.addDataSourceProperty("oracle.net.ssl_server_dn_match", "false");
        }
        return new HikariDataSource(hc);
    }

    private static String findTruststore() {
        for (String p : new String[]{"docker/truststore.jks", "../docker/truststore.jks", "truststore.jks"})
            if (new java.io.File(p).exists()) return p;
        return null;
    }

    private static Map<QueryDefinition, Map<String, Object>> buildTargetedParams(
            String mongoConnStr, String mongoDbName, QueryParameterGenerator paramGen) {
        Map<QueryDefinition, Map<String, Object>> result = new EnumMap<>(QueryDefinition.class);
        try (MongoClient client = MongoClients.create(mongoConnStr)) {
            MongoDatabase db = client.getDatabase(mongoDbName);

            result.put(QueryDefinition.Q1, paramGen.generate(QueryDefinition.Q1));

            Document bri = db.getCollection("bookRoleInvestor").find(
                    new Document("investorType", "Client").append("viewableFlag", "Y")).first();
            if (bri != null) {
                Map<String, Object> q2 = new LinkedHashMap<>();
                q2.put("advisorId", bri.getList("advisors", Document.class).get(0).getString("advisorId"));
                q2.put("partyRoleId", bri.get("partyRoleId"));
                q2.put("searchTerm", bri.getString("investorFullName").split(" ")[0].toLowerCase());
                result.put(QueryDefinition.Q2, q2);
            } else result.put(QueryDefinition.Q2, paramGen.generate(QueryDefinition.Q2));

            Document bri3 = db.getCollection("bookRoleInvestor").find(
                    new Document("investorType", "Client").append("viewableSource", "Y")
                            .append("entitlements.advisoryContext.0", new Document("$exists", true))).first();
            if (bri3 != null) {
                Map<String, Object> q3 = new LinkedHashMap<>();
                q3.put("advisorId", bri3.getList("advisors", Document.class).get(0).getString("advisorId"));
                q3.put("advisoryContext", ((Document) bri3.get("entitlements")).getList("advisoryContext", String.class).get(0));
                q3.put("dataOwnerPartyRoleId", ((Document)((Document) bri3.get("entitlements")).get("pxClient")).get("dataOwnerPartyRoleId"));
                result.put(QueryDefinition.Q3, q3);
            } else result.put(QueryDefinition.Q3, paramGen.generate(QueryDefinition.Q3));

            Map<String, Object> q4 = new LinkedHashMap<>();
            q4.put("advisorId", result.get(QueryDefinition.Q1).get("advisorId"));
            q4.put("minMarketValue", 10000.0);
            q4.put("maxMarketValue", 50000000.0);
            result.put(QueryDefinition.Q4, q4);

            Document brg = db.getCollection("bookRoleGroup").find(
                    new Document("visibleFlag", new Document("$ne", "N"))
                            .append("entitlements.advisoryContext.0", new Document("$exists", true))
                            .append("personaNm.0", new Document("$exists", true))).first();
            if (brg != null) {
                Map<String, Object> q5 = new LinkedHashMap<>();
                q5.put("dataOwnerPartyRoleId", brg.get("dataOwnerPartyRoleId"));
                q5.put("advisoryContext", brg.get("entitlements", Document.class).getList("advisoryContext", String.class).get(0));
                q5.put("personaNm", brg.getList("personaNm", String.class).get(0));
                double mv = brg.getDouble("totalViewableAccountsMarketValue");
                q5.put("minMarketValue", mv - 1.0); q5.put("maxMarketValue", mv + 1.0);
                result.put(QueryDefinition.Q5, q5);
            } else result.put(QueryDefinition.Q5, paramGen.generate(QueryDefinition.Q5));

            Document brg6 = db.getCollection("bookRoleGroup").find(
                    new Document("visibleFlag", new Document("$ne", "N"))
                            .append("entitlements.advisoryContext.0", new Document("$exists", true))
                            .append("entitlements.pxPartyRoleIdList.0", new Document("$exists", true))).first();
            if (brg6 != null) {
                Map<String, Object> q6 = new LinkedHashMap<>();
                Document ent = brg6.get("entitlements", Document.class);
                q6.put("advisoryContext", ent.getList("advisoryContext", String.class).get(0));
                q6.put("pxPartyRoleId", ent.getList("pxPartyRoleIdList", Number.class).get(0).longValue());
                double mv = brg6.getDouble("totalViewableAccountsMarketValue");
                q6.put("minMarketValue", mv - 1.0); q6.put("maxMarketValue", mv + 1.0);
                result.put(QueryDefinition.Q6, q6);
            } else result.put(QueryDefinition.Q6, paramGen.generate(QueryDefinition.Q6));

            Document acct = db.getCollection("account").find(
                    new Document("viewableSource", "Y").append("holdings.0", new Document("$exists", true))
                            .append("entitlements.pxPartyRoleIdList.0", new Document("$exists", true))).first();
            if (acct != null) {
                Map<String, Object> q7 = new LinkedHashMap<>();
                q7.put("pxPartyRoleId", acct.get("entitlements", Document.class).getList("pxPartyRoleIdList", Number.class).get(0).longValue());
                q7.put("fundTicker", acct.getList("holdings", Document.class).get(0).getString("fundTicker"));
                result.put(QueryDefinition.Q7, q7);
            } else result.put(QueryDefinition.Q7, paramGen.generate(QueryDefinition.Q7));

            Document adv = db.getCollection("advisor").find(
                    new Document("advisorHierarchy.0", new Document("$exists", true))
                            .append("entitlements.pxClient.dataOwnerPartyRoleId", new Document("$exists", true))).first();
            if (adv != null) {
                Map<String, Object> q8 = new LinkedHashMap<>();
                q8.put("dataOwnerPartyRoleId", ((Document) adv.get("entitlements")).get("pxClient", Document.class).get("dataOwnerPartyRoleId"));
                q8.put("partyNodePathValue", adv.getList("advisorHierarchy", Document.class).get(0).getString("partyNodePathValue"));
                result.put(QueryDefinition.Q8, q8);
            } else result.put(QueryDefinition.Q8, paramGen.generate(QueryDefinition.Q8));

            Document adv9 = db.getCollection("advisor").find(
                    new Document("entitlements.pxPartyRoleIdList.0", new Document("$exists", true))).first();
            if (adv9 != null) {
                Map<String, Object> q9 = new LinkedHashMap<>();
                q9.put("pxPartyRoleId", adv9.get("entitlements", Document.class).getList("pxPartyRoleIdList", Number.class).get(0).longValue());
                double mv = adv9.getDouble("accountViewableMarketValue");
                q9.put("minMarketValue", mv - 1.0); q9.put("maxMarketValue", mv + 1.0);
                result.put(QueryDefinition.Q9, q9);
            } else result.put(QueryDefinition.Q9, paramGen.generate(QueryDefinition.Q9));
        }
        return result;
    }
}
