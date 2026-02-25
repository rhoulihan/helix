package com.helix.benchmark.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.helix.benchmark.config.DatabaseTarget;
import com.helix.benchmark.config.SchemaModel;
import com.helix.benchmark.connection.ConnectionManager;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.zaxxer.hikari.HikariDataSource;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;

public class ResultValidator {
    private static final Logger log = LoggerFactory.getLogger(ResultValidator.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static boolean validate(ConnectionManager connMgr, HikariDataSource jdbcDs,
                                    QueryParameterGenerator paramGen, int runsPerQuery) {
        log.info("=== Result Validation: Comparing all 6 targets ===");

        MongoQueryExecutor mongoExec = new MongoQueryExecutor();
        OracleJdbcQueryExecutor oracleJdbcExec = new OracleJdbcQueryExecutor();
        OracleRelationalQueryExecutor relExec = new OracleRelationalQueryExecutor();
        OracleDualityViewQueryExecutor dvExec = new OracleDualityViewQueryExecutor();

        int totalChecks = 0, passed = 0, failed = 0;

        for (QueryDefinition query : QueryDefinition.values()) {
            for (int run = 0; run < runsPerQuery; run++) {
                Map<String, Object> params = paramGen.generate(query);
                totalChecks++;
                try {
                    boolean ok;
                    if (query.isAggregation()) {
                        ok = validateAggregation(query, params, connMgr, jdbcDs,
                                mongoExec, oracleJdbcExec, relExec, dvExec, run);
                    } else {
                        ok = validateFind(query, params, connMgr, jdbcDs,
                                mongoExec, oracleJdbcExec, relExec, dvExec, run);
                    }
                    if (ok) passed++;
                    else failed++;
                } catch (Exception e) {
                    log.error("  FAIL {} run {}: {}", query.queryName(), run, e.getMessage());
                    failed++;
                }
            }
        }

        log.info("=== Validation complete: {}/{} passed, {} failed ===", passed, totalChecks, failed);
        return failed == 0;
    }

    // ---- Q1-Q4: Aggregation queries (return investor + advisor rows) ----

    private static boolean validateAggregation(QueryDefinition query, Map<String, Object> params,
                                                ConnectionManager connMgr, HikariDataSource jdbcDs,
                                                MongoQueryExecutor mongoExec,
                                                OracleJdbcQueryExecutor oracleJdbcExec,
                                                OracleRelationalQueryExecutor relExec,
                                                OracleDualityViewQueryExecutor dvExec,
                                                int run) throws Exception {
        Map<String, List<String[]>> results = new LinkedHashMap<>();

        // MongoDB Native
        results.put("Mongo", executeMongoAgg(connMgr, mongoExec, query, params,
                DatabaseTarget.MONGO_NATIVE));

        // Oracle JDBC (SODA JSON collections)
        results.put("JDBC", executeJdbcAgg(jdbcDs, oracleJdbcExec, query, params));

        // Oracle MongoDB API (non-DV — native collection names, native field paths)
        results.put("MongoAPI", executeMongoAgg(connMgr, mongoExec, query, params,
                DatabaseTarget.ORACLE_MONGO_API));

        // Oracle Relational
        results.put("Rel", executeJdbcAgg(jdbcDs, relExec, query, params));

        // Oracle Duality View
        results.put("DV", executeJdbcAgg(jdbcDs, dvExec, query, params));

        // Oracle Mongo API (DV)
        results.put("DV-Mongo", executeMongoAgg(connMgr, mongoExec, query, params,
                DatabaseTarget.ORACLE_MONGO_API_DV));

        return compareAggResults(query, run, params, results);
    }

    private static List<String[]> executeMongoAgg(ConnectionManager connMgr, MongoQueryExecutor mongoExec,
                                                    QueryDefinition query, Map<String, Object> params,
                                                    DatabaseTarget target) {
        String connStr = connMgr.getMongoConnectionString(target);
        String dbName = connMgr.getDatabaseName(target);
        try (MongoClient client = MongoClients.create(connStr)) {
            MongoDatabase db = client.getDatabase(dbName);
            String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, target);
            MongoCollection<Document> col = db.getCollection(collName);
            var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED, params, target);
            List<Document> docs = col.aggregate(pipeline).into(new ArrayList<>());
            List<String[]> rows = new ArrayList<>();
            for (Document doc : docs) {
                rows.add(extractAggRow(doc));
            }
            return rows;
        }
    }

    private static String[] extractAggRow(Document doc) {
        // Flattened projection: advisorId, viewableMarketValue at top level
        String aid = doc.getString("advisorId");
        Number mv = doc.get("viewableMarketValue") instanceof Number n ? n : 0.0;
        String id = doc.getString("_id");
        return new String[]{aid != null ? aid : "null", fmt(mv), id != null ? id : "null"};
    }

    private static List<String[]> executeJdbcAgg(HikariDataSource ds,
                                                   OracleJdbcQueryExecutor exec,
                                                   QueryDefinition query,
                                                   Map<String, Object> params) throws Exception {
        OracleJdbcQueryExecutor.SqlQuery sqlQuery = exec.buildSql(query, SchemaModel.EMBEDDED, params);
        List<String[]> rows = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             var ps = conn.prepareStatement(sqlQuery.sql())) {
            for (int i = 0; i < sqlQuery.parameters().size(); i++) {
                ps.setObject(i + 1, sqlQuery.parameters().get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String aid = rs.getString("advisorId");
                    double mv = rs.getDouble("viewableMarketValue");
                    String id = rs.getString("_id");
                    rows.add(new String[]{aid, fmt(mv), id != null ? id : "null"});
                }
            }
        }
        return rows;
    }

    private static boolean compareAggResults(QueryDefinition query, int run,
                                              Map<String, Object> params,
                                              Map<String, List<String[]>> results) {
        boolean ok = true;
        String label = query.queryName() + " run " + run;

        var entries = new ArrayList<>(results.entrySet());
        List<String[]> baseline = entries.get(0).getValue();

        // Check sizes
        boolean sizeMismatch = false;
        for (int i = 1; i < entries.size(); i++) {
            if (entries.get(i).getValue().size() != baseline.size()) {
                sizeMismatch = true;
                break;
            }
        }
        if (sizeMismatch) {
            StringBuilder msg = new StringBuilder();
            for (var entry : entries) {
                if (!msg.isEmpty()) msg.append(", ");
                msg.append(entry.getKey()).append("=").append(entry.getValue().size());
            }
            log.warn("  MISMATCH {} counts: {} | params={}", label, msg, paramSummary(params));
            ok = false;
        }

        // Row-by-row comparison
        int minSize = entries.stream().mapToInt(e -> e.getValue().size()).min().orElse(0);
        int rowMismatches = 0;
        for (int i = 0; i < minSize; i++) {
            String[] baseRow = baseline.get(i);
            boolean rowOk = true;
            for (int j = 1; j < entries.size(); j++) {
                String[] otherRow = entries.get(j).getValue().get(i);
                if (!Objects.equals(baseRow[0], otherRow[0])
                        || !Objects.equals(baseRow[1], otherRow[1])
                        || !Objects.equals(baseRow[2], otherRow[2])) {
                    rowOk = false;
                    break;
                }
            }
            if (!rowOk) {
                if (rowMismatches < 3) {
                    StringBuilder msg = new StringBuilder();
                    for (var entry : entries) {
                        String[] row = entry.getValue().get(i);
                        msg.append(entry.getKey()).append("=[").append(row[2]).append(",")
                                .append(row[0]).append(",").append(row[1]).append("] ");
                    }
                    log.warn("  MISMATCH {} row {}: {}", label, i, msg);
                }
                rowMismatches++;
                ok = false;
            }
        }
        if (rowMismatches > 3) {
            log.warn("  ... and {} more row mismatches for {}", rowMismatches - 3, label);
        }

        if (ok) {
            log.info("  PASS {} — {} rows match | params={}", label, baseline.size(), paramSummary(params));
        }
        return ok;
    }

    // ---- Q5-Q9: Find queries (return full documents) ----

    private static boolean validateFind(QueryDefinition query, Map<String, Object> params,
                                         ConnectionManager connMgr, HikariDataSource jdbcDs,
                                         MongoQueryExecutor mongoExec,
                                         OracleJdbcQueryExecutor oracleJdbcExec,
                                         OracleRelationalQueryExecutor relExec,
                                         OracleDualityViewQueryExecutor dvExec,
                                         int run) throws Exception {
        Map<String, Set<String>> results = new LinkedHashMap<>();

        // MongoDB Native
        results.put("Mongo", executeMongoFindIds(connMgr, mongoExec, query, params,
                DatabaseTarget.MONGO_NATIVE));

        // Oracle JDBC (SODA JSON collections)
        results.put("JDBC", executeJdbcFindIds(jdbcDs, oracleJdbcExec, query, params));

        // Oracle MongoDB API (non-DV)
        results.put("MongoAPI", executeMongoFindIds(connMgr, mongoExec, query, params,
                DatabaseTarget.ORACLE_MONGO_API));

        // Oracle Relational
        results.put("Rel", executeJdbcFindIds(jdbcDs, relExec, query, params));

        // Oracle Duality View
        results.put("DV", executeJdbcFindIds(jdbcDs, dvExec, query, params));

        // Oracle Mongo API (DV)
        results.put("DV-Mongo", executeMongoFindIds(connMgr, mongoExec, query, params,
                DatabaseTarget.ORACLE_MONGO_API_DV));

        return compareFindResults(query, run, params, results);
    }

    private static Set<String> executeMongoFindIds(ConnectionManager connMgr, MongoQueryExecutor mongoExec,
                                                     QueryDefinition query, Map<String, Object> params,
                                                     DatabaseTarget target) {
        String connStr = connMgr.getMongoConnectionString(target);
        String dbName = connMgr.getDatabaseName(target);
        try (MongoClient client = MongoClients.create(connStr)) {
            MongoDatabase db = client.getDatabase(dbName);
            String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, target);
            MongoCollection<Document> col = db.getCollection(collName);
            var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params, target);
            List<Document> docs = col.find(filter).into(new ArrayList<>());
            Set<String> ids = new TreeSet<>();
            for (Document doc : docs) {
                ids.add(doc.getString("_id"));
            }
            return ids;
        }
    }

    private static Set<String> executeJdbcFindIds(HikariDataSource ds,
                                                    OracleJdbcQueryExecutor exec,
                                                    QueryDefinition query,
                                                    Map<String, Object> params) throws Exception {
        OracleJdbcQueryExecutor.SqlQuery sqlQuery = exec.buildSql(query, SchemaModel.EMBEDDED, params);
        Set<String> ids = new TreeSet<>();
        try (Connection conn = ds.getConnection();
             var ps = conn.prepareStatement(sqlQuery.sql())) {
            for (int i = 0; i < sqlQuery.parameters().size(); i++) {
                ps.setObject(i + 1, sqlQuery.parameters().get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String json = rs.getString(1);
                    if (json != null) {
                        try {
                            JsonNode node = mapper.readTree(json);
                            JsonNode idNode = node.get("_id");
                            if (idNode != null) {
                                ids.add(idNode.asText());
                            }
                        } catch (Exception e) {
                            log.debug("Could not parse JSON: {}", e.getMessage());
                        }
                    }
                }
            }
        }
        return ids;
    }

    private static boolean compareFindResults(QueryDefinition query, int run,
                                               Map<String, Object> params,
                                               Map<String, Set<String>> results) {
        boolean ok = true;
        String label = query.queryName() + " run " + run;

        var entries = new ArrayList<>(results.entrySet());
        String baseName = entries.get(0).getKey();
        Set<String> baseline = entries.get(0).getValue();

        // Check sizes
        boolean sizeMismatch = false;
        for (int i = 1; i < entries.size(); i++) {
            if (entries.get(i).getValue().size() != baseline.size()) {
                sizeMismatch = true;
                break;
            }
        }
        if (sizeMismatch) {
            StringBuilder msg = new StringBuilder();
            for (var entry : entries) {
                if (!msg.isEmpty()) msg.append(", ");
                msg.append(entry.getKey()).append("=").append(entry.getValue().size());
            }
            log.warn("  MISMATCH {} counts: {} | params={}", label, msg, paramSummary(params));
            ok = false;
        }

        // ID set differences vs baseline
        for (int i = 1; i < entries.size(); i++) {
            String targetName = entries.get(i).getKey();
            Set<String> targetIds = entries.get(i).getValue();

            Set<String> baseNotTarget = diff(baseline, targetIds);
            Set<String> targetNotBase = diff(targetIds, baseline);

            if (!baseNotTarget.isEmpty()) {
                log.warn("  {} in {} NOT {}: {} (first 3: {})", label, baseName, targetName,
                        baseNotTarget.size(), first3(baseNotTarget));
                ok = false;
            }
            if (!targetNotBase.isEmpty()) {
                log.warn("  {} in {} NOT {}: {} (first 3: {})", label, targetName, baseName,
                        targetNotBase.size(), first3(targetNotBase));
                ok = false;
            }
        }

        if (ok) {
            log.info("  PASS {} — {} IDs match | params={}", label, baseline.size(), paramSummary(params));
        }
        return ok;
    }

    // ---- Helpers ----

    private static String fmt(Number n) {
        return n != null ? String.format("%.2f", n.doubleValue()) : "0.00";
    }

    private static Set<String> diff(Set<String> a, Set<String> b) {
        Set<String> result = new TreeSet<>(a);
        result.removeAll(b);
        return result;
    }

    private static String first3(Set<String> set) {
        return set.stream().limit(3).toList().toString();
    }

    private static String paramSummary(Map<String, Object> params) {
        StringBuilder sb = new StringBuilder("{");
        int i = 0;
        for (var e : params.entrySet()) {
            if (i > 0) sb.append(", ");
            sb.append(e.getKey()).append("=");
            Object v = e.getValue();
            if (v instanceof Double d) {
                sb.append(String.format("%.2f", d));
            } else {
                sb.append(v);
            }
            i++;
        }
        sb.append("}");
        return sb.toString();
    }
}
