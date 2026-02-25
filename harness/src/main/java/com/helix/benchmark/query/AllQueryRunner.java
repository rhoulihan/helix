package com.helix.benchmark.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;

/**
 * Runs all 9 queries on all 6 database targets, displaying actual results for comparison.
 */
public class AllQueryRunner {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String LINE = "═══════════════════════════════════════════════════════════════════════════════════";
    private static final String DASH = "───────────────────────────────────────────────────────────────────────────────────";

    public static void main(String[] args) throws Exception {
        // Set truststore for TLS
        for (String p : new String[]{"docker/truststore.jks", "../docker/truststore.jks", "truststore.jks"}) {
            if (new java.io.File(p).exists()) {
                String abs = new java.io.File(p).getAbsolutePath();
                System.setProperty("javax.net.ssl.trustStore", abs);
                System.setProperty("javax.net.ssl.trustStoreType", "JKS");
                break;
            }
        }

        BenchmarkConfig config = HelixBenchmarkMain.loadConfig(null);
        ConnectionManager connMgr = new ConnectionManager(config);

        ReferenceRegistry registry = new ReferenceRegistry(
                config.advisoryContextPoolSize(), config.partyRoleIdPoolSize(), config.finInstIdPoolSize());
        populateRegistry(connMgr, registry);
        QueryParameterGenerator paramGen = new QueryParameterGenerator(registry);

        MongoQueryExecutor mongoExec = new MongoQueryExecutor();
        OracleJdbcQueryExecutor oracleJdbcExec = new OracleJdbcQueryExecutor();
        OracleRelationalQueryExecutor relExec = new OracleRelationalQueryExecutor();
        OracleDualityViewQueryExecutor dvExec = new OracleDualityViewQueryExecutor();

        String mongoConnStr = connMgr.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
        String mongoDbName = connMgr.getDatabaseName(DatabaseTarget.MONGO_NATIVE);

        // Pre-sample parameters from actual data
        try (MongoClient initClient = MongoClients.create(mongoConnStr)) {
            paramGen.initFromData(initClient.getDatabase(mongoDbName), 50);
        }
        HikariDataSource ds = createDs(connMgr);

        // Build targeted params from actual data so queries return non-empty results
        Map<QueryDefinition, Map<String, Object>> targetedParams =
                buildTargetedParams(mongoConnStr, mongoDbName, paramGen);

        int totalMatch = 0, totalFail = 0;

        for (QueryDefinition query : QueryDefinition.values()) {
            Map<String, Object> params = targetedParams.get(query);

            System.out.println();
            System.out.println(LINE);
            System.out.printf("  %s: %s%n", query.queryName(), query.description());
            System.out.printf("  Parameters: %s%n", formatParams(params));
            System.out.println(LINE);

            if (query.isAggregation()) {
                boolean ok = runAggregation(query, params, mongoExec, oracleJdbcExec, relExec, dvExec,
                        mongoConnStr, mongoDbName, ds, connMgr);
                if (ok) totalMatch++; else totalFail++;
            } else {
                boolean ok = runFind(query, params, mongoExec, oracleJdbcExec, relExec, dvExec,
                        mongoConnStr, mongoDbName, ds, connMgr);
                if (ok) totalMatch++; else totalFail++;
            }
        }

        System.out.println();
        System.out.println(LINE);
        System.out.printf("  OVERALL: %d/%d queries matched across all 6 targets%n",
                totalMatch, totalMatch + totalFail);
        if (totalFail > 0) {
            System.out.printf("  WARNING: %d queries had mismatches!%n", totalFail);
        }
        System.out.println(LINE);

        ds.close();
    }

    // ===================== Q1-Q4: Aggregation queries =====================

    private static boolean runAggregation(QueryDefinition query, Map<String, Object> params,
                                           MongoQueryExecutor mongoExec,
                                           OracleJdbcQueryExecutor oracleJdbcExec,
                                           OracleRelationalQueryExecutor relExec,
                                           OracleDualityViewQueryExecutor dvExec,
                                           String mongoConnStr, String mongoDbName,
                                           HikariDataSource ds,
                                           ConnectionManager connMgr) throws Exception {
        Map<String, List<String[]>> results = new LinkedHashMap<>();
        long t0, elapsed;

        // --- MongoDB Native ---
        try (MongoClient client = MongoClients.create(mongoConnStr)) {
            MongoDatabase db = client.getDatabase(mongoDbName);
            MongoCollection<Document> col = db.getCollection(query.embeddedCollection());
            var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED,
                    params, DatabaseTarget.MONGO_NATIVE);
            t0 = System.nanoTime();
            List<Document> docs = col.aggregate(pipeline).into(new ArrayList<>());
            elapsed = System.nanoTime() - t0;
            List<String[]> rows = new ArrayList<>();
            for (Document doc : docs) {
                String aid = doc.getString("advisorId");
                Number mv = doc.get("viewableMarketValue") instanceof Number n ? n : 0.0;
                Number accts = doc.get("noOfViewableAccts") instanceof Number n ? n : 0;
                rows.add(new String[]{aid != null ? aid : "null", fmtMv(mv), fmtInt(accts)});
            }
            results.put("MongoDB Native", rows);
        }
        printTarget("MongoDB Native", elapsed);
        printAggHeader();
        for (int i = 0; i < results.get("MongoDB Native").size(); i++)
            printAggRow(i + 1, results.get("MongoDB Native").get(i));
        System.out.printf("  -> %d rows%n%n", results.get("MongoDB Native").size());

        // --- Oracle JDBC (SODA JSON) ---
        {
            OracleJdbcQueryExecutor.SqlQuery sqlQuery = oracleJdbcExec.buildSql(query, SchemaModel.EMBEDDED, params);
            List<String[]> rows = new ArrayList<>();
            try (Connection conn = ds.getConnection();
                 var ps = conn.prepareStatement(sqlQuery.sql())) {
                bindParams(ps, sqlQuery.parameters());
                t0 = System.nanoTime();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(new String[]{
                                rs.getString("advisorId"),
                                fmtMv(rs.getDouble("viewableMarketValue")),
                                fmtInt(rs.getInt("noOfViewableAccts"))
                        });
                    }
                }
                elapsed = System.nanoTime() - t0;
            }
            results.put("Oracle JDBC", rows);
            printTarget("Oracle JDBC", elapsed);
            printAggHeader();
            for (int i = 0; i < rows.size(); i++) printAggRow(i + 1, rows.get(i));
            System.out.printf("  -> %d rows%n%n", rows.size());
        }

        // --- Oracle MongoDB API (non-DV) ---
        {
            String apiConnStr = connMgr.getMongoConnectionString(DatabaseTarget.ORACLE_MONGO_API);
            String apiDbName = connMgr.getDatabaseName(DatabaseTarget.ORACLE_MONGO_API);
            try (MongoClient client = MongoClients.create(apiConnStr)) {
                MongoDatabase db = client.getDatabase(apiDbName);
                String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API);
                MongoCollection<Document> col = db.getCollection(collName);
                var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED,
                        params, DatabaseTarget.ORACLE_MONGO_API);
                t0 = System.nanoTime();
                List<Document> docs = col.aggregate(pipeline).into(new ArrayList<>());
                elapsed = System.nanoTime() - t0;
                List<String[]> rows = new ArrayList<>();
                for (Document doc : docs) {
                    String aid = doc.getString("advisorId");
                    Number mv = doc.get("viewableMarketValue") instanceof Number n ? n : 0.0;
                    Number accts = doc.get("noOfViewableAccts") instanceof Number n ? n : 0;
                    rows.add(new String[]{aid != null ? aid : "null", fmtMv(mv), fmtInt(accts)});
                }
                results.put("Oracle MongoDB API", rows);
            }
            printTarget("Oracle MongoDB API", elapsed);
            printAggHeader();
            for (int i = 0; i < results.get("Oracle MongoDB API").size(); i++)
                printAggRow(i + 1, results.get("Oracle MongoDB API").get(i));
            System.out.printf("  -> %d rows%n%n", results.get("Oracle MongoDB API").size());
        }

        // --- Oracle Relational ---
        {
            OracleJdbcQueryExecutor.SqlQuery relSql = relExec.buildSql(query, SchemaModel.EMBEDDED, params);
            List<String[]> rows = new ArrayList<>();
            try (Connection conn = ds.getConnection();
                 var ps = conn.prepareStatement(relSql.sql())) {
                bindParams(ps, relSql.parameters());
                t0 = System.nanoTime();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(new String[]{
                                rs.getString("advisorId"),
                                fmtMv(rs.getDouble("viewableMarketValue")),
                                fmtInt(rs.getInt("noOfViewableAccts"))
                        });
                    }
                }
                elapsed = System.nanoTime() - t0;
            }
            results.put("Oracle Relational", rows);
            printTarget("Oracle Relational", elapsed);
            printAggHeader();
            for (int i = 0; i < rows.size(); i++) printAggRow(i + 1, rows.get(i));
            System.out.printf("  -> %d rows%n%n", rows.size());
        }

        // --- Oracle Duality View ---
        {
            OracleJdbcQueryExecutor.SqlQuery dvSql = dvExec.buildSql(query, SchemaModel.EMBEDDED, params);
            List<String[]> rows = new ArrayList<>();
            try (Connection conn = ds.getConnection();
                 var ps = conn.prepareStatement(dvSql.sql())) {
                bindParams(ps, dvSql.parameters());
                t0 = System.nanoTime();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(new String[]{
                                rs.getString("advisorId"),
                                fmtMv(rs.getDouble("viewableMarketValue")),
                                fmtInt(rs.getInt("noOfViewableAccts"))
                        });
                    }
                }
                elapsed = System.nanoTime() - t0;
            }
            results.put("Oracle Duality View", rows);
            printTarget("Oracle Duality View", elapsed);
            printAggHeader();
            for (int i = 0; i < rows.size(); i++) printAggRow(i + 1, rows.get(i));
            System.out.printf("  -> %d rows%n%n", rows.size());
        }

        // --- Oracle Mongo API (DV) ---
        {
            String dvMongoConnStr = connMgr.getMongoConnectionString(DatabaseTarget.ORACLE_MONGO_API_DV);
            String dvMongoDbName = connMgr.getDatabaseName(DatabaseTarget.ORACLE_MONGO_API_DV);
            try (MongoClient client = MongoClients.create(dvMongoConnStr)) {
                MongoDatabase db = client.getDatabase(dvMongoDbName);
                String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API_DV);
                MongoCollection<Document> col = db.getCollection(collName);
                var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED,
                        params, DatabaseTarget.ORACLE_MONGO_API_DV);
                t0 = System.nanoTime();
                List<Document> docs = col.aggregate(pipeline).into(new ArrayList<>());
                elapsed = System.nanoTime() - t0;
                List<String[]> rows = new ArrayList<>();
                for (Document doc : docs) {
                    String aid = doc.getString("advisorId");
                    Number mv = doc.get("viewableMarketValue") instanceof Number n ? n : 0.0;
                    Number accts = doc.get("noOfViewableAccts") instanceof Number n ? n : 0;
                    rows.add(new String[]{aid != null ? aid : "null", fmtMv(mv), fmtInt(accts)});
                }
                results.put("Oracle Mongo API (DV)", rows);
            }
            printTarget("Oracle Mongo API (DV)", elapsed);
            printAggHeader();
            for (int i = 0; i < results.get("Oracle Mongo API (DV)").size(); i++)
                printAggRow(i + 1, results.get("Oracle Mongo API (DV)").get(i));
            System.out.printf("  -> %d rows%n%n", results.get("Oracle Mongo API (DV)").size());
        }

        // Compare
        return compareAgg(results);
    }

    private static boolean compareAgg(Map<String, List<String[]>> results) {
        var entries = new ArrayList<>(results.entrySet());
        List<String[]> baseline = entries.get(0).getValue();
        boolean ok = true;

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
            System.out.printf("  X Row count mismatch: %s%n", msg);
            ok = false;
        }

        // Row-by-row
        int minSize = entries.stream().mapToInt(e -> e.getValue().size()).min().orElse(0);
        int mismatches = 0;
        for (int i = 0; i < minSize; i++) {
            boolean rowOk = true;
            for (int j = 1; j < entries.size(); j++) {
                if (!entries.get(0).getValue().get(i)[0].equals(entries.get(j).getValue().get(i)[0])
                        || !entries.get(0).getValue().get(i)[1].equals(entries.get(j).getValue().get(i)[1])) {
                    rowOk = false;
                    break;
                }
            }
            if (!rowOk) {
                if (mismatches < 3) {
                    StringBuilder msg = new StringBuilder();
                    for (var entry : entries) {
                        String[] row = entry.getValue().get(i);
                        msg.append(entry.getKey()).append("=[").append(row[0]).append(",").append(row[1]).append("] ");
                    }
                    System.out.printf("  X Row %d: %s%n", i + 1, msg);
                }
                mismatches++;
                ok = false;
            }
        }
        if (ok) {
            System.out.printf("  MATCH — %d rows identical across all %d targets%n",
                    baseline.size(), entries.size());
        } else if (mismatches > 3) {
            System.out.printf("  ... and %d more mismatches%n", mismatches - 3);
        }
        return ok;
    }

    // ===================== Q5-Q9: Find queries =====================

    private static boolean runFind(QueryDefinition query, Map<String, Object> params,
                                    MongoQueryExecutor mongoExec,
                                    OracleJdbcQueryExecutor oracleJdbcExec,
                                    OracleRelationalQueryExecutor relExec,
                                    OracleDualityViewQueryExecutor dvExec,
                                    String mongoConnStr, String mongoDbName,
                                    HikariDataSource ds,
                                    ConnectionManager connMgr) throws Exception {
        Map<String, Set<String>> results = new LinkedHashMap<>();
        long t0, elapsed;

        // --- MongoDB Native ---
        try (MongoClient client = MongoClients.create(mongoConnStr)) {
            MongoDatabase db = client.getDatabase(mongoDbName);
            MongoCollection<Document> col = db.getCollection(query.embeddedCollection());
            var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params, DatabaseTarget.MONGO_NATIVE);
            t0 = System.nanoTime();
            List<Document> docs = col.find(filter).into(new ArrayList<>());
            elapsed = System.nanoTime() - t0;
            Set<String> ids = new TreeSet<>();
            for (Document doc : docs) ids.add(doc.getString("_id"));
            results.put("MongoDB Native", ids);
        }
        printTarget("MongoDB Native", elapsed);
        printIds("  IDs", results.get("MongoDB Native"), 10);
        System.out.printf("  -> %d documents%n%n", results.get("MongoDB Native").size());

        // --- Oracle JDBC (SODA JSON) ---
        {
            OracleJdbcQueryExecutor.SqlQuery sqlQuery = oracleJdbcExec.buildSql(query, SchemaModel.EMBEDDED, params);
            Set<String> ids = new TreeSet<>();
            try (Connection conn = ds.getConnection();
                 var ps = conn.prepareStatement(sqlQuery.sql())) {
                bindParams(ps, sqlQuery.parameters());
                t0 = System.nanoTime();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String json = rs.getString(1);
                        if (json != null) {
                            JsonNode node = mapper.readTree(json);
                            JsonNode idNode = node.get("_id");
                            if (idNode != null) ids.add(idNode.asText());
                        }
                    }
                }
                elapsed = System.nanoTime() - t0;
            }
            results.put("Oracle JDBC", ids);
            printTarget("Oracle JDBC", elapsed);
            printIds("  IDs", ids, 10);
            System.out.printf("  -> %d rows%n%n", ids.size());
        }

        // --- Oracle MongoDB API (non-DV) ---
        {
            String apiConnStr = connMgr.getMongoConnectionString(DatabaseTarget.ORACLE_MONGO_API);
            String apiDbName = connMgr.getDatabaseName(DatabaseTarget.ORACLE_MONGO_API);
            try (MongoClient client = MongoClients.create(apiConnStr)) {
                MongoDatabase db = client.getDatabase(apiDbName);
                String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API);
                MongoCollection<Document> col = db.getCollection(collName);
                var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params, DatabaseTarget.ORACLE_MONGO_API);
                t0 = System.nanoTime();
                List<Document> docs = col.find(filter).into(new ArrayList<>());
                elapsed = System.nanoTime() - t0;
                Set<String> ids = new TreeSet<>();
                for (Document doc : docs) ids.add(doc.getString("_id"));
                results.put("Oracle MongoDB API", ids);
            }
            printTarget("Oracle MongoDB API", elapsed);
            printIds("  IDs", results.get("Oracle MongoDB API"), 10);
            System.out.printf("  -> %d documents%n%n", results.get("Oracle MongoDB API").size());
        }

        // --- Oracle Relational ---
        {
            OracleJdbcQueryExecutor.SqlQuery relSql = relExec.buildSql(query, SchemaModel.EMBEDDED, params);
            Set<String> ids = new TreeSet<>();
            try (Connection conn = ds.getConnection();
                 var ps = conn.prepareStatement(relSql.sql())) {
                bindParams(ps, relSql.parameters());
                t0 = System.nanoTime();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String json = rs.getString(1);
                        if (json != null) {
                            JsonNode node = mapper.readTree(json);
                            JsonNode idNode = node.get("_id");
                            if (idNode != null) ids.add(idNode.asText());
                        }
                    }
                }
                elapsed = System.nanoTime() - t0;
            }
            results.put("Oracle Relational", ids);
            printTarget("Oracle Relational", elapsed);
            printIds("  IDs", ids, 10);
            System.out.printf("  -> %d rows%n%n", ids.size());
        }

        // --- Oracle Duality View ---
        {
            OracleJdbcQueryExecutor.SqlQuery dvSql = dvExec.buildSql(query, SchemaModel.EMBEDDED, params);
            Set<String> ids = new TreeSet<>();
            try (Connection conn = ds.getConnection();
                 var ps = conn.prepareStatement(dvSql.sql())) {
                bindParams(ps, dvSql.parameters());
                t0 = System.nanoTime();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String json = rs.getString(1);
                        if (json != null) {
                            JsonNode node = mapper.readTree(json);
                            JsonNode idNode = node.get("_id");
                            if (idNode != null) ids.add(idNode.asText());
                        }
                    }
                }
                elapsed = System.nanoTime() - t0;
            }
            results.put("Oracle Duality View", ids);
            printTarget("Oracle Duality View", elapsed);
            printIds("  IDs", ids, 10);
            System.out.printf("  -> %d documents%n%n", ids.size());
        }

        // --- Oracle Mongo API (DV) ---
        {
            String dvMongoConnStr = connMgr.getMongoConnectionString(DatabaseTarget.ORACLE_MONGO_API_DV);
            String dvMongoDbName = connMgr.getDatabaseName(DatabaseTarget.ORACLE_MONGO_API_DV);
            try (MongoClient client = MongoClients.create(dvMongoConnStr)) {
                MongoDatabase db = client.getDatabase(dvMongoDbName);
                String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API_DV);
                MongoCollection<Document> col = db.getCollection(collName);
                var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params, DatabaseTarget.ORACLE_MONGO_API_DV);
                t0 = System.nanoTime();
                List<Document> docs = col.find(filter).into(new ArrayList<>());
                elapsed = System.nanoTime() - t0;
                Set<String> ids = new TreeSet<>();
                for (Document doc : docs) ids.add(doc.getString("_id"));
                results.put("Oracle Mongo API (DV)", ids);
            }
            printTarget("Oracle Mongo API (DV)", elapsed);
            printIds("  IDs", results.get("Oracle Mongo API (DV)"), 10);
            System.out.printf("  -> %d documents%n%n", results.get("Oracle Mongo API (DV)").size());
        }

        // Compare
        return compareFind(results);
    }

    private static boolean compareFind(Map<String, Set<String>> results) {
        var entries = new ArrayList<>(results.entrySet());
        String baseName = entries.get(0).getKey();
        Set<String> baseline = entries.get(0).getValue();

        boolean ok = true;
        for (int i = 1; i < entries.size(); i++) {
            if (!baseline.equals(entries.get(i).getValue())) {
                ok = false;
                break;
            }
        }

        if (ok) {
            System.out.printf("  MATCH — %d IDs identical across all %d targets%n",
                    baseline.size(), entries.size());
        } else {
            StringBuilder msg = new StringBuilder();
            for (var entry : entries) {
                if (!msg.isEmpty()) msg.append(", ");
                msg.append(entry.getKey()).append("=").append(entry.getValue().size());
            }
            System.out.printf("  X MISMATCH — %s%n", msg);
            for (int i = 1; i < entries.size(); i++) {
                String tName = entries.get(i).getKey();
                Set<String> tIds = entries.get(i).getValue();
                Set<String> baseNotT = diff(baseline, tIds);
                Set<String> tNotBase = diff(tIds, baseline);
                if (!baseNotT.isEmpty())
                    System.out.printf("    In %s NOT %s (%d): %s%n", baseName, tName, baseNotT.size(), first(baseNotT, 3));
                if (!tNotBase.isEmpty())
                    System.out.printf("    In %s NOT %s (%d): %s%n", tName, baseName, tNotBase.size(), first(tNotBase, 3));
            }
        }
        return ok;
    }

    // ===================== Helpers =====================

    private static void bindParams(java.sql.PreparedStatement ps, List<Object> params) throws Exception {
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }
    }

    private static void printTarget(String name, long elapsedNanos) {
        System.out.printf("  > %s (%.2f ms)%n", name, elapsedNanos / 1_000_000.0);
    }

    private static void printAggHeader() {
        System.out.printf("    %-4s %-20s %15s %8s%n", "#", "Advisor ID", "Market Value", "Accts");
        System.out.printf("    %-4s %-20s %15s %8s%n", "--", "------------------", "-------------", "-----");
    }

    private static void printAggRow(int idx, String[] row) {
        System.out.printf("    %-4d %-20s %15s %8s%n", idx, row[0], row[1], row[2]);
    }

    private static void printIds(String label, Set<String> ids, int max) {
        if (ids.isEmpty()) {
            System.out.printf("%s: (none)%n", label);
            return;
        }
        List<String> list = new ArrayList<>(ids);
        int show = Math.min(list.size(), max);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < show; i++) {
            if (i > 0) sb.append(", ");
            sb.append(list.get(i));
        }
        if (list.size() > max) sb.append(", ... (").append(list.size() - max).append(" more)");
        System.out.printf("%s: [%s]%n", label, sb);
    }

    private static String fmtMv(Number n) {
        return n != null ? String.format("%,.2f", n.doubleValue()) : "0.00";
    }

    private static String fmtInt(Number n) {
        return n != null ? String.valueOf(n.intValue()) : "0";
    }

    private static String formatParams(Map<String, Object> params) {
        StringBuilder sb = new StringBuilder("{");
        int i = 0;
        for (var e : params.entrySet()) {
            if (i > 0) sb.append(", ");
            sb.append(e.getKey()).append("=");
            if (e.getValue() instanceof Double d) sb.append(String.format("%.2f", d));
            else sb.append(e.getValue());
            i++;
        }
        sb.append("}");
        return sb.toString();
    }

    private static Set<String> diff(Set<String> a, Set<String> b) {
        Set<String> result = new TreeSet<>(a);
        result.removeAll(b);
        return result;
    }

    private static String first(Set<String> set, int n) {
        return set.stream().limit(n).toList().toString();
    }

    /**
     * Queries MongoDB to find actual parameter values that return non-empty results.
     */
    private static Map<QueryDefinition, Map<String, Object>> buildTargetedParams(
            String mongoConnStr, String mongoDbName, QueryParameterGenerator paramGen) {

        Map<QueryDefinition, Map<String, Object>> result = new EnumMap<>(QueryDefinition.class);

        try (MongoClient client = MongoClients.create(mongoConnStr)) {
            MongoDatabase db = client.getDatabase(mongoDbName);

            // Q1: just needs an advisorId — always works with random
            result.put(QueryDefinition.Q1, paramGen.generate(QueryDefinition.Q1));

            // Q2: pick a real investor doc with a known advisor, name, partyRoleId
            Document bri = db.getCollection("bookRoleInvestor").find(
                    new Document("investorType", "Client")
                            .append("viewableFlag", "Y")).first();
            if (bri != null) {
                Map<String, Object> q2 = new HashMap<>();
                List<Document> advisors = bri.getList("advisors", Document.class);
                q2.put("advisorId", advisors.get(0).getString("advisorId"));
                q2.put("partyRoleId", bri.get("partyRoleId"));
                String name = bri.getString("investorFullName");
                // Use first word of name as search term
                q2.put("searchTerm", name.split(" ")[0].toLowerCase());
                result.put(QueryDefinition.Q2, q2);
            } else {
                result.put(QueryDefinition.Q2, paramGen.generate(QueryDefinition.Q2));
            }

            // Q3: pick a real investor with advisory context and entitlements
            Document bri3 = db.getCollection("bookRoleInvestor").find(
                    new Document("investorType", "Client")
                            .append("viewableSource", "Y")
                            .append("entitlements.advisoryContext.0", new Document("$exists", true))).first();
            if (bri3 != null) {
                Map<String, Object> q3 = new HashMap<>();
                List<Document> advs = bri3.getList("advisors", Document.class);
                q3.put("advisorId", advs.get(0).getString("advisorId"));
                List<String> contexts = ((Document) bri3.get("entitlements")).getList("advisoryContext", String.class);
                q3.put("advisoryContext", contexts.get(0));
                Document pxClient = ((Document) bri3.get("entitlements")).get("pxClient", Document.class);
                q3.put("dataOwnerPartyRoleId", pxClient.get("dataOwnerPartyRoleId"));
                result.put(QueryDefinition.Q3, q3);
            } else {
                result.put(QueryDefinition.Q3, paramGen.generate(QueryDefinition.Q3));
            }

            // Q4: use wide market value range with a known advisor
            {
                Map<String, Object> q4 = new HashMap<>();
                q4.put("advisorId", result.get(QueryDefinition.Q1).get("advisorId"));
                q4.put("minMarketValue", 10000.0);
                q4.put("maxMarketValue", 50000000.0);
                result.put(QueryDefinition.Q4, q4);
            }

            // Q5: bookRoleGroup — personaNm is top-level, dataOwnerPartyRoleId is top-level,
            //     advisoryContext is under entitlements, totalViewableAccountsMarketValue is top-level
            Document brg = db.getCollection("bookRoleGroup").find(
                    new Document("visibleFlag", new Document("$ne", "N"))
                            .append("entitlements.advisoryContext.0", new Document("$exists", true))
                            .append("personaNm.0", new Document("$exists", true))).first();
            if (brg != null) {
                Map<String, Object> q5 = new HashMap<>();
                q5.put("dataOwnerPartyRoleId", brg.get("dataOwnerPartyRoleId"));
                Document ent = brg.get("entitlements", Document.class);
                q5.put("advisoryContext", ent.getList("advisoryContext", String.class).get(0));
                q5.put("personaNm", brg.getList("personaNm", String.class).get(0));
                double mv = brg.getDouble("totalViewableAccountsMarketValue");
                q5.put("minMarketValue", mv - 1.0);
                q5.put("maxMarketValue", mv + 1.0);
                result.put(QueryDefinition.Q5, q5);
            } else {
                result.put(QueryDefinition.Q5, paramGen.generate(QueryDefinition.Q5));
            }

            // Q6: bookRoleGroup — entitlements.advisoryContext + entitlements.pxPartyRoleIdList
            Document brg6 = db.getCollection("bookRoleGroup").find(
                    new Document("visibleFlag", new Document("$ne", "N"))
                            .append("entitlements.advisoryContext.0", new Document("$exists", true))
                            .append("entitlements.pxPartyRoleIdList.0", new Document("$exists", true))).first();
            if (brg6 != null) {
                Map<String, Object> q6 = new HashMap<>();
                Document ent = brg6.get("entitlements", Document.class);
                q6.put("advisoryContext", ent.getList("advisoryContext", String.class).get(0));
                List<Number> prIds = ent.getList("pxPartyRoleIdList", Number.class);
                q6.put("pxPartyRoleId", prIds.get(0).longValue());
                double mv = brg6.getDouble("totalViewableAccountsMarketValue");
                q6.put("minMarketValue", mv - 1.0);
                q6.put("maxMarketValue", mv + 1.0);
                result.put(QueryDefinition.Q6, q6);
            } else {
                result.put(QueryDefinition.Q6, paramGen.generate(QueryDefinition.Q6));
            }

            // Q7: account — entitlements.pxPartyRoleIdList + holdings.fundTicker
            Document acct = db.getCollection("account").find(
                    new Document("viewableSource", "Y")
                            .append("holdings.0", new Document("$exists", true))
                            .append("entitlements.pxPartyRoleIdList.0", new Document("$exists", true))).first();
            if (acct != null) {
                Map<String, Object> q7 = new HashMap<>();
                Document ent = acct.get("entitlements", Document.class);
                List<Number> prIds = ent.getList("pxPartyRoleIdList", Number.class);
                q7.put("pxPartyRoleId", prIds.get(0).longValue());
                List<Document> holdings = acct.getList("holdings", Document.class);
                q7.put("fundTicker", holdings.get(0).getString("fundTicker"));
                result.put(QueryDefinition.Q7, q7);
            } else {
                result.put(QueryDefinition.Q7, paramGen.generate(QueryDefinition.Q7));
            }

            // Q8: advisor — entitlements.pxClient.dataOwnerPartyRoleId + advisorHierarchy.partyNodePathValue
            Document adv = db.getCollection("advisor").find(
                    new Document("advisorHierarchy.0", new Document("$exists", true))
                            .append("entitlements.pxClient.dataOwnerPartyRoleId", new Document("$exists", true))).first();
            if (adv != null) {
                Map<String, Object> q8 = new HashMap<>();
                Document ent = adv.get("entitlements", Document.class);
                Document pxClient = ent.get("pxClient", Document.class);
                q8.put("dataOwnerPartyRoleId", pxClient.get("dataOwnerPartyRoleId"));
                List<Document> hier = adv.getList("advisorHierarchy", Document.class);
                q8.put("partyNodePathValue", hier.get(0).getString("partyNodePathValue"));
                result.put(QueryDefinition.Q8, q8);
            } else {
                result.put(QueryDefinition.Q8, paramGen.generate(QueryDefinition.Q8));
            }

            // Q9: advisor — entitlements.pxPartyRoleIdList + accountViewableMarketValue
            Document adv9 = db.getCollection("advisor").find(
                    new Document("entitlements.pxPartyRoleIdList.0", new Document("$exists", true))).first();
            if (adv9 != null) {
                Map<String, Object> q9 = new HashMap<>();
                Document ent = adv9.get("entitlements", Document.class);
                List<Number> prIds = ent.getList("pxPartyRoleIdList", Number.class);
                q9.put("pxPartyRoleId", prIds.get(0).longValue());
                double mv = adv9.getDouble("accountViewableMarketValue");
                q9.put("minMarketValue", mv - 1.0);
                q9.put("maxMarketValue", mv + 1.0);
                result.put(QueryDefinition.Q9, q9);
            } else {
                result.put(QueryDefinition.Q9, paramGen.generate(QueryDefinition.Q9));
            }
        }

        return result;
    }

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
        for (String p : new String[]{"docker/truststore.jks", "../docker/truststore.jks", "truststore.jks"}) {
            if (new java.io.File(p).exists()) return p;
        }
        return null;
    }

    private static void populateRegistry(ConnectionManager connMgr, ReferenceRegistry registry) {
        String connStr = connMgr.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
        String dbName = connMgr.getDatabaseName(DatabaseTarget.MONGO_NATIVE);
        try (MongoClient client = MongoClients.create(connStr)) {
            MongoDatabase db = client.getDatabase(dbName);
            for (Document doc : db.getCollection("advisor").find().projection(new Document("_id", 1))) {
                registry.registerAdvisorId(doc.getString("_id"));
            }
            for (Document doc : db.getCollection("bookRoleInvestor").find()
                    .projection(new Document("investorId", 1)).limit(10000)) {
                String id = doc.getString("investorId");
                if (id != null) registry.registerInvestorId(id);
            }
        }
    }
}
