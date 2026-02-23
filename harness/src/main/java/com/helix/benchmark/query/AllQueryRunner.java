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
 * Runs all 9 queries on MongoDB, Oracle Relational, Oracle Duality View,
 * and Oracle Mongo API (DV), displaying actual results for comparison.
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
        OracleRelationalQueryExecutor relExec = new OracleRelationalQueryExecutor();
        OracleDualityViewQueryExecutor dvExec = new OracleDualityViewQueryExecutor();

        String mongoConnStr = connMgr.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
        String mongoDbName = connMgr.getDatabaseName(DatabaseTarget.MONGO_NATIVE);
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
                boolean ok = runAggregation(query, params, mongoExec, relExec, dvExec,
                        mongoConnStr, mongoDbName, ds, connMgr);
                if (ok) totalMatch++; else totalFail++;
            } else {
                boolean ok = runFind(query, params, mongoExec, relExec, dvExec,
                        mongoConnStr, mongoDbName, ds, connMgr);
                if (ok) totalMatch++; else totalFail++;
            }
        }

        System.out.println();
        System.out.println(LINE);
        System.out.printf("  OVERALL: %d/%d queries matched across all 4 targets%n",
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
                                           OracleRelationalQueryExecutor relExec,
                                           OracleDualityViewQueryExecutor dvExec,
                                           String mongoConnStr, String mongoDbName,
                                           HikariDataSource ds,
                                           ConnectionManager connMgr) throws Exception {
        // --- MongoDB ---
        List<String[]> mongoRows;
        long t0, elapsed;
        try (MongoClient client = MongoClients.create(mongoConnStr)) {
            MongoDatabase db = client.getDatabase(mongoDbName);
            MongoCollection<Document> col = db.getCollection(query.embeddedCollection());
            var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED,
                    params, DatabaseTarget.MONGO_NATIVE);
            t0 = System.nanoTime();
            List<Document> docs = col.aggregate(pipeline).into(new ArrayList<>());
            elapsed = System.nanoTime() - t0;
            mongoRows = new ArrayList<>();
            for (Document doc : docs) {
                String aid = doc.getString("advisorId");
                Number mv = doc.get("viewableMarketValue") instanceof Number n ? n : 0.0;
                Number accts = doc.get("noOfViewableAccts") instanceof Number n ? n : 0;
                mongoRows.add(new String[]{aid != null ? aid : "null", fmtMv(mv), fmtInt(accts)});
            }
        }
        printTarget("MongoDB Native", elapsed);
        printAggHeader();
        for (int i = 0; i < mongoRows.size(); i++) printAggRow(i + 1, mongoRows.get(i));
        System.out.printf("  → %d rows%n%n", mongoRows.size());

        // --- Oracle Relational ---
        OracleJdbcQueryExecutor.SqlQuery relSql = relExec.buildSql(query, SchemaModel.EMBEDDED, params);
        List<String[]> relRows = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             var ps = conn.prepareStatement(relSql.sql())) {
            bindParams(ps, relSql.parameters());
            t0 = System.nanoTime();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    relRows.add(new String[]{
                            rs.getString("advisor_id"),
                            fmtMv(rs.getDouble("viewable_mv")),
                            fmtInt(rs.getInt("no_of_viewable_accts"))
                    });
                }
            }
            elapsed = System.nanoTime() - t0;
        }
        printTarget("Oracle Relational", elapsed);
        printAggHeader();
        for (int i = 0; i < relRows.size(); i++) printAggRow(i + 1, relRows.get(i));
        System.out.printf("  → %d rows%n%n", relRows.size());

        // --- Oracle Duality View ---
        OracleJdbcQueryExecutor.SqlQuery dvSql = dvExec.buildSql(query, SchemaModel.EMBEDDED, params);
        List<String[]> dvRows = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             var ps = conn.prepareStatement(dvSql.sql())) {
            bindParams(ps, dvSql.parameters());
            t0 = System.nanoTime();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    dvRows.add(new String[]{
                            rs.getString("advisor_id"),
                            fmtMv(rs.getDouble("viewable_mv")),
                            fmtInt(rs.getInt("no_of_viewable_accts"))
                    });
                }
            }
            elapsed = System.nanoTime() - t0;
        }
        printTarget("Oracle Duality View", elapsed);
        printAggHeader();
        for (int i = 0; i < dvRows.size(); i++) printAggRow(i + 1, dvRows.get(i));
        System.out.printf("  → %d rows%n%n", dvRows.size());

        // --- Oracle Mongo API (DV) ---
        String dvMongoConnStr = connMgr.getMongoConnectionString(DatabaseTarget.ORACLE_MONGO_API_DV);
        String dvMongoDbName = connMgr.getDatabaseName(DatabaseTarget.ORACLE_MONGO_API_DV);
        List<String[]> dvMongoRows;
        try (MongoClient client = MongoClients.create(dvMongoConnStr)) {
            MongoDatabase db = client.getDatabase(dvMongoDbName);
            String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API_DV);
            MongoCollection<Document> col = db.getCollection(collName);
            var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED,
                    params, DatabaseTarget.ORACLE_MONGO_API_DV);
            t0 = System.nanoTime();
            List<Document> docs = col.aggregate(pipeline).into(new ArrayList<>());
            elapsed = System.nanoTime() - t0;
            dvMongoRows = new ArrayList<>();
            for (Document doc : docs) {
                String aid = doc.getString("advisorId");
                Number mv = doc.get("viewableMarketValue") instanceof Number n ? n : 0.0;
                Number accts = doc.get("noOfViewableAccts") instanceof Number n ? n : 0;
                dvMongoRows.add(new String[]{aid != null ? aid : "null", fmtMv(mv), fmtInt(accts)});
            }
        }
        printTarget("Oracle Mongo API (DV)", elapsed);
        printAggHeader();
        for (int i = 0; i < dvMongoRows.size(); i++) printAggRow(i + 1, dvMongoRows.get(i));
        System.out.printf("  → %d rows%n%n", dvMongoRows.size());

        // Compare
        return compareAgg(query, mongoRows, relRows, dvRows, dvMongoRows);
    }

    private static boolean compareAgg(QueryDefinition query, List<String[]> mongo,
                                       List<String[]> rel, List<String[]> dv,
                                       List<String[]> dvMongo) {
        boolean ok = true;
        if (mongo.size() != rel.size() || mongo.size() != dv.size() || mongo.size() != dvMongo.size()) {
            System.out.printf("  ✗ Row count mismatch: Mongo=%d, Rel=%d, DV=%d, DV-Mongo=%d%n",
                    mongo.size(), rel.size(), dv.size(), dvMongo.size());
            ok = false;
        }
        int max = Math.min(mongo.size(), Math.min(rel.size(), Math.min(dv.size(), dvMongo.size())));
        int mismatches = 0;
        for (int i = 0; i < max; i++) {
            if (!mongo.get(i)[0].equals(rel.get(i)[0]) || !mongo.get(i)[0].equals(dv.get(i)[0])
                    || !mongo.get(i)[0].equals(dvMongo.get(i)[0])
                    || !mongo.get(i)[1].equals(rel.get(i)[1]) || !mongo.get(i)[1].equals(dv.get(i)[1])
                    || !mongo.get(i)[1].equals(dvMongo.get(i)[1])) {
                if (mismatches < 3) {
                    System.out.printf("  ✗ Row %d: Mongo=[%s,%s] Rel=[%s,%s] DV=[%s,%s] DV-Mongo=[%s,%s]%n",
                            i + 1, mongo.get(i)[0], mongo.get(i)[1],
                            rel.get(i)[0], rel.get(i)[1], dv.get(i)[0], dv.get(i)[1],
                            dvMongo.get(i)[0], dvMongo.get(i)[1]);
                }
                mismatches++;
                ok = false;
            }
        }
        if (ok) {
            System.out.printf("  MATCH — %d rows identical across all 4 targets%n", mongo.size());
        } else if (mismatches > 3) {
            System.out.printf("  ... and %d more mismatches%n", mismatches - 3);
        }
        return ok;
    }

    // ===================== Q5-Q9: Find queries =====================

    private static boolean runFind(QueryDefinition query, Map<String, Object> params,
                                    MongoQueryExecutor mongoExec,
                                    OracleRelationalQueryExecutor relExec,
                                    OracleDualityViewQueryExecutor dvExec,
                                    String mongoConnStr, String mongoDbName,
                                    HikariDataSource ds,
                                    ConnectionManager connMgr) throws Exception {
        // --- MongoDB ---
        Set<String> mongoIds;
        long t0, elapsed;
        try (MongoClient client = MongoClients.create(mongoConnStr)) {
            MongoDatabase db = client.getDatabase(mongoDbName);
            MongoCollection<Document> col = db.getCollection(query.embeddedCollection());
            var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params);
            t0 = System.nanoTime();
            List<Document> docs = col.find(filter).into(new ArrayList<>());
            elapsed = System.nanoTime() - t0;
            mongoIds = new TreeSet<>();
            for (Document doc : docs) {
                mongoIds.add(doc.getString("_id"));
            }
        }
        printTarget("MongoDB Native", elapsed);
        printIds("  IDs", mongoIds, 10);
        System.out.printf("  → %d documents%n%n", mongoIds.size());

        // --- Oracle Relational ---
        OracleJdbcQueryExecutor.SqlQuery relSql = relExec.buildSql(query, SchemaModel.EMBEDDED, params);
        Set<String> relIds = new TreeSet<>();
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
                        if (idNode != null) relIds.add(idNode.asText());
                    }
                }
            }
            elapsed = System.nanoTime() - t0;
        }
        printTarget("Oracle Relational", elapsed);
        printIds("  IDs", relIds, 10);
        System.out.printf("  → %d rows%n%n", relIds.size());

        // --- Oracle Duality View ---
        OracleJdbcQueryExecutor.SqlQuery dvSql = dvExec.buildSql(query, SchemaModel.EMBEDDED, params);
        Set<String> dvIds = new TreeSet<>();
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
                        if (idNode != null) dvIds.add(idNode.asText());
                    }
                }
            }
            elapsed = System.nanoTime() - t0;
        }
        printTarget("Oracle Duality View", elapsed);
        printIds("  IDs", dvIds, 10);
        System.out.printf("  → %d documents%n%n", dvIds.size());

        // --- Oracle Mongo API (DV) ---
        String dvMongoConnStr = connMgr.getMongoConnectionString(DatabaseTarget.ORACLE_MONGO_API_DV);
        String dvMongoDbName = connMgr.getDatabaseName(DatabaseTarget.ORACLE_MONGO_API_DV);
        Set<String> dvMongoIds;
        try (MongoClient client = MongoClients.create(dvMongoConnStr)) {
            MongoDatabase db = client.getDatabase(dvMongoDbName);
            String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API_DV);
            MongoCollection<Document> col = db.getCollection(collName);
            var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params);
            t0 = System.nanoTime();
            List<Document> docs = col.find(filter).into(new ArrayList<>());
            elapsed = System.nanoTime() - t0;
            dvMongoIds = new TreeSet<>();
            for (Document doc : docs) {
                dvMongoIds.add(doc.getString("_id"));
            }
        }
        printTarget("Oracle Mongo API (DV)", elapsed);
        printIds("  IDs", dvMongoIds, 10);
        System.out.printf("  → %d documents%n%n", dvMongoIds.size());

        // Compare
        return compareFind(query, mongoIds, relIds, dvIds, dvMongoIds);
    }

    private static boolean compareFind(QueryDefinition query, Set<String> mongo,
                                        Set<String> rel, Set<String> dv, Set<String> dvMongo) {
        boolean ok = mongo.equals(rel) && mongo.equals(dv) && mongo.equals(dvMongo);
        if (ok) {
            System.out.printf("  MATCH — %d IDs identical across all 4 targets%n", mongo.size());
        } else {
            System.out.printf("  ✗ MISMATCH — Mongo=%d, Rel=%d, DV=%d, DV-Mongo=%d%n",
                    mongo.size(), rel.size(), dv.size(), dvMongo.size());
            Set<String> mongoNotRel = diff(mongo, rel);
            Set<String> relNotMongo = diff(rel, mongo);
            Set<String> mongoNotDv = diff(mongo, dv);
            Set<String> mongoNotDvMongo = diff(mongo, dvMongo);
            if (!mongoNotRel.isEmpty())
                System.out.printf("    In Mongo NOT Rel (%d): %s%n", mongoNotRel.size(), first(mongoNotRel, 3));
            if (!relNotMongo.isEmpty())
                System.out.printf("    In Rel NOT Mongo (%d): %s%n", relNotMongo.size(), first(relNotMongo, 3));
            if (!mongoNotDv.isEmpty())
                System.out.printf("    In Mongo NOT DV (%d): %s%n", mongoNotDv.size(), first(mongoNotDv, 3));
            if (!mongoNotDvMongo.isEmpty())
                System.out.printf("    In Mongo NOT DV-Mongo (%d): %s%n", mongoNotDvMongo.size(), first(mongoNotDvMongo, 3));
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
        System.out.printf("  ▸ %s (%.2f ms)%n", name, elapsedNanos / 1_000_000.0);
    }

    private static void printAggHeader() {
        System.out.printf("    %-4s %-20s %15s %8s%n", "#", "Advisor ID", "Market Value", "Accts");
        System.out.printf("    %-4s %-20s %15s %8s%n", "──", "──────────────────", "─────────────", "─────");
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
