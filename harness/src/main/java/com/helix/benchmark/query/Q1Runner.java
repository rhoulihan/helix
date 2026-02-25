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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Standalone runner that executes Q1 on all three targets and prints actual results side-by-side.
 */
public class Q1Runner {

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
        // Pre-sample parameters from actual data
        String initConnStr = connMgr.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
        String initDbName = connMgr.getDatabaseName(DatabaseTarget.MONGO_NATIVE);
        try (MongoClient initClient = MongoClients.create(initConnStr)) {
            paramGen.initFromData(initClient.getDatabase(initDbName), 50);
        }
        Map<String, Object> params = paramGen.generate(QueryDefinition.Q1);
        String advisorId = (String) params.get("advisorId");

        System.out.println("=============================================================");
        System.out.println("  Q1: Investor list by advisor — advisorId=" + advisorId);
        System.out.println("=============================================================");
        System.out.println();

        HikariDataSource ds = createDs(connMgr);

        // --- MongoDB Native ---
        System.out.println(">>> MongoDB Native");
        System.out.println("─────────────────────────────────────────────────────────────");
        MongoQueryExecutor mongoExec = new MongoQueryExecutor();
        String connStr = connMgr.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
        String dbName = connMgr.getDatabaseName(DatabaseTarget.MONGO_NATIVE);
        List<String[]> mongoRows;
        long t0, elapsed;
        try (MongoClient client = MongoClients.create(connStr)) {
            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> col = db.getCollection("bookRoleInvestor");
            var pipeline = mongoExec.buildAggregationPipeline(QueryDefinition.Q1, SchemaModel.EMBEDDED,
                    params, DatabaseTarget.MONGO_NATIVE);
            t0 = System.nanoTime();
            List<Document> docs = col.aggregate(pipeline).into(new ArrayList<>());
            elapsed = System.nanoTime() - t0;
            mongoRows = new ArrayList<>();
            for (Document doc : docs) {
                Document advisor = (Document) doc.get("advisor");
                String aid = advisor != null ? advisor.getString("advisorId") : "null";
                Number mv = advisor != null ? advisor.getDouble("viewableMarketValue") : 0.0;
                Number accts = advisor != null ? (Number) advisor.get("noOfViewableAccts") : 0;
                mongoRows.add(new String[]{aid, fmt(mv), fmtInt(accts)});
            }
        }
        printHeader();
        for (int i = 0; i < mongoRows.size(); i++) {
            printRow(i + 1, mongoRows.get(i));
        }
        System.out.printf("  Total: %d rows | Latency: %.2f ms%n%n", mongoRows.size(), elapsed / 1_000_000.0);

        // --- Oracle Relational ---
        System.out.println(">>> Oracle Relational (JOIN)");
        System.out.println("─────────────────────────────────────────────────────────────");
        OracleRelationalQueryExecutor relExec = new OracleRelationalQueryExecutor();
        OracleJdbcQueryExecutor.SqlQuery relSql = relExec.buildSql(QueryDefinition.Q1, SchemaModel.EMBEDDED, params);
        System.out.println("  SQL: " + relSql.sql().replaceAll("\\s+", " ").trim());
        System.out.println();
        List<String[]> relRows = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             var ps = conn.prepareStatement(relSql.sql())) {
            for (int i = 0; i < relSql.parameters().size(); i++) {
                ps.setObject(i + 1, relSql.parameters().get(i));
            }
            t0 = System.nanoTime();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    relRows.add(new String[]{
                            rs.getString("advisor_id"),
                            fmt(rs.getDouble("viewable_mv")),
                            fmtInt(rs.getInt("no_of_viewable_accts"))
                    });
                }
            }
            elapsed = System.nanoTime() - t0;
        }
        printHeader();
        for (int i = 0; i < relRows.size(); i++) {
            printRow(i + 1, relRows.get(i));
        }
        System.out.printf("  Total: %d rows | Latency: %.2f ms%n%n", relRows.size(), elapsed / 1_000_000.0);

        // --- Oracle Duality View ---
        System.out.println(">>> Oracle Duality View (JSON_TABLE over dv_book_role_investor)");
        System.out.println("─────────────────────────────────────────────────────────────");
        OracleDualityViewQueryExecutor dvExec = new OracleDualityViewQueryExecutor();
        OracleJdbcQueryExecutor.SqlQuery dvSql = dvExec.buildSql(QueryDefinition.Q1, SchemaModel.EMBEDDED, params);
        System.out.println("  SQL: " + dvSql.sql().replaceAll("\\s+", " ").trim());
        System.out.println();
        List<String[]> dvRows = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             var ps = conn.prepareStatement(dvSql.sql())) {
            for (int i = 0; i < dvSql.parameters().size(); i++) {
                ps.setObject(i + 1, dvSql.parameters().get(i));
            }
            t0 = System.nanoTime();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    dvRows.add(new String[]{
                            rs.getString("advisor_id"),
                            fmt(rs.getDouble("viewable_mv")),
                            fmtInt(rs.getInt("viewable_accts"))
                    });
                }
            }
            elapsed = System.nanoTime() - t0;
        }
        printHeader();
        for (int i = 0; i < dvRows.size(); i++) {
            printRow(i + 1, dvRows.get(i));
        }
        System.out.printf("  Total: %d rows | Latency: %.2f ms%n%n", dvRows.size(), elapsed / 1_000_000.0);

        // --- Comparison ---
        System.out.println("=============================================================");
        System.out.println("  COMPARISON");
        System.out.println("=============================================================");
        boolean match = true;
        if (mongoRows.size() != relRows.size() || mongoRows.size() != dvRows.size()) {
            System.out.printf("  Row counts differ: Mongo=%d, Rel=%d, DV=%d%n",
                    mongoRows.size(), relRows.size(), dvRows.size());
            match = false;
        }
        int max = Math.min(mongoRows.size(), Math.min(relRows.size(), dvRows.size()));
        int mismatches = 0;
        for (int i = 0; i < max; i++) {
            String mId = mongoRows.get(i)[0], mMv = mongoRows.get(i)[1];
            String rId = relRows.get(i)[0], rMv = relRows.get(i)[1];
            String dId = dvRows.get(i)[0], dMv = dvRows.get(i)[1];
            if (!mId.equals(rId) || !mId.equals(dId) || !mMv.equals(rMv) || !mMv.equals(dMv)) {
                if (mismatches < 5) {
                    System.out.printf("  Row %d MISMATCH: Mongo=[%s,%s] Rel=[%s,%s] DV=[%s,%s]%n",
                            i + 1, mId, mMv, rId, rMv, dId, dMv);
                }
                mismatches++;
                match = false;
            }
        }
        if (match) {
            System.out.printf("  ALL %d ROWS MATCH across all 3 targets%n", mongoRows.size());
        } else {
            System.out.printf("  %d mismatches found%n", mismatches);
        }
        System.out.println();

        ds.close();
    }

    private static void printHeader() {
        System.out.printf("  %-4s %-20s %15s %8s%n", "#", "Advisor ID", "Market Value", "Accts");
        System.out.printf("  %-4s %-20s %15s %8s%n", "──", "──────────────────", "─────────────", "─────");
    }

    private static void printRow(int idx, String[] row) {
        System.out.printf("  %-4d %-20s %15s %8s%n", idx, row[0], row[1], row[2]);
    }

    private static String fmt(Number n) {
        return n != null ? String.format("%,.2f", n.doubleValue()) : "0.00";
    }

    private static String fmtInt(Number n) {
        return n != null ? String.valueOf(n.intValue()) : "0";
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
