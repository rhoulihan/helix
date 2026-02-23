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
        log.info("=== Result Validation: Comparing all targets ===");

        MongoQueryExecutor mongoExec = new MongoQueryExecutor();
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
                                mongoExec, relExec, dvExec, run);
                    } else {
                        ok = validateFind(query, params, connMgr, jdbcDs,
                                mongoExec, relExec, dvExec, run);
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
                                                OracleRelationalQueryExecutor relExec,
                                                OracleDualityViewQueryExecutor dvExec,
                                                int run) throws Exception {
        // MongoDB
        String connStr = connMgr.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
        String dbName = connMgr.getDatabaseName(DatabaseTarget.MONGO_NATIVE);
        List<String[]> mongoRows;
        try (MongoClient client = MongoClients.create(connStr)) {
            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> col = db.getCollection(query.embeddedCollection());
            var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED,
                    params, DatabaseTarget.MONGO_NATIVE);
            List<Document> docs = col.aggregate(pipeline).into(new ArrayList<>());
            mongoRows = new ArrayList<>();
            for (Document doc : docs) {
                mongoRows.add(extractAggRow(doc));
            }
        }

        // Relational
        List<String[]> relRows = executeJdbcAgg(jdbcDs, relExec, query, params);

        // Duality View
        List<String[]> dvRows = executeJdbcAgg(jdbcDs, dvExec, query, params);

        // Oracle Mongo API (DV)
        String dvMongoConnStr = connMgr.getMongoConnectionString(DatabaseTarget.ORACLE_MONGO_API_DV);
        String dvMongoDbName = connMgr.getDatabaseName(DatabaseTarget.ORACLE_MONGO_API_DV);
        List<String[]> dvMongoRows;
        try (MongoClient client = MongoClients.create(dvMongoConnStr)) {
            MongoDatabase db = client.getDatabase(dvMongoDbName);
            String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API_DV);
            MongoCollection<Document> col = db.getCollection(collName);
            var pipeline = mongoExec.buildAggregationPipeline(query, SchemaModel.EMBEDDED,
                    params, DatabaseTarget.ORACLE_MONGO_API_DV);
            List<Document> docs = col.aggregate(pipeline).into(new ArrayList<>());
            dvMongoRows = new ArrayList<>();
            for (Document doc : docs) {
                dvMongoRows.add(extractAggRow(doc));
            }
        }

        // Compare
        return compareAggResults(query, run, params, mongoRows, relRows, dvRows, dvMongoRows);
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
                    String aid = rs.getString("advisor_id");
                    double mv = rs.getDouble("viewable_mv");
                    String id = rs.getString("id");
                    rows.add(new String[]{aid, fmt(mv), id != null ? id : "null"});
                }
            }
        }
        return rows;
    }

    private static boolean compareAggResults(QueryDefinition query, int run,
                                              Map<String, Object> params,
                                              List<String[]> mongo, List<String[]> rel,
                                              List<String[]> dv, List<String[]> dvMongo) {
        boolean ok = true;
        String label = query.queryName() + " run " + run;

        if (mongo.size() != rel.size() || mongo.size() != dv.size() || mongo.size() != dvMongo.size()) {
            log.warn("  MISMATCH {} counts: Mongo={}, Rel={}, DV={}, DV-Mongo={} | params={}",
                    label, mongo.size(), rel.size(), dv.size(), dvMongo.size(), paramSummary(params));
            ok = false;
        }

        // Compare row-by-row (results are ordered by viewable_mv DESC)
        int max = Math.min(mongo.size(), Math.min(rel.size(), Math.min(dv.size(), dvMongo.size())));
        int rowMismatches = 0;
        for (int i = 0; i < max; i++) {
            String mId = mongo.get(i)[2], rId = rel.get(i)[2], dId = dv.get(i)[2], dmId = dvMongo.get(i)[2];
            String mMv = mongo.get(i)[1], rMv = rel.get(i)[1], dMv = dv.get(i)[1], dmMv = dvMongo.get(i)[1];
            String mAid = mongo.get(i)[0], rAid = rel.get(i)[0], dAid = dv.get(i)[0], dmAid = dvMongo.get(i)[0];

            if (!Objects.equals(mAid, rAid) || !Objects.equals(mAid, dAid) || !Objects.equals(mAid, dmAid)
                    || !Objects.equals(mMv, rMv) || !Objects.equals(mMv, dMv) || !Objects.equals(mMv, dmMv)
                    || !Objects.equals(mId, rId) || !Objects.equals(mId, dId) || !Objects.equals(mId, dmId)) {
                if (rowMismatches < 3) {
                    log.warn("  MISMATCH {} row {}: Mongo=[{},{},{}] Rel=[{},{},{}] DV=[{},{},{}] DV-Mongo=[{},{},{}]",
                            label, i, mId, mAid, mMv, rId, rAid, rMv, dId, dAid, dMv, dmId, dmAid, dmMv);
                }
                rowMismatches++;
                ok = false;
            }
        }
        if (rowMismatches > 3) {
            log.warn("  ... and {} more row mismatches for {}", rowMismatches - 3, label);
        }

        if (ok) {
            log.info("  PASS {} — {} rows match | params={}", label, mongo.size(), paramSummary(params));
        }
        return ok;
    }

    // ---- Q5-Q9: Find queries (return full documents) ----

    private static boolean validateFind(QueryDefinition query, Map<String, Object> params,
                                         ConnectionManager connMgr, HikariDataSource jdbcDs,
                                         MongoQueryExecutor mongoExec,
                                         OracleRelationalQueryExecutor relExec,
                                         OracleDualityViewQueryExecutor dvExec,
                                         int run) throws Exception {
        // MongoDB — collect full documents
        String connStr = connMgr.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
        String dbName = connMgr.getDatabaseName(DatabaseTarget.MONGO_NATIVE);
        Map<String, JsonNode> mongoDocs;
        try (MongoClient client = MongoClients.create(connStr)) {
            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> col = db.getCollection(query.embeddedCollection());
            var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params);
            List<Document> docs = col.find(filter).into(new ArrayList<>());
            mongoDocs = new TreeMap<>();
            for (Document doc : docs) {
                String id = doc.getString("_id");
                mongoDocs.put(id, mapper.readTree(doc.toJson()));
            }
        }

        // Relational — now returns JSON documents via JSON_OBJECT
        Map<String, JsonNode> relDocs = executeJdbcFindDocs(jdbcDs, relExec, query, params);

        // Duality View — returns json_serialize(data)
        Map<String, JsonNode> dvDocs = executeJdbcFindDocs(jdbcDs, dvExec, query, params);

        // Oracle Mongo API (DV) — via MongoDB driver
        String dvMongoConnStr = connMgr.getMongoConnectionString(DatabaseTarget.ORACLE_MONGO_API_DV);
        String dvMongoDbName = connMgr.getDatabaseName(DatabaseTarget.ORACLE_MONGO_API_DV);
        Map<String, JsonNode> dvMongoDocs;
        try (MongoClient client = MongoClients.create(dvMongoConnStr)) {
            MongoDatabase db = client.getDatabase(dvMongoDbName);
            String collName = mongoExec.getCollectionName(query, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API_DV);
            MongoCollection<Document> col = db.getCollection(collName);
            var filter = mongoExec.buildFindFilter(query, SchemaModel.EMBEDDED, params);
            List<Document> docs = col.find(filter).into(new ArrayList<>());
            dvMongoDocs = new TreeMap<>();
            for (Document doc : docs) {
                String id = doc.getString("_id");
                dvMongoDocs.put(id, mapper.readTree(doc.toJson()));
            }
        }

        return compareFindResults(query, run, params,
                mongoDocs.keySet(), relDocs.keySet(), dvDocs.keySet(), dvMongoDocs.keySet());
    }

    private static Map<String, JsonNode> executeJdbcFindDocs(HikariDataSource ds,
                                                               OracleJdbcQueryExecutor exec,
                                                               QueryDefinition query,
                                                               Map<String, Object> params) throws Exception {
        OracleJdbcQueryExecutor.SqlQuery sqlQuery = exec.buildSql(query, SchemaModel.EMBEDDED, params);
        Map<String, JsonNode> docs = new TreeMap<>();
        try (Connection conn = ds.getConnection();
             var ps = conn.prepareStatement(sqlQuery.sql())) {
            for (int i = 0; i < sqlQuery.parameters().size(); i++) {
                ps.setObject(i + 1, sqlQuery.parameters().get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // Both relational (JSON_OBJECT) and DV (json_serialize) return JSON in column 1
                    String json = rs.getString(1);
                    if (json != null) {
                        try {
                            JsonNode node = mapper.readTree(json);
                            JsonNode idNode = node.get("_id");
                            if (idNode != null) {
                                docs.put(idNode.asText(), node);
                            }
                        } catch (Exception e) {
                            log.debug("Could not parse JSON: {}", e.getMessage());
                        }
                    }
                }
            }
        }
        return docs;
    }

    private static boolean compareFindResults(QueryDefinition query, int run,
                                               Map<String, Object> params,
                                               Set<String> mongo, Set<String> rel,
                                               Set<String> dv, Set<String> dvMongo) {
        boolean ok = true;
        String label = query.queryName() + " run " + run;

        if (mongo.size() != rel.size() || mongo.size() != dv.size() || mongo.size() != dvMongo.size()) {
            log.warn("  MISMATCH {} counts: Mongo={}, Rel={}, DV={}, DV-Mongo={} | params={}",
                    label, mongo.size(), rel.size(), dv.size(), dvMongo.size(), paramSummary(params));
            ok = false;
        }

        // ID set differences
        Set<String> mongoNotRel = diff(mongo, rel);
        Set<String> relNotMongo = diff(rel, mongo);
        Set<String> mongoNotDv = diff(mongo, dv);
        Set<String> dvNotMongo = diff(dv, mongo);
        Set<String> mongoNotDvMongo = diff(mongo, dvMongo);
        Set<String> dvMongoNotMongo = diff(dvMongo, mongo);

        if (!mongoNotRel.isEmpty()) {
            log.warn("  {} in Mongo NOT Rel: {} (first 3: {})", label, mongoNotRel.size(), first3(mongoNotRel));
            ok = false;
        }
        if (!relNotMongo.isEmpty()) {
            log.warn("  {} in Rel NOT Mongo: {} (first 3: {})", label, relNotMongo.size(), first3(relNotMongo));
            ok = false;
        }
        if (!mongoNotDv.isEmpty()) {
            log.warn("  {} in Mongo NOT DV: {} (first 3: {})", label, mongoNotDv.size(), first3(mongoNotDv));
            ok = false;
        }
        if (!dvNotMongo.isEmpty()) {
            log.warn("  {} in DV NOT Mongo: {} (first 3: {})", label, dvNotMongo.size(), first3(dvNotMongo));
            ok = false;
        }
        if (!mongoNotDvMongo.isEmpty()) {
            log.warn("  {} in Mongo NOT DV-Mongo: {} (first 3: {})", label, mongoNotDvMongo.size(), first3(mongoNotDvMongo));
            ok = false;
        }
        if (!dvMongoNotMongo.isEmpty()) {
            log.warn("  {} in DV-Mongo NOT Mongo: {} (first 3: {})", label, dvMongoNotMongo.size(), first3(dvMongoNotMongo));
            ok = false;
        }

        if (ok) {
            log.info("  PASS {} — {} IDs match | params={}", label, mongo.size(), paramSummary(params));
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
