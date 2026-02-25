package com.helix.benchmark;

import com.helix.benchmark.benchmark.BenchmarkResult;
import com.helix.benchmark.benchmark.BenchmarkRunner;
import com.helix.benchmark.benchmark.QueryDetail;
import com.helix.benchmark.config.BenchmarkConfig;
import com.helix.benchmark.config.DatabaseTarget;
import com.helix.benchmark.config.SchemaModel;
import com.helix.benchmark.connection.ConnectionManager;
import com.helix.benchmark.datagen.DataLoader;
import com.helix.benchmark.datagen.RelationalDataLoader;
import com.helix.benchmark.datagen.ReferenceRegistry;
import com.helix.benchmark.datagen.TestDataGenerator;
import com.helix.benchmark.query.*;
import com.helix.benchmark.report.HtmlReportGenerator;
import com.helix.benchmark.schema.MongoSchemaManager;
import com.helix.benchmark.schema.OracleDualityViewSchemaManager;
import com.helix.benchmark.schema.OracleRelationalSchemaManager;
import com.helix.benchmark.schema.OracleSchemaManager;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HelixBenchmarkMain {
    private static final Logger log = LoggerFactory.getLogger(HelixBenchmarkMain.class);

    public record Configuration(DatabaseTarget target, SchemaModel model, String id) {
        public Configuration(DatabaseTarget target, SchemaModel model) {
            this(target, model, target.name() + "_" + model.name());
        }
    }

    public static List<Configuration> allConfigurations() {
        List<Configuration> configs = new ArrayList<>();
        for (DatabaseTarget target : DatabaseTarget.values()) {
            for (SchemaModel model : SchemaModel.values()) {
                configs.add(new Configuration(target, model));
            }
        }
        return configs;
    }

    public static List<Configuration> activeConfigurations(Set<DatabaseTarget> activeTargets) {
        List<Configuration> configs = new ArrayList<>();
        for (DatabaseTarget target : DatabaseTarget.values()) {
            if (!activeTargets.contains(target)) continue;
            for (SchemaModel model : SchemaModel.values()) {
                configs.add(new Configuration(target, model));
            }
        }
        return configs;
    }

    public static BenchmarkConfig loadConfig(String configPath) {
        try {
            InputStream is;
            if (configPath != null) {
                is = new FileInputStream(configPath);
            } else {
                is = HelixBenchmarkMain.class.getResourceAsStream("/benchmark-config.yaml");
                if (is == null) {
                    is = HelixBenchmarkMain.class.getClassLoader()
                            .getResourceAsStream("benchmark-config.yaml");
                }
                if (is == null) {
                    // Try loading from working directory
                    try {
                        is = new FileInputStream("benchmark-config.yaml");
                    } catch (Exception e) {
                        // Fall back to test config
                        is = HelixBenchmarkMain.class.getResourceAsStream("/test-config.yaml");
                    }
                }
            }
            return BenchmarkConfig.load(is);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config: " + e.getMessage(), e);
        }
    }

    static void configureTruststore() {
        String truststorePath = findTruststore();
        if (truststorePath != null) {
            java.io.File tsFile = new java.io.File(truststorePath);
            String absPath = tsFile.getAbsolutePath();
            System.setProperty("javax.net.ssl.trustStore", absPath);
            System.setProperty("javax.net.ssl.trustStoreType", "JKS");
            log.info("Configured JVM truststore: {}", absPath);
        }
    }

    private static final int CHUNK_SIZE = 50_000;

    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : null;
        BenchmarkConfig config = loadConfig(configPath);

        // Configure JVM-level truststore for Oracle MongoDB API TLS connections
        configureTruststore();

        ConnectionManager connectionManager = new ConnectionManager(config);
        Set<DatabaseTarget> activeTargets = config.activeTargets();

        log.info("=== Helix Database Benchmark Harness ===");
        log.info("Active targets: {}", activeTargets);
        log.info("Configurations: {} ({} targets x embedded model)", activeTargets.size(), activeTargets.size());

        // Step 1 & 2: Generate + Load data (skip if already loaded)
        ReferenceRegistry registry = new ReferenceRegistry(
                config.advisoryContextPoolSize(),
                config.partyRoleIdPoolSize(),
                config.finInstIdPoolSize()
        );
        TestDataGenerator generator = new TestDataGenerator(registry);
        HikariDataSource jdbcDataSource = createJdbcDataSource(connectionManager);

        MongoSchemaManager mongoSchemaManager = new MongoSchemaManager();
        OracleSchemaManager oracleSchemaManager = new OracleSchemaManager();
        OracleRelationalSchemaManager relSchemaManager = new OracleRelationalSchemaManager();
        OracleDualityViewSchemaManager dvSchemaManager = new OracleDualityViewSchemaManager();

        // Duality views depend on relational tables — ensure relational is implicitly active
        boolean needsRelational = activeTargets.contains(DatabaseTarget.ORACLE_RELATIONAL)
                || activeTargets.contains(DatabaseTarget.ORACLE_DUALITY_VIEW)
                || activeTargets.contains(DatabaseTarget.ORACLE_MONGO_API_DV);

        boolean dataExists = checkDataExists(connectionManager, config.advisorCount());
        if (dataExists) {
            log.info("--- Data already loaded, skipping data generation ---");
            // Still need to populate the registry with advisor/investor IDs for query params
            populateRegistryFromDb(connectionManager, registry);

            // Backfill relational tables if needed (for relational or duality view targets)
            if (needsRelational && jdbcDataSource != null
                    && !checkRelationalDataExists(jdbcDataSource)) {
                log.info("--- Backfilling Oracle Relational tables from MongoDB ---");
                backfillRelationalFromMongo(connectionManager, jdbcDataSource, relSchemaManager, config.jdbcBatchSize());
            }
        } else {
            log.info("--- Step 1/2: Generating and loading data in chunks ---");
            DataLoader dataLoader = new DataLoader();
            RelationalDataLoader relDataLoader = new RelationalDataLoader();
            int mongoBatchSize = config.batchSize();

            // Pre-create schemas (drop + create collections/tables)
            prepareSchemas(connectionManager, mongoSchemaManager, oracleSchemaManager, relSchemaManager, jdbcDataSource, activeTargets, needsRelational);

            // Generate advisors first (small, needed for referential integrity)
            List<Document> advisors = generator.generateAdvisors(config.advisorCount());
            log.info("Generated {} advisors", advisors.size());
            loadChunkToAllTargets(connectionManager, dataLoader, relDataLoader, jdbcDataSource,
                    "advisor", advisors, mongoBatchSize, config.jdbcBatchSize(), activeTargets, needsRelational);

            // Generate + load BookRoleInvestors in chunks
            log.info("Generating and loading {} BookRoleInvestors in chunks of {}...",
                    config.bookRoleInvestorCount(), CHUNK_SIZE);
            loadInChunks(generator, "bookRoleInvestor", config.bookRoleInvestorCount(),
                    connectionManager, dataLoader, relDataLoader, jdbcDataSource, mongoBatchSize, config.jdbcBatchSize(), activeTargets, needsRelational);

            // Generate + load BookRoleGroups in chunks
            log.info("Generating and loading {} BookRoleGroups in chunks of {}...",
                    config.bookRoleGroupCount(), CHUNK_SIZE);
            loadInChunks(generator, "bookRoleGroup", config.bookRoleGroupCount(),
                    connectionManager, dataLoader, relDataLoader, jdbcDataSource, mongoBatchSize, config.jdbcBatchSize(), activeTargets, needsRelational);

            // Generate + load Accounts in chunks
            log.info("Generating and loading {} Accounts in chunks of {}...",
                    config.accountCount(), CHUNK_SIZE);
            loadInChunks(generator, "account", config.accountCount(),
                    connectionManager, dataLoader, relDataLoader, jdbcDataSource, mongoBatchSize, config.jdbcBatchSize(), activeTargets, needsRelational);

            log.info("All data loaded");
        }

        // Always ensure indexes exist (safe to re-run — duplicates are caught)
        createAllIndexes(connectionManager, mongoSchemaManager, oracleSchemaManager, relSchemaManager, jdbcDataSource, activeTargets, needsRelational);

        // Create duality views (after relational tables and indexes are ready)
        if ((activeTargets.contains(DatabaseTarget.ORACLE_DUALITY_VIEW)
                || activeTargets.contains(DatabaseTarget.ORACLE_MONGO_API_DV)) && jdbcDataSource != null) {
            createDualityViews(jdbcDataSource, dvSchemaManager);
        }

        // Step 2.5: Validate result consistency across all targets
        QueryParameterGenerator paramGen = new QueryParameterGenerator(registry);
        // Pre-sample parameters from actual MongoDB data to guarantee results
        String mongoConnStr = connectionManager.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
        String mongoDbName = connectionManager.getDatabaseName(DatabaseTarget.MONGO_NATIVE);
        try (MongoClient mongoClient = MongoClients.create(mongoConnStr)) {
            MongoDatabase mongoDB = mongoClient.getDatabase(mongoDbName);
            paramGen.initFromData(mongoDB, 100);
        }
        log.info("--- Step 2.5: Validating results across all targets ---");
        boolean validationPassed = ResultValidator.validate(connectionManager, jdbcDataSource, paramGen, 1);
        if (!validationPassed) {
            log.warn("Result validation detected mismatches — review warnings above");
        }

        // Step 3: Run benchmarks
        log.info("--- Step 3: Running benchmarks ---");
        BenchmarkRunner runner = new BenchmarkRunner(
                config.warmUpIterations(), config.measurementIterations());
        MongoQueryExecutor mongoExecutor = new MongoQueryExecutor();
        OracleJdbcQueryExecutor oracleExecutor = new OracleJdbcQueryExecutor();
        OracleRelationalQueryExecutor relExecutor = new OracleRelationalQueryExecutor();
        OracleDualityViewQueryExecutor dvExecutor = new OracleDualityViewQueryExecutor();
        List<BenchmarkResult> allResults = new ArrayList<>();
        List<QueryDetail> allDetails = new ArrayList<>();
        String ordsBaseUrl = config.ordsBaseUrl();

        for (Configuration cfg : activeConfigurations(activeTargets)) {
            if (cfg.target().usesMongoDriver()) {
                String connStr = connectionManager.getMongoConnectionString(cfg.target());
                String dbName = connectionManager.getDatabaseName(cfg.target());
                try (MongoClient client = MongoClients.create(connStr)) {
                    MongoDatabase db = client.getDatabase(dbName);
                    DatabaseTarget target = cfg.target();

                    for (QueryDefinition query : QueryDefinition.values()) {
                        try {
                            String collName = mongoExecutor.getCollectionName(query, cfg.model(), target);
                            MongoCollection<Document> collection = db.getCollection(collName);

                            BenchmarkResult result = runner.run(
                                    query.queryName(), cfg.id(),
                                    () -> {
                                        Map<String, Object> params = paramGen.generate(query);
                                        if (query.isAggregation()) {
                                            mongoExecutor.executeAggregation(
                                                    collection, query, cfg.model(), params, target);
                                        } else {
                                            mongoExecutor.executeFind(
                                                    collection, query, cfg.model(), params, target);
                                        }
                                        return null;
                                    }
                            );
                            allResults.add(result);

                            // Capture query detail after benchmark timing
                            try {
                                Map<String, Object> sampleParams = paramGen.generate(query);
                                QueryDetail detail = mongoExecutor.captureQueryDetail(
                                        collection, query, cfg.model(), sampleParams, target, cfg.id());
                                allDetails.add(detail);
                            } catch (Exception ex) {
                                log.warn("Failed to capture detail for {} on {}: {}", query.queryName(), cfg.id(), ex.getMessage());
                            }
                        } catch (Exception e) {
                            log.warn("Benchmark failed for {} on {}: {}", query.queryName(), cfg.id(), e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to connect to {} for benchmarks: {}", cfg.target(), e.getMessage());
                }
            } else if (cfg.target().usesJdbc() && jdbcDataSource != null) {
                OracleJdbcQueryExecutor jdbcExec = switch (cfg.target()) {
                    case ORACLE_RELATIONAL -> relExecutor;
                    case ORACLE_DUALITY_VIEW -> dvExecutor;
                    default -> oracleExecutor;
                };

                for (QueryDefinition query : QueryDefinition.values()) {
                    try {
                        BenchmarkResult result = runner.run(
                                query.queryName(), cfg.id(),
                                () -> {
                                    Map<String, Object> params = paramGen.generate(query);
                                    OracleJdbcQueryExecutor.SqlQuery sqlQuery =
                                            jdbcExec.buildSql(query, cfg.model(), params);
                                    try (Connection conn = jdbcDataSource.getConnection();
                                         var ps = conn.prepareStatement(sqlQuery.sql())) {
                                        for (int i = 0; i < sqlQuery.parameters().size(); i++) {
                                            ps.setObject(i + 1, sqlQuery.parameters().get(i));
                                        }
                                        try (ResultSet rs = ps.executeQuery()) {
                                            while (rs.next()) {
                                                rs.getString(1);
                                            }
                                        }
                                    } catch (Exception ex) {
                                        throw new RuntimeException(ex);
                                    }
                                    return null;
                                }
                        );
                        allResults.add(result);

                        // Capture query detail after benchmark timing
                        try (Connection conn = jdbcDataSource.getConnection()) {
                            Map<String, Object> sampleParams = paramGen.generate(query);
                            QueryDetail detail = jdbcExec.captureQueryDetail(
                                    conn, query, cfg.model(), sampleParams, cfg.id(), ordsBaseUrl);
                            allDetails.add(detail);
                        } catch (Exception ex) {
                            log.warn("Failed to capture detail for {} on {}: {}", query.queryName(), cfg.id(), ex.getMessage());
                        }
                    } catch (Exception e) {
                        log.warn("Benchmark failed for {} on {}: {}", query.queryName(), cfg.id(), e.getMessage());
                    }
                }
            }
        }

        // Step 4: Generate report
        log.info("--- Step 4: Generating report ---");
        if (!allResults.isEmpty()) {
            try {
                HtmlReportGenerator reportGen = new HtmlReportGenerator();
                Path reportPath = Paths.get("benchmark-report.html");
                reportGen.generateToFile(allResults, allDetails, reportPath);
                log.info("Report generated: {}", reportPath.toAbsolutePath());
            } catch (Exception e) {
                log.error("Failed to generate report: {}", e.getMessage(), e);
            }
        } else {
            log.warn("No benchmark results collected. Check database connectivity.");
        }

        // Cleanup
        if (jdbcDataSource != null) {
            jdbcDataSource.close();
        }

        log.info("=== Benchmark complete ===");
    }

    private static void loadInChunks(TestDataGenerator generator, String collectionType,
                                      int totalCount, ConnectionManager connectionManager,
                                      DataLoader dataLoader, RelationalDataLoader relDataLoader,
                                      HikariDataSource jdbcDataSource,
                                      int mongoBatchSize, int jdbcBatchSize,
                                      Set<DatabaseTarget> activeTargets, boolean needsRelational) {
        int loaded = 0;
        while (loaded < totalCount) {
            int chunkSize = Math.min(CHUNK_SIZE, totalCount - loaded);
            List<Document> chunk = switch (collectionType) {
                case "bookRoleInvestor" -> generator.generateBookRoleInvestors(loaded, chunkSize);
                case "bookRoleGroup" -> generator.generateBookRoleGroups(loaded, chunkSize);
                case "account" -> generator.generateAccounts(loaded, chunkSize);
                default -> throw new IllegalArgumentException("Unknown collection: " + collectionType);
            };
            log.info("Generated chunk [{}-{}) of {} for {}", loaded, loaded + chunkSize, totalCount, collectionType);

            loadChunkToAllTargets(connectionManager, dataLoader, relDataLoader, jdbcDataSource,
                    collectionType, chunk, mongoBatchSize, jdbcBatchSize, activeTargets, needsRelational);
            loaded += chunkSize;
        }
        log.info("Completed loading {} {} documents", totalCount, collectionType);
    }

    private static void loadChunkToAllTargets(ConnectionManager connectionManager,
                                               DataLoader dataLoader, RelationalDataLoader relDataLoader,
                                               HikariDataSource jdbcDataSource,
                                               String collectionType, List<Document> chunk,
                                               int mongoBatchSize, int jdbcBatchSize,
                                               Set<DatabaseTarget> activeTargets, boolean needsRelational) {
        String embeddedCollectionName = collectionType;
        String jdbcTableName = switch (collectionType) {
            case "advisor" -> "jdbc_advisor";
            case "bookRoleInvestor" -> "jdbc_book_role_investor";
            case "bookRoleGroup" -> "jdbc_book_role_group";
            case "account" -> "jdbc_account";
            default -> throw new IllegalArgumentException("Unknown collection: " + collectionType);
        };

        // Load to MongoDB targets (Native + Oracle Mongo API)
        for (DatabaseTarget target : new DatabaseTarget[]{DatabaseTarget.MONGO_NATIVE, DatabaseTarget.ORACLE_MONGO_API}) {
            if (!activeTargets.contains(target)) continue;
            try {
                String connStr = connectionManager.getMongoConnectionString(target);
                String dbName = connectionManager.getDatabaseName(target);
                try (MongoClient client = MongoClients.create(connStr)) {
                    MongoDatabase db = client.getDatabase(dbName);
                    MongoCollection<Document> col = db.getCollection(embeddedCollectionName);
                    dataLoader.loadToMongo(col, chunk, mongoBatchSize);
                }
            } catch (Exception e) {
                log.warn("Failed to load {} to {}: {}", collectionType, target, e.getMessage());
            }
        }

        // Load to Oracle JDBC (JSON collection tables)
        if (activeTargets.contains(DatabaseTarget.ORACLE_JDBC) && jdbcDataSource != null) {
            try {
                dataLoader.loadToOracle(jdbcDataSource, jdbcTableName, chunk, jdbcBatchSize);
            } catch (Exception e) {
                log.warn("Failed to load {} to JDBC: {}", collectionType, e.getMessage());
            }
        }

        // Load to Oracle Relational tables (also needed for duality views)
        if (needsRelational && jdbcDataSource != null) {
            try {
                relDataLoader.loadToRelational(jdbcDataSource, collectionType, chunk, jdbcBatchSize);
            } catch (Exception e) {
                log.warn("Failed to load {} to Relational: {}", collectionType, e.getMessage());
            }
        }
    }

    private static void prepareSchemas(ConnectionManager connectionManager,
                                        MongoSchemaManager mongoSchemaManager,
                                        OracleSchemaManager oracleSchemaManager,
                                        OracleRelationalSchemaManager relSchemaManager,
                                        HikariDataSource jdbcDataSource,
                                        Set<DatabaseTarget> activeTargets, boolean needsRelational) {
        // Prepare MongoDB schemas (drop collections)
        for (DatabaseTarget target : new DatabaseTarget[]{DatabaseTarget.MONGO_NATIVE, DatabaseTarget.ORACLE_MONGO_API}) {
            if (!activeTargets.contains(target)) continue;
            try {
                String connStr = connectionManager.getMongoConnectionString(target);
                String dbName = connectionManager.getDatabaseName(target);
                try (MongoClient client = MongoClients.create(connStr)) {
                    MongoDatabase db = client.getDatabase(dbName);
                    for (String name : mongoSchemaManager.getCollectionNames(SchemaModel.EMBEDDED)) {
                        db.getCollection(name).drop();
                        log.info("Dropped collection {} on {}", name, target);
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to prepare {} schema: {}", target, e.getMessage());
            }
        }

        // Prepare Oracle JDBC schemas (drop + create tables)
        if (activeTargets.contains(DatabaseTarget.ORACLE_JDBC) && jdbcDataSource != null) {
            try (Connection conn = jdbcDataSource.getConnection(); Statement stmt = conn.createStatement()) {
                for (String table : oracleSchemaManager.getTableNames(SchemaModel.EMBEDDED)) {
                    try {
                        stmt.execute("DROP TABLE " + table + " PURGE");
                    } catch (Exception ignored) {}
                }
                for (String ddl : oracleSchemaManager.getCreateTableStatements(SchemaModel.EMBEDDED)) {
                    stmt.execute(ddl);
                }
                conn.commit();
                log.info("Oracle JDBC tables prepared");
            } catch (Exception e) {
                log.warn("Failed to prepare Oracle JDBC schema: {}", e.getMessage());
            }
        }

        // Prepare Oracle Relational schemas (drop children first, then parents, then create)
        // Also needed when duality views are active since they depend on relational tables
        if (needsRelational && jdbcDataSource != null) {
            try (Connection conn = jdbcDataSource.getConnection(); Statement stmt = conn.createStatement()) {
                for (String table : relSchemaManager.getDropOrder()) {
                    try {
                        stmt.execute("DROP TABLE " + table + " PURGE");
                    } catch (Exception ignored) {}
                }
                for (String ddl : relSchemaManager.getCreateTableStatements()) {
                    stmt.execute(ddl);
                }
                conn.commit();
                log.info("Oracle Relational tables prepared");
            } catch (Exception e) {
                log.warn("Failed to prepare Oracle Relational schema: {}", e.getMessage());
            }
        }
    }

    private static void createAllIndexes(ConnectionManager connectionManager,
                                          MongoSchemaManager mongoSchemaManager,
                                          OracleSchemaManager oracleSchemaManager,
                                          OracleRelationalSchemaManager relSchemaManager,
                                          HikariDataSource jdbcDataSource,
                                          Set<DatabaseTarget> activeTargets, boolean needsRelational) {
        // MongoDB indexes
        for (DatabaseTarget target : new DatabaseTarget[]{DatabaseTarget.MONGO_NATIVE, DatabaseTarget.ORACLE_MONGO_API}) {
            if (!activeTargets.contains(target)) continue;
            try {
                String connStr = connectionManager.getMongoConnectionString(target);
                String dbName = connectionManager.getDatabaseName(target);
                try (MongoClient client = MongoClients.create(connStr)) {
                    MongoDatabase db = client.getDatabase(dbName);
                    for (var idx : mongoSchemaManager.getIndexDefinitions(SchemaModel.EMBEDDED)) {
                        Document keys = new Document();
                        for (var e : idx.keys().entrySet()) {
                            keys.append(e.getKey(), e.getValue());
                        }
                        db.getCollection(idx.collection()).createIndex(keys);
                    }
                    log.info("Indexes created for {}", target);
                }
            } catch (Exception e) {
                log.warn("Failed to create indexes on {}: {}", target, e.getMessage());
            }
        }

        // Oracle JDBC indexes
        if (activeTargets.contains(DatabaseTarget.ORACLE_JDBC) && jdbcDataSource != null) {
            try (Connection conn = jdbcDataSource.getConnection(); Statement stmt = conn.createStatement()) {
                for (String indexSql : oracleSchemaManager.getIndexStatements(SchemaModel.EMBEDDED)) {
                    try {
                        stmt.execute(indexSql);
                    } catch (Exception e) {
                        log.warn("Index creation failed (may already exist): {}", e.getMessage());
                    }
                }
                log.info("Oracle JDBC indexes created");
                conn.commit();
            } catch (Exception e) {
                log.warn("Failed to create Oracle JDBC indexes: {}", e.getMessage());
            }

        }

        // Oracle Relational indexes (also needed for duality views)
        if (needsRelational && jdbcDataSource != null) {
            try (Connection conn = jdbcDataSource.getConnection(); Statement stmt = conn.createStatement()) {
                for (String indexSql : relSchemaManager.getIndexStatements()) {
                    try {
                        stmt.execute(indexSql);
                    } catch (Exception e) {
                        log.warn("Relational index creation failed (may already exist): {}", e.getMessage());
                    }
                }
                log.info("Oracle Relational indexes created");
                conn.commit();
            } catch (Exception e) {
                log.warn("Failed to create Oracle Relational indexes: {}", e.getMessage());
            }
        }
    }

    private static void createDualityViews(HikariDataSource jdbcDataSource,
                                              OracleDualityViewSchemaManager dvSchemaManager) {
        try (Connection conn = jdbcDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            for (String ddl : dvSchemaManager.getCreateViewStatements()) {
                try {
                    stmt.execute(ddl);
                } catch (Exception e) {
                    log.warn("Duality view creation failed: {}", e.getMessage());
                }
            }
            conn.commit();
            log.info("Oracle Duality Views created");
        } catch (Exception e) {
            log.warn("Failed to create duality views: {}", e.getMessage());
        }
    }

    private static HikariDataSource createJdbcDataSource(ConnectionManager connMgr) {
        try {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(connMgr.getJdbcUrl());
            hikariConfig.setUsername(connMgr.getJdbcUsername());
            hikariConfig.setPassword(connMgr.getJdbcPassword());
            hikariConfig.setMaximumPoolSize(connMgr.getJdbcMaxPoolSize());
            hikariConfig.setAutoCommit(false);
            hikariConfig.addDataSourceProperty("oracle.jdbc.J2EE13Compliant", "true");

            // Trust self-signed TLS certificate from Oracle ADB-Free
            String truststorePath = findTruststore();
            if (truststorePath != null) {
                hikariConfig.addDataSourceProperty("javax.net.ssl.trustStore", truststorePath);
                hikariConfig.addDataSourceProperty("javax.net.ssl.trustStorePassword", connMgr.getJdbcPassword());
                hikariConfig.addDataSourceProperty("javax.net.ssl.trustStoreType", "JKS");
                hikariConfig.addDataSourceProperty("oracle.net.ssl_server_dn_match", "false");
            }

            return new HikariDataSource(hikariConfig);
        } catch (Exception e) {
            log.warn("Could not create JDBC DataSource: {}", e.getMessage());
            return null;
        }
    }

    private static String findTruststore() {
        // Look for truststore.jks in common locations
        String[] paths = {
                "docker/truststore.jks",
                "../docker/truststore.jks",
                "truststore.jks"
        };
        for (String path : paths) {
            if (new java.io.File(path).exists()) {
                log.info("Found truststore at: {}", path);
                return path;
            }
        }
        log.warn("No truststore.jks found; JDBC TLS connection may fail");
        return null;
    }

    private static boolean checkDataExists(ConnectionManager connectionManager, int expectedAdvisorCount) {
        try {
            String connStr = connectionManager.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
            String dbName = connectionManager.getDatabaseName(DatabaseTarget.MONGO_NATIVE);
            try (MongoClient client = MongoClients.create(connStr)) {
                MongoDatabase db = client.getDatabase(dbName);
                long advisorCount = db.getCollection("advisor").countDocuments();
                long investorCount = db.getCollection("bookRoleInvestor").countDocuments();
                log.info("Existing data check: advisor={}, bookRoleInvestor={}", advisorCount, investorCount);
                return advisorCount >= expectedAdvisorCount && investorCount > 0;
            }
        } catch (Exception e) {
            log.warn("Could not check existing data: {}", e.getMessage());
            return false;
        }
    }

    private static boolean checkRelationalDataExists(HikariDataSource jdbcDataSource) {
        String[] tables = {"rel_advisor", "rel_book_role_investor", "rel_book_role_group", "rel_account"};
        try (Connection conn = jdbcDataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            for (String table : tables) {
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    if (rs.next() && rs.getLong(1) == 0) {
                        log.info("Relational table {} is empty, backfill needed", table);
                        return false;
                    }
                }
            }
            log.info("Relational data check: all parent tables populated");
            return true;
        } catch (Exception e) {
            log.info("Relational tables not found (will create): {}", e.getMessage());
        }
        return false;
    }

    private static void backfillRelationalFromMongo(ConnectionManager connectionManager,
                                                     HikariDataSource jdbcDataSource,
                                                     OracleRelationalSchemaManager relSchemaManager,
                                                     int jdbcBatchSize) {
        // Create tables
        try (Connection conn = jdbcDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            for (String table : relSchemaManager.getDropOrder()) {
                try { stmt.execute("DROP TABLE " + table + " PURGE"); } catch (Exception ignored) {}
            }
            for (String ddl : relSchemaManager.getCreateTableStatements()) {
                stmt.execute(ddl);
            }
            conn.commit();
            log.info("Oracle Relational tables created for backfill");
        } catch (Exception e) {
            log.warn("Failed to create relational tables: {}", e.getMessage());
            return;
        }

        // Read from Mongo and load to relational
        RelationalDataLoader relDataLoader = new RelationalDataLoader();
        try {
            String connStr = connectionManager.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
            String dbName = connectionManager.getDatabaseName(DatabaseTarget.MONGO_NATIVE);
            try (MongoClient client = MongoClients.create(connStr)) {
                MongoDatabase db = client.getDatabase(dbName);

                for (String collectionType : new String[]{"advisor", "bookRoleInvestor", "bookRoleGroup", "account"}) {
                    log.info("Backfilling {} to relational tables...", collectionType);
                    List<Document> chunk = new ArrayList<>();
                    int loaded = 0;
                    for (Document doc : db.getCollection(collectionType).find()) {
                        chunk.add(doc);
                        if (chunk.size() >= CHUNK_SIZE) {
                            relDataLoader.loadToRelational(jdbcDataSource, collectionType, chunk, jdbcBatchSize);
                            loaded += chunk.size();
                            log.info("Backfilled {} {} documents", loaded, collectionType);
                            chunk = new ArrayList<>();
                        }
                    }
                    if (!chunk.isEmpty()) {
                        relDataLoader.loadToRelational(jdbcDataSource, collectionType, chunk, jdbcBatchSize);
                        loaded += chunk.size();
                    }
                    log.info("Backfill complete: {} {} documents", loaded, collectionType);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to backfill relational data: {}", e.getMessage());
        }

        // Create indexes
        try (Connection conn = jdbcDataSource.getConnection(); Statement stmt = conn.createStatement()) {
            for (String indexSql : relSchemaManager.getIndexStatements()) {
                try { stmt.execute(indexSql); } catch (Exception e) {
                    log.warn("Relational index failed: {}", e.getMessage());
                }
            }
            conn.commit();
            log.info("Oracle Relational indexes created after backfill");
        } catch (Exception e) {
            log.warn("Failed to create relational indexes: {}", e.getMessage());
        }
    }

    private static void populateRegistryFromDb(ConnectionManager connectionManager, ReferenceRegistry registry) {
        try {
            String connStr = connectionManager.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE);
            String dbName = connectionManager.getDatabaseName(DatabaseTarget.MONGO_NATIVE);
            try (MongoClient client = MongoClients.create(connStr)) {
                MongoDatabase db = client.getDatabase(dbName);

                // Load advisor IDs
                for (Document doc : db.getCollection("advisor").find()
                        .projection(new Document("_id", 1))) {
                    registry.registerAdvisorId(doc.getString("_id"));
                }
                log.info("Loaded {} advisor IDs from DB", registry.getAdvisorIds().size());

                // Load a sample of investor IDs (don't need all for param generation)
                int investorCount = 0;
                for (Document doc : db.getCollection("bookRoleInvestor").find()
                        .projection(new Document("investorId", 1))
                        .limit(10000)) {
                    String investorId = doc.getString("investorId");
                    if (investorId != null) {
                        registry.registerInvestorId(investorId);
                        investorCount++;
                    }
                }
                log.info("Loaded {} investor IDs from DB", investorCount);
            }
        } catch (Exception e) {
            log.warn("Could not populate registry from DB: {}", e.getMessage());
        }
    }

}
