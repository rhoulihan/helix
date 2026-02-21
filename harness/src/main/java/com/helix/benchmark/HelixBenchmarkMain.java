package com.helix.benchmark;

import com.helix.benchmark.benchmark.BenchmarkResult;
import com.helix.benchmark.benchmark.BenchmarkRunner;
import com.helix.benchmark.config.BenchmarkConfig;
import com.helix.benchmark.config.DatabaseTarget;
import com.helix.benchmark.config.SchemaModel;
import com.helix.benchmark.connection.ConnectionManager;
import com.helix.benchmark.datagen.DataLoader;
import com.helix.benchmark.datagen.ReferenceRegistry;
import com.helix.benchmark.datagen.TestDataGenerator;
import com.helix.benchmark.query.*;
import com.helix.benchmark.report.HtmlReportGenerator;
import com.helix.benchmark.schema.MongoSchemaManager;
import com.helix.benchmark.schema.OracleSchemaManager;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : null;
        BenchmarkConfig config = loadConfig(configPath);
        ConnectionManager connectionManager = new ConnectionManager(config);

        log.info("=== Helix Database Benchmark Harness ===");
        log.info("Configurations: 6 (3 targets x 2 models)");

        // Step 1: Generate test data
        log.info("--- Step 1: Generating test data ---");
        ReferenceRegistry registry = new ReferenceRegistry(
                config.advisoryContextPoolSize(),
                config.partyRoleIdPoolSize(),
                config.finInstIdPoolSize()
        );
        TestDataGenerator generator = new TestDataGenerator(registry);

        List<Document> advisors = generator.generateAdvisors(config.advisorCount());
        log.info("Generated {} advisors", advisors.size());

        List<Document> bookRoleInvestors = generator.generateBookRoleInvestors(config.bookRoleInvestorCount());
        log.info("Generated {} BookRoleInvestors", bookRoleInvestors.size());

        List<Document> bookRoleGroups = generator.generateBookRoleGroups(config.bookRoleGroupCount());
        log.info("Generated {} BookRoleGroups", bookRoleGroups.size());

        List<Document> accounts = generator.generateAccounts(config.accountCount());
        log.info("Generated {} Accounts", accounts.size());

        // Prepare normalized data
        Map<String, List<Document>> embeddedData = Map.of(
                "advisor", advisors,
                "bookRoleInvestor", bookRoleInvestors,
                "bookRoleGroup", bookRoleGroups,
                "account", accounts
        );
        List<Document> normalizedDocs = TestDataGenerator.transformToNormalized(embeddedData);
        log.info("Transformed to {} normalized documents", normalizedDocs.size());

        // Step 2: Load data into each target + model combination
        log.info("--- Step 2: Loading data into targets ---");
        DataLoader dataLoader = new DataLoader();
        MongoSchemaManager mongoSchemaManager = new MongoSchemaManager();
        int mongoBatchSize = config.batchSize();

        for (Configuration cfg : allConfigurations()) {
            log.info("Loading data for: {}", cfg.id());
            try {
                if (cfg.target().usesMongoDriver()) {
                    loadMongoData(connectionManager, cfg, mongoSchemaManager,
                            dataLoader, mongoBatchSize, embeddedData, normalizedDocs);
                }
                // Oracle JDBC loading would go here with actual Oracle connection
            } catch (Exception e) {
                log.warn("Failed to load data for {}: {}", cfg.id(), e.getMessage());
            }
        }

        // Step 3: Run benchmarks
        log.info("--- Step 3: Running benchmarks ---");
        BenchmarkRunner runner = new BenchmarkRunner(
                config.warmUpIterations(), config.measurementIterations());
        MongoQueryExecutor mongoExecutor = new MongoQueryExecutor();
        OracleJdbcQueryExecutor oracleExecutor = new OracleJdbcQueryExecutor();
        QueryParameterGenerator paramGen = new QueryParameterGenerator(registry);
        List<BenchmarkResult> allResults = new ArrayList<>();

        for (Configuration cfg : allConfigurations()) {
            for (QueryDefinition query : QueryDefinition.values()) {
                try {
                    if (cfg.target().usesMongoDriver()) {
                        String connStr = connectionManager.getMongoConnectionString(cfg.target());
                        String dbName = connectionManager.getDatabaseName(cfg.target());
                        try (MongoClient client = MongoClients.create(connStr)) {
                            MongoDatabase db = client.getDatabase(dbName);
                            String collName = mongoExecutor.getCollectionName(query, cfg.model());
                            MongoCollection<Document> collection = db.getCollection(collName);
                            DatabaseTarget target = cfg.target();

                            BenchmarkResult result = runner.run(
                                    query.queryName(), cfg.id(),
                                    () -> {
                                        Map<String, Object> params = paramGen.generate(query);
                                        if (query.isAggregation()) {
                                            mongoExecutor.executeAggregation(
                                                    collection, query, cfg.model(), params, target);
                                        } else {
                                            mongoExecutor.executeFind(
                                                    collection, query, cfg.model(), params);
                                        }
                                        return null;
                                    }
                            );
                            allResults.add(result);
                        }
                    }
                    // Oracle JDBC execution would go here
                } catch (Exception e) {
                    log.warn("Benchmark failed for {} on {}: {}", query.queryName(), cfg.id(), e.getMessage());
                }
            }
        }

        // Step 4: Generate report
        log.info("--- Step 4: Generating report ---");
        if (!allResults.isEmpty()) {
            try {
                HtmlReportGenerator reportGen = new HtmlReportGenerator();
                Path reportPath = Paths.get("benchmark-report.html");
                reportGen.generateToFile(allResults, reportPath);
                log.info("Report generated: {}", reportPath.toAbsolutePath());
            } catch (Exception e) {
                log.error("Failed to generate report: {}", e.getMessage(), e);
            }
        } else {
            log.warn("No benchmark results collected. Check database connectivity.");
        }

        log.info("=== Benchmark complete ===");
    }

    private static void loadMongoData(ConnectionManager connMgr, Configuration cfg,
                                       MongoSchemaManager schemaManager, DataLoader dataLoader,
                                       int batchSize, Map<String, List<Document>> embeddedData,
                                       List<Document> normalizedDocs) {
        String connStr = connMgr.getMongoConnectionString(cfg.target());
        String dbName = connMgr.getDatabaseName(cfg.target());

        try (MongoClient client = MongoClients.create(connStr)) {
            MongoDatabase db = client.getDatabase(dbName);

            // Drop existing collections
            for (String name : schemaManager.getCollectionNames(cfg.model())) {
                db.getCollection(name).drop();
            }

            if (cfg.model() == SchemaModel.EMBEDDED) {
                for (var entry : embeddedData.entrySet()) {
                    MongoCollection<Document> col = db.getCollection(entry.getKey());
                    dataLoader.loadToMongo(col, entry.getValue(), batchSize);
                }
            } else {
                MongoCollection<Document> col = db.getCollection("helix");
                dataLoader.loadToMongo(col, normalizedDocs, batchSize);
            }

            // Create indexes
            for (var idx : schemaManager.getIndexDefinitions(cfg.model())) {
                Document keys = new Document();
                for (var e : idx.keys().entrySet()) {
                    keys.append(e.getKey(), e.getValue());
                }
                db.getCollection(idx.collection()).createIndex(keys);
            }

            log.info("Data loaded and indexes created for {}", cfg.id());
        }
    }
}
