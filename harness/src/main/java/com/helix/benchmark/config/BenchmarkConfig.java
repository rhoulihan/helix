package com.helix.benchmark.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;

public class BenchmarkConfig {
    private final JsonNode root;

    private BenchmarkConfig(JsonNode root) {
        this.root = root;
    }

    public static BenchmarkConfig load(InputStream inputStream) {
        if (inputStream == null) {
            throw new IllegalArgumentException("Config input stream must not be null");
        }
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            JsonNode root = mapper.readTree(inputStream);
            return new BenchmarkConfig(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load benchmark config", e);
        }
    }

    // Benchmark settings
    public int warmUpIterations() {
        return root.path("benchmark").path("warmUpIterations").asInt(50);
    }

    public int measurementIterations() {
        return root.path("benchmark").path("measurementIterations").asInt(200);
    }

    public int batchSize() {
        return root.path("benchmark").path("batchSize").asInt(1000);
    }

    public int jdbcBatchSize() {
        return root.path("benchmark").path("jdbcBatchSize").asInt(500);
    }

    // Data generation settings
    public int advisorCount() {
        return root.path("dataGeneration").path("advisorCount").asInt(1000);
    }

    public int accountCount() {
        return root.path("dataGeneration").path("accountCount").asInt(100000);
    }

    public int bookRoleGroupCount() {
        return root.path("dataGeneration").path("bookRoleGroupCount").asInt(30000);
    }

    public int bookRoleInvestorCount() {
        return root.path("dataGeneration").path("bookRoleInvestorCount").asInt(150000);
    }

    public int advisoryContextPoolSize() {
        return root.path("dataGeneration").path("advisoryContextPoolSize").asInt(5000);
    }

    public int partyRoleIdPoolSize() {
        return root.path("dataGeneration").path("partyRoleIdPoolSize").asInt(10000);
    }

    public int finInstIdPoolSize() {
        return root.path("dataGeneration").path("finInstIdPoolSize").asInt(50);
    }

    public double targetSizeGb() {
        return root.path("dataGeneration").path("targetSizeGb").asDouble(1.5);
    }

    // Connection settings
    public String mongoNativeUri() {
        return root.path("connections").path("mongoNative").path("uri").asText();
    }

    public String mongoNativeDatabase() {
        return root.path("connections").path("mongoNative").path("database").asText();
    }

    public String oracleJdbcUrl() {
        return root.path("connections").path("oracleJdbc").path("url").asText();
    }

    public String oracleJdbcUsername() {
        return root.path("connections").path("oracleJdbc").path("username").asText();
    }

    public String oracleJdbcPassword() {
        return root.path("connections").path("oracleJdbc").path("password").asText();
    }

    public int oracleJdbcMaxPoolSize() {
        return root.path("connections").path("oracleJdbc").path("maxPoolSize").asInt(10);
    }

    public String oracleMongoApiUri() {
        return root.path("connections").path("oracleMongoApi").path("uri").asText();
    }

    public String oracleMongoApiDatabase() {
        return root.path("connections").path("oracleMongoApi").path("database").asText("helix");
    }

    public String configurationId(DatabaseTarget target, SchemaModel model) {
        return target.name() + "_" + model.name();
    }
}
