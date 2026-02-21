package com.helix.benchmark.connection;

import com.helix.benchmark.config.BenchmarkConfig;
import com.helix.benchmark.config.DatabaseTarget;

public class ConnectionManager {
    private final BenchmarkConfig config;

    public ConnectionManager(BenchmarkConfig config) {
        this.config = config;
    }

    public String getMongoConnectionString(DatabaseTarget target) {
        return switch (target) {
            case MONGO_NATIVE -> config.mongoNativeUri();
            case ORACLE_MONGO_API -> config.oracleMongoApiUri();
            case ORACLE_JDBC -> throw new IllegalArgumentException(
                    "ORACLE_JDBC does not use MongoDB driver");
        };
    }

    public String getDatabaseName(DatabaseTarget target) {
        return switch (target) {
            case MONGO_NATIVE -> config.mongoNativeDatabase();
            case ORACLE_MONGO_API -> config.oracleMongoApiDatabase();
            case ORACLE_JDBC -> throw new IllegalArgumentException(
                    "ORACLE_JDBC does not use MongoDB database name");
        };
    }

    public String getJdbcUrl() {
        return config.oracleJdbcUrl();
    }

    public String getJdbcUsername() {
        return config.oracleJdbcUsername();
    }

    public String getJdbcPassword() {
        return config.oracleJdbcPassword();
    }

    public int getJdbcMaxPoolSize() {
        return config.oracleJdbcMaxPoolSize();
    }
}
