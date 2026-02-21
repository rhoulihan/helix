package com.helix.benchmark.connection;

import com.helix.benchmark.config.BenchmarkConfig;
import com.helix.benchmark.config.DatabaseTarget;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionManagerTest {

    private BenchmarkConfig config;

    @BeforeEach
    void setUp() {
        config = BenchmarkConfig.load(
                getClass().getResourceAsStream("/test-config.yaml"));
    }

    @Test
    void shouldCreateInstanceFromConfig() {
        ConnectionManager cm = new ConnectionManager(config);
        assertThat(cm).isNotNull();
    }

    @Test
    void shouldReturnMongoConnectionStringForNative() {
        ConnectionManager cm = new ConnectionManager(config);
        assertThat(cm.getMongoConnectionString(DatabaseTarget.MONGO_NATIVE))
                .isEqualTo("mongodb://localhost:27017");
    }

    @Test
    void shouldReturnMongoConnectionStringForOracleApi() {
        ConnectionManager cm = new ConnectionManager(config);
        assertThat(cm.getMongoConnectionString(DatabaseTarget.ORACLE_MONGO_API))
                .isEqualTo("mongodb://localhost:27018");
    }

    @Test
    void shouldReturnDatabaseName() {
        ConnectionManager cm = new ConnectionManager(config);
        assertThat(cm.getDatabaseName(DatabaseTarget.MONGO_NATIVE)).isEqualTo("helix");
    }

    @Test
    void shouldThrowForMongoStringOnJdbcTarget() {
        ConnectionManager cm = new ConnectionManager(config);
        assertThatThrownBy(() -> cm.getMongoConnectionString(DatabaseTarget.ORACLE_JDBC))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldReturnJdbcUrl() {
        ConnectionManager cm = new ConnectionManager(config);
        assertThat(cm.getJdbcUrl()).isEqualTo("jdbc:oracle:thin:@localhost:1521/helix");
    }

    @Test
    void shouldReturnJdbcCredentials() {
        ConnectionManager cm = new ConnectionManager(config);
        assertThat(cm.getJdbcUsername()).isEqualTo("ADMIN");
        assertThat(cm.getJdbcPassword()).isEqualTo("Welcome_12345!");
    }

    @Test
    void shouldReturnMaxPoolSize() {
        ConnectionManager cm = new ConnectionManager(config);
        assertThat(cm.getJdbcMaxPoolSize()).isEqualTo(5);
    }
}
