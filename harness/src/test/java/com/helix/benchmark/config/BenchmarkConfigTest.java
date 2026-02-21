package com.helix.benchmark.config;

import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkConfigTest {

    @Test
    void shouldLoadFromYamlInputStream() {
        InputStream is = getClass().getResourceAsStream("/test-config.yaml");
        BenchmarkConfig config = BenchmarkConfig.load(is);

        assertThat(config).isNotNull();
    }

    @Test
    void shouldParseBenchmarkSettings() {
        BenchmarkConfig config = loadTestConfig();

        assertThat(config.warmUpIterations()).isEqualTo(10);
        assertThat(config.measurementIterations()).isEqualTo(50);
        assertThat(config.batchSize()).isEqualTo(500);
        assertThat(config.jdbcBatchSize()).isEqualTo(250);
    }

    @Test
    void shouldParseDataGenerationSettings() {
        BenchmarkConfig config = loadTestConfig();

        assertThat(config.advisorCount()).isEqualTo(100);
        assertThat(config.accountCount()).isEqualTo(1000);
        assertThat(config.bookRoleGroupCount()).isEqualTo(300);
        assertThat(config.bookRoleInvestorCount()).isEqualTo(1500);
    }

    @Test
    void shouldParseConnectionSettings() {
        BenchmarkConfig config = loadTestConfig();

        assertThat(config.mongoNativeUri()).isEqualTo("mongodb://localhost:27017");
        assertThat(config.mongoNativeDatabase()).isEqualTo("helix");
        assertThat(config.oracleJdbcUrl()).isEqualTo("jdbc:oracle:thin:@localhost:1521/helix");
        assertThat(config.oracleMongoApiUri()).isEqualTo("mongodb://localhost:27018");
    }

    @Test
    void shouldThrowOnNullInputStream() {
        assertThatThrownBy(() -> BenchmarkConfig.load(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldProvideConfigurationId() {
        BenchmarkConfig config = loadTestConfig();
        String configId = config.configurationId(DatabaseTarget.MONGO_NATIVE, SchemaModel.EMBEDDED);

        assertThat(configId).isEqualTo("MONGO_NATIVE_EMBEDDED");
    }

    @Test
    void shouldEnumerateAllSixConfigurations() {
        assertThat(DatabaseTarget.values()).hasSize(3);
        assertThat(SchemaModel.values()).hasSize(2);
        // 3 targets x 2 models = 6 configurations
        int totalConfigs = DatabaseTarget.values().length * SchemaModel.values().length;
        assertThat(totalConfigs).isEqualTo(6);
    }

    private BenchmarkConfig loadTestConfig() {
        InputStream is = getClass().getResourceAsStream("/test-config.yaml");
        return BenchmarkConfig.load(is);
    }
}
