package com.helix.benchmark;

import com.helix.benchmark.config.BenchmarkConfig;
import com.helix.benchmark.config.DatabaseTarget;
import com.helix.benchmark.config.SchemaModel;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class HelixBenchmarkMainTest {

    @Test
    void shouldEnumerateAllSixConfigurations() {
        var configs = HelixBenchmarkMain.allConfigurations();
        assertThat(configs).hasSize(6);
    }

    @Test
    void configurationsShouldCoverAllTargets() {
        var configs = HelixBenchmarkMain.allConfigurations();

        for (DatabaseTarget target : DatabaseTarget.values()) {
            assertThat(configs).anyMatch(c ->
                    c.target() == target && c.model() == SchemaModel.EMBEDDED);
        }
    }

    @Test
    void configurationShouldHaveUniqueId() {
        var configs = HelixBenchmarkMain.allConfigurations();
        var ids = configs.stream().map(HelixBenchmarkMain.Configuration::id).toList();
        assertThat(ids).doesNotHaveDuplicates();
    }

    @Test
    void shouldLoadDefaultConfig() {
        BenchmarkConfig config = HelixBenchmarkMain.loadConfig(null);
        assertThat(config).isNotNull();
    }

    @Test
    void configurationsContainJdbcTargets() {
        var configs = HelixBenchmarkMain.allConfigurations();
        assertThat(configs).anyMatch(c -> c.target() == DatabaseTarget.ORACLE_JDBC);
        assertThat(configs).anyMatch(c -> c.target() == DatabaseTarget.ORACLE_RELATIONAL);
        assertThat(configs).anyMatch(c -> c.target() == DatabaseTarget.ORACLE_DUALITY_VIEW);
        long jdbcCount = configs.stream()
                .filter(c -> c.target().usesJdbc())
                .count();
        assertThat(jdbcCount).isEqualTo(3);
    }

    @Test
    void shouldHaveConfigureTruststoreMethod() throws Exception {
        // Verify the configureTruststore method exists and is accessible
        Method method = HelixBenchmarkMain.class.getDeclaredMethod("configureTruststore");
        assertThat(method).isNotNull();
    }

    @Test
    void configurationsContainMongoDriverTargets() {
        var configs = HelixBenchmarkMain.allConfigurations();
        long mongoDriverConfigs = configs.stream()
                .filter(c -> c.target().usesMongoDriver())
                .count();
        assertThat(mongoDriverConfigs).isEqualTo(3); // MONGO_NATIVE + ORACLE_MONGO_API + ORACLE_MONGO_API_DV
    }

    @Test
    void configurationsContainOracleMongoApiDv() {
        var configs = HelixBenchmarkMain.allConfigurations();
        assertThat(configs).anyMatch(c -> c.target() == DatabaseTarget.ORACLE_MONGO_API_DV);
    }
}
