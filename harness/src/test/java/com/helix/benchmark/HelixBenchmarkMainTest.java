package com.helix.benchmark;

import com.helix.benchmark.config.BenchmarkConfig;
import com.helix.benchmark.config.DatabaseTarget;
import com.helix.benchmark.config.SchemaModel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HelixBenchmarkMainTest {

    @Test
    void shouldEnumerateAllSixConfigurations() {
        var configs = HelixBenchmarkMain.allConfigurations();
        assertThat(configs).hasSize(6);
    }

    @Test
    void configurationsShouldCoverAllTargetsAndModels() {
        var configs = HelixBenchmarkMain.allConfigurations();

        for (DatabaseTarget target : DatabaseTarget.values()) {
            for (SchemaModel model : SchemaModel.values()) {
                assertThat(configs).anyMatch(c ->
                        c.target() == target && c.model() == model);
            }
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
}
