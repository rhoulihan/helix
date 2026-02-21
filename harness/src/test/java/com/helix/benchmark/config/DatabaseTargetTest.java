package com.helix.benchmark.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DatabaseTargetTest {

    @Test
    void shouldHaveThreeTargets() {
        assertThat(DatabaseTarget.values()).hasSize(3);
    }

    @Test
    void shouldContainMongoNative() {
        assertThat(DatabaseTarget.valueOf("MONGO_NATIVE")).isNotNull();
    }

    @Test
    void shouldContainOracleJdbc() {
        assertThat(DatabaseTarget.valueOf("ORACLE_JDBC")).isNotNull();
    }

    @Test
    void shouldContainOracleMongoApi() {
        assertThat(DatabaseTarget.valueOf("ORACLE_MONGO_API")).isNotNull();
    }

    @Test
    void mongoNativeShouldUseMongoDriver() {
        assertThat(DatabaseTarget.MONGO_NATIVE.usesMongoDriver()).isTrue();
        assertThat(DatabaseTarget.MONGO_NATIVE.usesJdbc()).isFalse();
    }

    @Test
    void oracleJdbcShouldUseJdbc() {
        assertThat(DatabaseTarget.ORACLE_JDBC.usesJdbc()).isTrue();
        assertThat(DatabaseTarget.ORACLE_JDBC.usesMongoDriver()).isFalse();
    }

    @Test
    void oracleMongoApiShouldUseMongoDriver() {
        assertThat(DatabaseTarget.ORACLE_MONGO_API.usesMongoDriver()).isTrue();
        assertThat(DatabaseTarget.ORACLE_MONGO_API.usesJdbc()).isFalse();
    }

    @Test
    void shouldHaveDisplayNames() {
        assertThat(DatabaseTarget.MONGO_NATIVE.displayName()).isEqualTo("Native MongoDB 8.2");
        assertThat(DatabaseTarget.ORACLE_JDBC.displayName()).isEqualTo("Oracle 26ai JDBC");
        assertThat(DatabaseTarget.ORACLE_MONGO_API.displayName()).isEqualTo("Oracle 26ai MongoDB API");
    }
}
