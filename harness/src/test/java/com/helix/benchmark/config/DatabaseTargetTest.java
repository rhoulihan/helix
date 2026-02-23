package com.helix.benchmark.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DatabaseTargetTest {

    @Test
    void shouldHaveSixTargets() {
        assertThat(DatabaseTarget.values()).hasSize(6);
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
    void shouldContainOracleRelational() {
        assertThat(DatabaseTarget.valueOf("ORACLE_RELATIONAL")).isNotNull();
    }

    @Test
    void oracleRelationalShouldUseJdbc() {
        assertThat(DatabaseTarget.ORACLE_RELATIONAL.usesJdbc()).isTrue();
        assertThat(DatabaseTarget.ORACLE_RELATIONAL.usesMongoDriver()).isFalse();
    }

    @Test
    void shouldContainOracleDualityView() {
        assertThat(DatabaseTarget.valueOf("ORACLE_DUALITY_VIEW")).isNotNull();
    }

    @Test
    void oracleDualityViewShouldUseJdbc() {
        assertThat(DatabaseTarget.ORACLE_DUALITY_VIEW.usesJdbc()).isTrue();
        assertThat(DatabaseTarget.ORACLE_DUALITY_VIEW.usesMongoDriver()).isFalse();
    }

    @Test
    void shouldContainOracleMongoApiDv() {
        assertThat(DatabaseTarget.valueOf("ORACLE_MONGO_API_DV")).isNotNull();
    }

    @Test
    void oracleMongoApiDvShouldUseMongoDriver() {
        assertThat(DatabaseTarget.ORACLE_MONGO_API_DV.usesMongoDriver()).isTrue();
        assertThat(DatabaseTarget.ORACLE_MONGO_API_DV.usesJdbc()).isFalse();
    }

    @Test
    void shouldHaveDisplayNames() {
        assertThat(DatabaseTarget.MONGO_NATIVE.displayName()).isEqualTo("Native MongoDB 8.2");
        assertThat(DatabaseTarget.ORACLE_JDBC.displayName()).isEqualTo("Oracle 26ai JDBC");
        assertThat(DatabaseTarget.ORACLE_MONGO_API.displayName()).isEqualTo("Oracle 26ai MongoDB API");
        assertThat(DatabaseTarget.ORACLE_RELATIONAL.displayName()).isEqualTo("Oracle 26ai Relational");
        assertThat(DatabaseTarget.ORACLE_DUALITY_VIEW.displayName()).isEqualTo("Oracle 26ai Duality View");
        assertThat(DatabaseTarget.ORACLE_MONGO_API_DV.displayName()).isEqualTo("Oracle 26ai MongoDB API (DV)");
    }
}
