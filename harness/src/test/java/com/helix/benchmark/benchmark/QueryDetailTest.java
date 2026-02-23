package com.helix.benchmark.benchmark;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryDetailTest {

    @Test
    void shouldReturnActiveSqlMonitorUrlWhenSqlIdAndOrdsPresent() {
        QueryDetail detail = new QueryDetail("Q1", "ORACLE_JDBC_EMBEDDED",
                "SELECT ...", "Plan ...", "abc123def",
                "https://localhost:8443/ords/admin");

        assertThat(detail.activeSqlMonitorUrl())
                .isEqualTo("https://localhost:8443/ords/admin/_/sql/abc123def");
    }

    @Test
    void shouldReturnNullWhenSqlIdIsNull() {
        QueryDetail detail = new QueryDetail("Q1", "MONGO_NATIVE_EMBEDDED",
                "[ pipeline ]", "{ explain }", null, null);

        assertThat(detail.activeSqlMonitorUrl()).isNull();
    }

    @Test
    void shouldReturnNullWhenOrdsBaseUrlIsNull() {
        QueryDetail detail = new QueryDetail("Q1", "ORACLE_JDBC_EMBEDDED",
                "SELECT ...", "Plan ...", "abc123def", null);

        assertThat(detail.activeSqlMonitorUrl()).isNull();
    }

    @Test
    void shouldStoreAllFields() {
        QueryDetail detail = new QueryDetail("Q5", "ORACLE_JDBC_NORMALIZED",
                "SELECT * FROM ...", "Plan Table Output",
                "sql123", "https://host/ords/admin");

        assertThat(detail.queryName()).isEqualTo("Q5");
        assertThat(detail.configurationId()).isEqualTo("ORACLE_JDBC_NORMALIZED");
        assertThat(detail.queryText()).isEqualTo("SELECT * FROM ...");
        assertThat(detail.explainPlan()).isEqualTo("Plan Table Output");
        assertThat(detail.sqlId()).isEqualTo("sql123");
        assertThat(detail.ordsBaseUrl()).isEqualTo("https://host/ords/admin");
    }
}
