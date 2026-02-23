package com.helix.benchmark.query;

import com.helix.benchmark.config.SchemaModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OracleDualityViewQueryExecutorTest {

    private final OracleDualityViewQueryExecutor executor = new OracleDualityViewQueryExecutor();

    @ParameterizedTest
    @EnumSource(QueryDefinition.class)
    void shouldGenerateSqlForAllQueries(QueryDefinition query) {
        Map<String, Object> params = stubParams(query);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
        assertThat(sql).isNotNull();
        assertThat(sql.sql()).isNotBlank();
        assertThat(sql.sql()).containsIgnoringCase("SELECT");
    }

    @ParameterizedTest
    @EnumSource(QueryDefinition.class)
    void shouldUseDualityViewTableNames(QueryDefinition query) {
        Map<String, Object> params = stubParams(query);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).contains("dv_");
        assertThat(sql.sql()).doesNotContain("jdbc_");
        assertThat(sql.sql()).doesNotContain("rel_");
    }

    @ParameterizedTest
    @EnumSource(QueryDefinition.class)
    void shouldUseJsonFunctions(QueryDefinition query) {
        Map<String, Object> params = stubParams(query);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
        // Duality views use JSON functions (unlike relational which uses JOINs)
        assertThat(sql.sql()).satisfiesAnyOf(
                s -> assertThat(s).containsIgnoringCase("json_exists"),
                s -> assertThat(s).containsIgnoringCase("json_value"),
                s -> assertThat(s).containsIgnoringCase("JSON_TABLE")
        );
    }

    @Test
    void q1ThroughQ4ShouldUseJsonTableNotJoin() {
        for (QueryDefinition query : new QueryDefinition[]{
                QueryDefinition.Q1, QueryDefinition.Q2, QueryDefinition.Q3, QueryDefinition.Q4}) {
            Map<String, Object> params = stubParams(query);
            OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
            assertThat(sql.sql())
                    .as("Query %s should use JSON_TABLE", query)
                    .containsIgnoringCase("JSON_TABLE");
            assertThat(sql.sql())
                    .as("Query %s should not use JOIN", query)
                    .doesNotContainIgnoringCase("JOIN rel_");
        }
    }

    @Test
    void q3ShouldUseAdvisoryContextsPath() {
        Map<String, Object> params = Map.of(
                "advisorId", "ADV001", "advisoryContext", "CTX001", "dataOwnerPartyRoleId", 100L);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q3, SchemaModel.EMBEDDED, params);
        // Duality view uses flattened path (not $.entitlements.advisoryContext)
        assertThat(sql.sql()).contains("$.advisoryContexts[*]");
        assertThat(sql.sql()).contains("$.entDataOwnerPartyRoleId");
        assertThat(sql.sql()).doesNotContain("$.entitlements.");
    }

    @Test
    void q5ShouldUseAdvisoryContextsAndPersonaNmsPaths() {
        Map<String, Object> params = Map.of(
                "advisoryContext", "CTX001", "dataOwnerPartyRoleId", 100L,
                "personaNm", "Home Office", "minMarketValue", 1.0, "maxMarketValue", 10000.0);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q5, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).contains("$.advisoryContexts[*]");
        assertThat(sql.sql()).contains("$.personaNms[*]");
        assertThat(sql.sql()).contains("$.dataOwnerPartyRoleId");
    }

    @Test
    void q6ShouldUsePartyRoleIdsPaths() {
        Map<String, Object> params = Map.of("advisoryContext", "CTX001", "pxPartyRoleId", 123L,
                "minMarketValue", 0.0, "maxMarketValue", 100.0);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q6, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).contains("$.partyRoleIds[*]");
        assertThat(sql.sql()).contains("$.advisoryContexts[*]");
    }

    @Test
    void q7ShouldUsePartyRoleIdsAndHoldingsPaths() {
        Map<String, Object> params = Map.of("pxPartyRoleId", 123L, "fundTicker", "VTI");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q7, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).contains("$.partyRoleIds[*]");
        assertThat(sql.sql()).contains("$.holdings[*]");
        assertThat(sql.sql()).contains("dv_account");
    }

    @Test
    void q8ShouldUseFlattenedEntDataOwnerPath() {
        Map<String, Object> params = Map.of("dataOwnerPartyRoleId", 100L, "partyNodePathValue", "1-Region");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q8, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).contains("$.entDataOwnerPartyRoleId");
        assertThat(sql.sql()).contains("$.advisorHierarchy[*]");
        assertThat(sql.sql()).doesNotContain("$.entitlements.");
    }

    @Test
    void q9ShouldUsePartyRoleIdsPath() {
        Map<String, Object> params = Map.of("pxPartyRoleId", 123L, "minMarketValue", 0.0, "maxMarketValue", 40000000.0);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q9, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).contains("$.partyRoleIds[*]");
        assertThat(sql.sql()).contains("$.accountViewableMarketValue");
        assertThat(sql.sql()).contains("dv_advisor");
    }

    @Test
    void sqlQueryShouldHaveParameters() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q1, SchemaModel.EMBEDDED, params);
        assertThat(sql.parameters()).isNotEmpty();
    }

    @Test
    void shouldInheritBuildDisplaySql() {
        OracleJdbcQueryExecutor.SqlQuery query = new OracleJdbcQueryExecutor.SqlQuery(
                "SELECT * FROM dv_book_role_investor WHERE id = ?",
                java.util.List.of("test-id"));
        String result = executor.buildDisplaySql(query);
        assertThat(result).contains("-- Bind values:");
        assertThat(result).contains("'test-id'");
    }

    @Test
    void shouldInheritSubstituteLiterals() {
        OracleJdbcQueryExecutor.SqlQuery query = new OracleJdbcQueryExecutor.SqlQuery(
                "SELECT * FROM t WHERE a = ? AND b = ?",
                java.util.List.of("hello", 42));
        String result = executor.substituteLiterals(query);
        assertThat(result).isEqualTo("SELECT * FROM t WHERE a = 'hello' AND b = 42");
    }

    private Map<String, Object> stubParams(QueryDefinition query) {
        return switch (query) {
            case Q1 -> Map.of("advisorId", "ADV001");
            case Q2 -> Map.of("advisorId", "ADV001", "partyRoleId", 123L, "searchTerm", "smith");
            case Q3 -> Map.of("advisorId", "ADV001", "advisoryContext", "CTX001", "dataOwnerPartyRoleId", 100L);
            case Q4 -> Map.of("advisorId", "ADV001", "minMarketValue", 1.0, "maxMarketValue", 1000.0);
            case Q5 -> Map.of("advisoryContext", "CTX001", "dataOwnerPartyRoleId", 100L,
                    "personaNm", "Home Office", "minMarketValue", 1.0, "maxMarketValue", 10000.0);
            case Q6 -> Map.of("advisoryContext", "CTX001", "pxPartyRoleId", 123L,
                    "minMarketValue", 0.0, "maxMarketValue", 100.0);
            case Q7 -> Map.of("pxPartyRoleId", 123L, "fundTicker", "VTI");
            case Q8 -> Map.of("dataOwnerPartyRoleId", 100L, "partyNodePathValue", "1-Region");
            case Q9 -> Map.of("pxPartyRoleId", 123L, "minMarketValue", 0.0, "maxMarketValue", 40000000.0);
        };
    }
}
