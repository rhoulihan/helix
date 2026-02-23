package com.helix.benchmark.query;

import com.helix.benchmark.config.SchemaModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OracleRelationalQueryExecutorTest {

    private final OracleRelationalQueryExecutor executor = new OracleRelationalQueryExecutor();

    @ParameterizedTest
    @EnumSource(QueryDefinition.class)
    void shouldGenerateSqlForAllQueries(QueryDefinition query) {
        Map<String, Object> params = stubParams(query);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
        assertThat(sql).isNotNull();
        assertThat(sql.sql()).isNotBlank();
        assertThat(sql.sql()).containsIgnoringCase("SELECT");
    }

    @Test
    void q1ThroughQ4ShouldNotContainJsonFunctions() {
        for (QueryDefinition query : new QueryDefinition[]{
                QueryDefinition.Q1, QueryDefinition.Q2, QueryDefinition.Q3, QueryDefinition.Q4}) {
            Map<String, Object> params = stubParams(query);
            OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
            assertThat(sql.sql())
                    .as("Query %s should not use JSON functions", query)
                    .doesNotContainIgnoringCase("json_table")
                    .doesNotContainIgnoringCase("json_exists")
                    .doesNotContainIgnoringCase("json_value")
                    .doesNotContainIgnoringCase("json_serialize");
        }
    }

    @Test
    void q5ThroughQ9ShouldUseJsonObjectForDocumentReconstruction() {
        for (QueryDefinition query : new QueryDefinition[]{
                QueryDefinition.Q5, QueryDefinition.Q6, QueryDefinition.Q7, QueryDefinition.Q8, QueryDefinition.Q9}) {
            Map<String, Object> params = stubParams(query);
            OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
            assertThat(sql.sql())
                    .as("Query %s should use JSON_OBJECT", query)
                    .containsIgnoringCase("JSON_OBJECT");
            assertThat(sql.sql())
                    .as("Query %s should use JSON_ARRAYAGG", query)
                    .containsIgnoringCase("JSON_ARRAYAGG");
        }
    }

    @ParameterizedTest
    @EnumSource(QueryDefinition.class)
    void shouldUseRelationalTableNames(QueryDefinition query) {
        Map<String, Object> params = stubParams(query);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
        // Should use rel_ prefix tables, not jdbc_ prefix
        assertThat(sql.sql()).doesNotContain("jdbc_");
        assertThat(sql.sql()).contains("rel_");
    }

    @Test
    void q1ShouldUseJoinOnAdvisors() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q1, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("JOIN rel_bri_advisors");
        assertThat(sql.sql()).contains("rel_book_role_investor");
    }

    @Test
    void q1ThroughQ4ShouldUseJoinNotJsonTable() {
        for (QueryDefinition query : new QueryDefinition[]{
                QueryDefinition.Q1, QueryDefinition.Q2, QueryDefinition.Q3, QueryDefinition.Q4}) {
            Map<String, Object> params = stubParams(query);
            OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
            assertThat(sql.sql())
                    .as("Query %s should use JOIN", query)
                    .containsIgnoringCase("JOIN rel_bri_advisors");
        }
    }

    @Test
    void q5ShouldUseExistsSubqueries() {
        Map<String, Object> params = Map.of(
                "advisoryContext", "CTX001", "dataOwnerPartyRoleId", 100L,
                "personaNm", "Home Office", "minMarketValue", 1.0, "maxMarketValue", 10000.0);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q5, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("EXISTS");
        assertThat(sql.sql()).contains("rel_brg_advisory_ctx");
        assertThat(sql.sql()).contains("rel_brg_persona_nm");
        assertThat(sql.sql()).contains("rel_book_role_group");
    }

    @Test
    void q6ShouldUseExistsForPartyRoleIds() {
        Map<String, Object> params = Map.of("advisoryContext", "CTX001", "pxPartyRoleId", 123L,
                "minMarketValue", 0.0, "maxMarketValue", 100.0);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q6, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("EXISTS");
        assertThat(sql.sql()).contains("rel_brg_party_role_ids");
    }

    @Test
    void q7ShouldUseExistsForHoldingsAndPartyRoles() {
        Map<String, Object> params = Map.of("pxPartyRoleId", 123L, "fundTicker", "VTI");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q7, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("EXISTS");
        assertThat(sql.sql()).contains("rel_acct_party_role_ids");
        assertThat(sql.sql()).contains("rel_acct_holdings");
        assertThat(sql.sql()).contains("fund_ticker");
    }

    @Test
    void q8ShouldUseExistsForHierarchy() {
        Map<String, Object> params = Map.of("dataOwnerPartyRoleId", 100L, "partyNodePathValue", "1-Region");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q8, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("EXISTS");
        assertThat(sql.sql()).contains("rel_adv_hierarchy");
        assertThat(sql.sql()).contains("party_node_path_value");
    }

    @Test
    void q9ShouldUseExistsForPartyRoleIds() {
        Map<String, Object> params = Map.of("pxPartyRoleId", 123L, "minMarketValue", 0.0, "maxMarketValue", 40000000.0);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(QueryDefinition.Q9, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("EXISTS");
        assertThat(sql.sql()).contains("rel_adv_party_role_ids");
        assertThat(sql.sql()).contains("account_viewable_market_value");
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
                "SELECT * FROM rel_book_role_investor WHERE id = ?",
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
