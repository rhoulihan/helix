package com.helix.benchmark.query;

import com.helix.benchmark.config.SchemaModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OracleJdbcQueryExecutorTest {

    private final OracleJdbcQueryExecutor executor = new OracleJdbcQueryExecutor();

    @ParameterizedTest
    @EnumSource(QueryDefinition.class)
    void shouldGenerateSqlForAllQueriesEmbeddedModel(QueryDefinition query) {
        Map<String, Object> params = stubParams(query);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
        assertThat(sql).isNotNull();
        assertThat(sql.sql()).isNotBlank();
        assertThat(sql.sql()).containsIgnoringCase("SELECT");
    }

    @ParameterizedTest
    @EnumSource(QueryDefinition.class)
    void shouldGenerateSqlForAllQueriesNormalizedModel(QueryDefinition query) {
        Map<String, Object> params = stubParams(query);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.NORMALIZED, params);
        assertThat(sql).isNotNull();
        assertThat(sql.sql()).isNotBlank();
    }

    @Test
    void q1EmbeddedShouldQueryBookRoleInvestorTable() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("book_role_investor");
    }

    @Test
    void q1NormalizedShouldQueryHelixTableWithTypeFilter() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(
                QueryDefinition.Q1, SchemaModel.NORMALIZED, params);
        assertThat(sql.sql()).containsIgnoringCase("helix");
        assertThat(sql.sql()).containsIgnoringCase("BookRoleInvestor");
    }

    @Test
    void q1ShouldHaveJsonTableForAdvisorUnnest() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("JSON_TABLE");
    }

    @Test
    void q5ShouldUseJsonExists() {
        Map<String, Object> params = Map.of(
                "advisoryContext", "CTX001", "dataOwnerPartyRoleId", 100L,
                "personaNm", "Home Office", "minMarketValue", 1.0, "maxMarketValue", 10000.0);
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(
                QueryDefinition.Q5, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("json_exists");
    }

    @Test
    void q7ShouldFilterByFundTicker() {
        Map<String, Object> params = Map.of("pxPartyRoleId", 123L, "fundTicker", "VTI");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(
                QueryDefinition.Q7, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("fundTicker");
    }

    @Test
    void q8ShouldFilterByHierarchyPath() {
        Map<String, Object> params = Map.of("dataOwnerPartyRoleId", 100L, "partyNodePathValue", "1-Region");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(
                QueryDefinition.Q8, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("advisorHierarchy");
    }

    @Test
    void sqlQueryShouldHaveParameters() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params);
        assertThat(sql.parameters()).isNotEmpty();
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
