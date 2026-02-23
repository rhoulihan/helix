package com.helix.benchmark.query;

import com.helix.benchmark.config.SchemaModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OracleJdbcQueryExecutorTest {

    private final OracleJdbcQueryExecutor executor = new OracleJdbcQueryExecutor();

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
    void q1ShouldQueryBookRoleInvestorTable() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("jdbc_book_role_investor");
    }

    @Test
    void q1ShouldHaveJsonTableForAdvisorUnnest() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params);
        assertThat(sql.sql()).containsIgnoringCase("JSON_TABLE");
    }

    @Test
    void q1ThroughQ4ShouldUseJsonExistsForAdvisorPreFilter() {
        for (QueryDefinition query : new QueryDefinition[]{
                QueryDefinition.Q1, QueryDefinition.Q2, QueryDefinition.Q3, QueryDefinition.Q4}) {
            Map<String, Object> params = stubParams(query);
            OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
            assertThat(sql.sql())
                    .as("Query %s should use json_exists to pre-filter by advisorId", query)
                    .containsIgnoringCase("json_exists(b.data, '$.advisors[*]?(@.advisorId == $aid)'");
        }
    }

    @Test
    void q1ThroughQ4ShouldNotUseJsonSerializeWithWildcard() {
        for (QueryDefinition query : new QueryDefinition[]{
                QueryDefinition.Q1, QueryDefinition.Q2, QueryDefinition.Q3, QueryDefinition.Q4}) {
            Map<String, Object> params = stubParams(query);
            OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
            assertThat(sql.sql())
                    .as("Query %s should not use json_serialize with wildcard", query)
                    .doesNotContain("json_serialize(matched.*");
        }
    }

    @Test
    void q1ThroughQ4ShouldUseSelectStarFromSubquery() {
        for (QueryDefinition query : new QueryDefinition[]{
                QueryDefinition.Q1, QueryDefinition.Q2, QueryDefinition.Q3, QueryDefinition.Q4}) {
            Map<String, Object> params = stubParams(query);
            OracleJdbcQueryExecutor.SqlQuery sql = executor.buildSql(query, SchemaModel.EMBEDDED, params);
            assertThat(sql.sql())
                    .as("Query %s should SELECT * FROM subquery", query)
                    .containsIgnoringCase("SELECT * FROM");
        }
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

    @Test
    void substituteLiteralsShouldReplaceBindPlaceholders() {
        OracleJdbcQueryExecutor.SqlQuery query = new OracleJdbcQueryExecutor.SqlQuery(
                "SELECT * FROM t WHERE a = ? AND b = ?",
                List.of("hello", 42));
        String result = executor.substituteLiterals(query);
        assertThat(result).isEqualTo("SELECT * FROM t WHERE a = 'hello' AND b = 42");
    }

    @Test
    void substituteLiteralsShouldPreserveJsonPathQuestionMark() {
        // JSON path syntax: '$.advisors[*]?(@.advisorId == $aid)' uses ? inside quotes
        OracleJdbcQueryExecutor.SqlQuery query = new OracleJdbcQueryExecutor.SqlQuery(
                "WHERE json_exists(b.data, '$.advisors[*]?(@.advisorId == $aid)' PASSING ? AS \"aid\")",
                List.of("ADV001"));
        String result = executor.substituteLiterals(query);
        // The ? inside single quotes should be preserved, only the bind ? replaced
        assertThat(result).contains("?(@.advisorId == $aid)");
        assertThat(result).contains("'ADV001'");
        assertThat(result).doesNotContain("PASSING ?");
    }

    @Test
    void substituteLiteralsShouldEscapeSingleQuotesInStrings() {
        OracleJdbcQueryExecutor.SqlQuery query = new OracleJdbcQueryExecutor.SqlQuery(
                "SELECT * FROM t WHERE a = ?",
                List.of("it's"));
        String result = executor.substituteLiterals(query);
        assertThat(result).isEqualTo("SELECT * FROM t WHERE a = 'it''s'");
    }

    @Test
    void buildDisplaySqlShouldAppendBindValues() {
        OracleJdbcQueryExecutor.SqlQuery query = new OracleJdbcQueryExecutor.SqlQuery(
                "SELECT * FROM t WHERE a = ? AND b = ?",
                List.of("hello", 42));
        String result = executor.buildDisplaySql(query);
        assertThat(result).contains("SELECT * FROM t WHERE a = ? AND b = ?");
        assertThat(result).contains("-- Bind values:");
        assertThat(result).contains("-- :1 = 'hello'");
        assertThat(result).contains("-- :2 = 42");
    }

    @Test
    void buildDisplaySqlShouldHandleEmptyParams() {
        OracleJdbcQueryExecutor.SqlQuery query = new OracleJdbcQueryExecutor.SqlQuery(
                "SELECT 1 FROM DUAL", List.of());
        String result = executor.buildDisplaySql(query);
        assertThat(result).isEqualTo("SELECT 1 FROM DUAL");
        assertThat(result).doesNotContain("Bind values");
    }

    @Test
    void addGatherStatsHintShouldInsertAfterSelect() {
        String sql = "SELECT * FROM t WHERE a = 1";
        String result = OracleJdbcQueryExecutor.addGatherStatsHint(sql);
        assertThat(result).isEqualTo("SELECT /*+ GATHER_PLAN_STATISTICS */ * FROM t WHERE a = 1");
    }

    @Test
    void addGatherStatsHintShouldHandleSubqueryWrapped() {
        String sql = "SELECT * FROM (\n  SELECT jt.* FROM t b, JSON_TABLE(...) jt\n)";
        String result = OracleJdbcQueryExecutor.addGatherStatsHint(sql);
        assertThat(result).startsWith("SELECT /*+ GATHER_PLAN_STATISTICS */ * FROM");
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
