package com.helix.benchmark.query;

import com.helix.benchmark.benchmark.QueryDetail;
import com.helix.benchmark.config.SchemaModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class OracleJdbcQueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(OracleJdbcQueryExecutor.class);

    public record SqlQuery(String sql, List<Object> parameters) {}

    public SqlQuery buildSql(QueryDefinition query, SchemaModel model, Map<String, Object> params) {
        return switch (query) {
            case Q1 -> buildQ1Sql(params);
            case Q2 -> buildQ2Sql(params);
            case Q3 -> buildQ3Sql(params);
            case Q4 -> buildQ4Sql(params);
            case Q5 -> buildQ5Sql(params);
            case Q6 -> buildQ6Sql(params);
            case Q7 -> buildQ7Sql(params);
            case Q8 -> buildQ8Sql(params);
            case Q9 -> buildQ9Sql(params);
        };
    }

    private String tableName(QueryDefinition query) {
        return switch (query.embeddedCollection()) {
            case "bookRoleInvestor" -> "jdbc_book_role_investor";
            case "bookRoleGroup" -> "jdbc_book_role_group";
            case "account" -> "jdbc_account";
            case "advisor" -> "jdbc_advisor";
            default -> throw new IllegalArgumentException("Unknown collection: " + query.embeddedCollection());
        };
    }

    // Common investor columns for Q1-Q4 aggregation queries
    void appendJdbcInvestorColumns(StringBuilder sql) {
        sql.append("  SELECT json_value(b.data, '$._id') AS \"_id\",\n");
        sql.append("         json_value(b.data, '$.partyRoleId' RETURNING NUMBER) AS \"partyRoleId\",\n");
        sql.append("         json_value(b.data, '$.partyId' RETURNING NUMBER) AS \"partyId\",\n");
        sql.append("         json_value(b.data, '$.ssnTin') AS \"ssnTin\",\n");
        sql.append("         json_value(b.data, '$.finInstId' RETURNING NUMBER) AS \"finInstId\",\n");
        sql.append("         json_value(b.data, '$.investorType') AS \"investorType\",\n");
        sql.append("         json_value(b.data, '$.investorLastName') AS \"investorLastName\",\n");
        sql.append("         json_value(b.data, '$.investorFirstName') AS \"investorFirstName\",\n");
        sql.append("         json_value(b.data, '$.investorMiddleName') AS \"investorMiddleName\",\n");
        sql.append("         json_value(b.data, '$.investorFullName') AS \"investorFullName\",\n");
        sql.append("         json_value(b.data, '$.investorCity') AS \"investorCity\",\n");
        sql.append("         json_value(b.data, '$.investorState') AS \"investorState\",\n");
        sql.append("         json_value(b.data, '$.investorZipCode') AS \"investorZipCode\",\n");
        sql.append("         json_value(b.data, '$.clientAccess') AS \"clientAccess\",\n");
        sql.append("         json_value(b.data, '$.ETLUpdateTS') AS \"ETLUpdateTS\",\n");
        sql.append("         jt.advisor_id AS \"advisorId\",\n");
        sql.append("         jt.viewable_mv AS \"viewableMarketValue\",\n");
        sql.append("         jt.viewable_accts AS \"noOfViewableAccts\",\n");
        sql.append("         COUNT(*) OVER () AS \"totalCount\"\n");
    }

    void appendJdbcAdvisorJsonTable(StringBuilder sql, String table) {
        sql.append("  FROM ").append(table).append(" b,\n");
        sql.append("       JSON_TABLE(b.data, '$.advisors[*]' COLUMNS (\n");
        sql.append("           advisor_id    VARCHAR2(30) PATH '$.advisorId',\n");
        sql.append("           viewable_mv   NUMBER       PATH '$.viewableMarketValue',\n");
        sql.append("           viewable_accts NUMBER      PATH '$.noOfViewableAccts'\n");
        sql.append("       )) jt\n");
    }

    // --- Q1: BookRoleInvestor - Investor list by advisor ---
    private SqlQuery buildQ1Sql(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        String table = tableName(QueryDefinition.Q1);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        appendJdbcInvestorColumns(sql);
        appendJdbcAdvisorJsonTable(sql, table);
        sql.append("  WHERE json_exists(b.data, '$.advisors[*]?(@.advisorId == $aid)' PASSING ? AS \"aid\")\n");
        p.add(advisorId);
        sql.append("    AND json_value(b.data, '$.investorType') = 'Client'\n");
        sql.append("    AND json_value(b.data, '$.viewableSource') = 'Y'\n");
        sql.append("    AND jt.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND jt.viewable_accts >= 1\n");
        sql.append("  ORDER BY jt.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(")");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q2: BookRoleInvestor - Search by name with regex ---
    private SqlQuery buildQ2Sql(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        Long partyRoleId = ((Number) params.get("partyRoleId")).longValue();
        String searchTerm = (String) params.get("searchTerm");
        String table = tableName(QueryDefinition.Q2);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        appendJdbcInvestorColumns(sql);
        appendJdbcAdvisorJsonTable(sql, table);
        sql.append("  WHERE json_exists(b.data, '$.advisors[*]?(@.advisorId == $aid)' PASSING ? AS \"aid\")\n");
        p.add(advisorId);
        sql.append("    AND json_value(b.data, '$.investorType') = 'Client'\n");
        sql.append("    AND json_value(b.data, '$.viewableFlag') = 'Y'\n");
        sql.append("    AND json_value(b.data, '$.partyRoleId' RETURNING NUMBER) = ?\n");
        p.add(partyRoleId);
        sql.append("    AND UPPER(json_value(b.data, '$.investorFullName')) LIKE UPPER(?)\n");
        p.add("%" + searchTerm + "%");
        sql.append("    AND jt.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND jt.viewable_accts >= 1\n");
        sql.append("  ORDER BY jt.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(")");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q3: BookRoleInvestor - Entitlements + advisor ---
    private SqlQuery buildQ3Sql(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String table = tableName(QueryDefinition.Q3);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        appendJdbcInvestorColumns(sql);
        appendJdbcAdvisorJsonTable(sql, table);
        sql.append("  WHERE json_exists(b.data, '$.advisors[*]?(@.advisorId == $aid)' PASSING ? AS \"aid\")\n");
        p.add(advisorId);
        sql.append("    AND json_exists(b.data, '$.entitlements.advisoryContext[*]?(@ == $ctx)' PASSING ? AS \"ctx\")\n");
        p.add(advisoryContext);
        sql.append("    AND json_value(b.data, '$.entitlements.pxClient.dataOwnerPartyRoleId' RETURNING NUMBER) = ?\n");
        p.add(dataOwnerPartyRoleId);
        sql.append("    AND json_value(b.data, '$.investorType') = 'Client'\n");
        sql.append("    AND json_value(b.data, '$.viewableSource') = 'Y'\n");
        sql.append("    AND jt.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND jt.viewable_accts >= 1\n");
        sql.append("  ORDER BY jt.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(")");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q4: BookRoleInvestor - Market value range ---
    private SqlQuery buildQ4Sql(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q4);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        appendJdbcInvestorColumns(sql);
        appendJdbcAdvisorJsonTable(sql, table);
        sql.append("  WHERE json_exists(b.data, '$.advisors[*]?(@.advisorId == $aid)' PASSING ? AS \"aid\")\n");
        p.add(advisorId);
        sql.append("    AND json_value(b.data, '$.investorType') = 'Client'\n");
        sql.append("    AND json_value(b.data, '$.viewableSource') = 'Y'\n");
        sql.append("    AND jt.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND jt.viewable_accts >= 1\n");
        sql.append("    AND jt.viewable_mv >= ?\n");
        p.add(minMv);
        sql.append("    AND jt.viewable_mv <= ?\n");
        p.add(maxMv);
        sql.append("  ORDER BY jt.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(")");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q5: BookRoleGroup - Entitlements, persona, market value ---
    private SqlQuery buildQ5Sql(Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String personaNm = (String) params.get("personaNm");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q5);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data RETURNING CLOB PRETTY)\n");
        sql.append("FROM ").append(table).append(" b\n");
        sql.append("WHERE json_value(b.data, '$.dataOwnerPartyRoleId' RETURNING NUMBER) = ?\n");
        p.add(dataOwnerPartyRoleId);
        sql.append("  AND json_exists(b.data, '$.entitlements.advisoryContext[*]?(@ == $ctx)' PASSING ? AS \"ctx\")\n");
        p.add(advisoryContext);
        sql.append("  AND json_exists(b.data, '$.personaNm[*]?(@ == $pnm)' PASSING ? AS \"pnm\")\n");
        p.add(personaNm);
        sql.append("  AND NOT json_value(b.data, '$.visibleFlag') = 'N'\n");
        sql.append("  AND json_value(b.data, '$.totalViewableAccountsMarketValue' RETURNING NUMBER) BETWEEN ? AND ?\n");
        p.add(minMv);
        p.add(maxMv);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q6: BookRoleGroup - Entitlements + pxPartyRoleId ---
    private SqlQuery buildQ6Sql(Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q6);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data RETURNING CLOB PRETTY)\n");
        sql.append("FROM ").append(table).append(" b\n");
        sql.append("WHERE json_exists(b.data, '$.entitlements.advisoryContext[*]?(@ == $ctx)' PASSING ? AS \"ctx\")\n");
        p.add(advisoryContext);
        sql.append("  AND json_exists(b.data, '$.entitlements.pxPartyRoleIdList[*]?(@ == $uid)' PASSING ? AS \"uid\")\n");
        p.add(pxPartyRoleId);
        sql.append("  AND NOT json_value(b.data, '$.visibleFlag') = 'N'\n");
        sql.append("  AND json_value(b.data, '$.totalViewableAccountsMarketValue' RETURNING NUMBER) BETWEEN ? AND ?\n");
        p.add(minMv);
        p.add(maxMv);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q7: Account - Holdings fund ticker ---
    private SqlQuery buildQ7Sql(Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        String fundTicker = (String) params.get("fundTicker");
        String table = tableName(QueryDefinition.Q7);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data RETURNING CLOB PRETTY)\n");
        sql.append("FROM ").append(table).append(" a\n");
        sql.append("WHERE json_exists(a.data, '$.entitlements.pxPartyRoleIdList[*]?(@ == $uid)' PASSING ? AS \"uid\")\n");
        p.add(pxPartyRoleId);
        sql.append("  AND json_value(a.data, '$.viewableSource') = 'Y'\n");
        sql.append("  AND json_exists(a.data, '$.holdings[*]?(@.fundTicker == $ticker)' PASSING ? AS \"ticker\")\n");
        p.add(fundTicker);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q8: Advisor - Hierarchy path filter ---
    private SqlQuery buildQ8Sql(Map<String, Object> params) {
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String partyNodePathValue = (String) params.get("partyNodePathValue");
        String table = tableName(QueryDefinition.Q8);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data RETURNING CLOB PRETTY)\n");
        sql.append("FROM ").append(table).append(" a\n");
        sql.append("WHERE json_value(a.data, '$.entitlements.pxClient.dataOwnerPartyRoleId' RETURNING NUMBER) = ?\n");
        p.add(dataOwnerPartyRoleId);
        sql.append("  AND json_exists(a.data, '$.advisorHierarchy[*]?(@.partyNodePathValue == $val)' PASSING ? AS \"val\")\n");
        p.add(partyNodePathValue);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q9: Advisor - Market value range ---
    private SqlQuery buildQ9Sql(Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q9);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data RETURNING CLOB PRETTY)\n");
        sql.append("FROM ").append(table).append(" a\n");
        sql.append("WHERE json_exists(a.data, '$.entitlements.pxPartyRoleIdList[*]?(@ == $uid)' PASSING ? AS \"uid\")\n");
        p.add(pxPartyRoleId);
        sql.append("  AND json_value(a.data, '$.accountViewableMarketValue' RETURNING NUMBER) >= ?\n");
        p.add(minMv);
        sql.append("  AND json_value(a.data, '$.accountViewableMarketValue' RETURNING NUMBER) <= ?\n");
        p.add(maxMv);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Explain plan and SQL_ID capture ---

    public QueryDetail captureQueryDetail(Connection conn, QueryDefinition query,
                                           SchemaModel model, Map<String, Object> params,
                                           String configId, String ordsBaseUrl) {
        SqlQuery sqlQuery = buildSql(query, model, params);
        String displaySql = buildDisplaySql(sqlQuery);
        String explainPlan = captureExplainPlan(conn, sqlQuery);
        String sqlId = captureSqlId(conn, sqlQuery);
        return new QueryDetail(query.queryName(), configId, displaySql, explainPlan, sqlId, ordsBaseUrl);
    }

    public String buildDisplaySql(SqlQuery sqlQuery) {
        StringBuilder sb = new StringBuilder(sqlQuery.sql());
        if (!sqlQuery.parameters().isEmpty()) {
            sb.append("\n\n-- Bind values:");
            for (int i = 0; i < sqlQuery.parameters().size(); i++) {
                Object val = sqlQuery.parameters().get(i);
                sb.append("\n-- :").append(i + 1).append(" = ");
                if (val instanceof String) {
                    sb.append("'").append(val).append("'");
                } else {
                    sb.append(val);
                }
            }
        }
        return sb.toString();
    }

    public String captureExplainPlan(Connection conn, SqlQuery sqlQuery) {
        // Try actual execution stats first (GATHER_PLAN_STATISTICS + DISPLAY_CURSOR)
        String actualPlan = captureActualPlan(conn, sqlQuery);
        if (actualPlan != null) {
            return actualPlan;
        }

        // Fall back to static EXPLAIN PLAN
        return captureStaticPlan(conn, sqlQuery);
    }

    private String captureActualPlan(Connection conn, SqlQuery sqlQuery) {
        try {
            // Execute with GATHER_PLAN_STATISTICS hint to collect actual row counts
            String hintedSql = addGatherStatsHint(sqlQuery.sql());
            try (var ps = conn.prepareStatement(hintedSql)) {
                for (int i = 0; i < sqlQuery.parameters().size(); i++) {
                    ps.setObject(i + 1, sqlQuery.parameters().get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        // drain results
                    }
                }
            }

            // Find the SQL_ID of the hinted query
            String sqlPrefix = hintedSql.length() > 100
                    ? hintedSql.substring(0, 100) : hintedSql;
            String sqlId = null;
            String childNumber = null;
            try (var ps = conn.prepareStatement(
                    "SELECT sql_id, child_number FROM V$SQL WHERE sql_text LIKE ? ORDER BY last_active_time DESC FETCH FIRST 1 ROW ONLY")) {
                ps.setString(1, sqlPrefix + "%");
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        sqlId = rs.getString(1);
                        childNumber = rs.getString(2);
                    }
                }
            }

            if (sqlId == null) {
                return null;
            }

            // Use DISPLAY_CURSOR with ALLSTATS LAST to show actual vs estimated rows
            StringBuilder plan = new StringBuilder();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR('" +
                                 sqlId + "', " + childNumber + ", 'ALLSTATS LAST'))")) {
                while (rs.next()) {
                    plan.append(rs.getString(1)).append("\n");
                }
            }

            String result = plan.toString();
            if (result.contains("could not find") || result.isBlank()) {
                return null;
            }
            return result;
        } catch (Exception e) {
            log.debug("Could not capture actual plan (falling back to static): {}", e.getMessage());
            return null;
        }
    }

    private String captureStaticPlan(Connection conn, SqlQuery sqlQuery) {
        String stmtId = UUID.randomUUID().toString().substring(0, 30);
        try {
            String sqlWithLiterals = substituteLiterals(sqlQuery);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("EXPLAIN PLAN SET STATEMENT_ID = '" + stmtId + "' FOR " + sqlWithLiterals);

                StringBuilder plan = new StringBuilder();
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', '" + stmtId + "', 'ALL'))")) {
                    while (rs.next()) {
                        plan.append(rs.getString(1)).append("\n");
                    }
                }

                // Clean up plan table entry
                try {
                    stmt.execute("DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = '" + stmtId + "'");
                    conn.commit();
                } catch (Exception ignored) {}

                return plan.toString();
            }
        } catch (Exception e) {
            log.warn("Failed to capture explain plan: {}", e.getMessage());
            return "Explain plan unavailable: " + e.getMessage();
        }
    }

    static String addGatherStatsHint(String sql) {
        // Insert /*+ GATHER_PLAN_STATISTICS */ after the first SELECT
        String upper = sql.stripLeading().toUpperCase();
        if (upper.startsWith("SELECT")) {
            int idx = sql.stripLeading().indexOf("SELECT") + "SELECT".length();
            int leadingSpaces = sql.length() - sql.stripLeading().length();
            return sql.substring(0, leadingSpaces + idx)
                    + " /*+ GATHER_PLAN_STATISTICS */"
                    + sql.substring(leadingSpaces + idx);
        }
        return sql;
    }

    public String substituteLiterals(SqlQuery sqlQuery) {
        String sql = sqlQuery.sql();
        List<Object> params = sqlQuery.parameters();
        StringBuilder result = new StringBuilder();
        int paramIndex = 0;
        boolean inSingleQuote = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (c == '\'' && !inSingleQuote) {
                inSingleQuote = true;
                result.append(c);
            } else if (c == '\'' && inSingleQuote) {
                // Check for escaped quote ''
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    result.append("''");
                    i++;
                } else {
                    inSingleQuote = false;
                    result.append(c);
                }
            } else if (c == '?' && !inSingleQuote) {
                if (paramIndex < params.size()) {
                    Object val = params.get(paramIndex);
                    if (val instanceof String) {
                        result.append("'").append(((String) val).replace("'", "''")).append("'");
                    } else {
                        result.append(val);
                    }
                    paramIndex++;
                } else {
                    result.append(c);
                }
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    public String captureSqlId(Connection conn, SqlQuery sqlQuery) {
        try {
            // Execute the query once so it appears in V$SQL
            try (var ps = conn.prepareStatement(sqlQuery.sql())) {
                for (int i = 0; i < sqlQuery.parameters().size(); i++) {
                    ps.setObject(i + 1, sqlQuery.parameters().get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        // drain results
                    }
                }
            }

            // Look up sql_id from V$SQL using first 100 chars of SQL text
            String sqlPrefix = sqlQuery.sql().length() > 100
                    ? sqlQuery.sql().substring(0, 100)
                    : sqlQuery.sql();
            try (var ps = conn.prepareStatement(
                    "SELECT sql_id FROM V$SQL WHERE sql_text LIKE ? AND ROWNUM = 1")) {
                ps.setString(1, sqlPrefix + "%");
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to capture SQL_ID (V$SQL access may be denied): {}", e.getMessage());
        }
        return null;
    }
}
