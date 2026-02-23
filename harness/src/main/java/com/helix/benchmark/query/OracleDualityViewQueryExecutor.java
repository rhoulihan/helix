package com.helix.benchmark.query;

import com.helix.benchmark.config.SchemaModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OracleDualityViewQueryExecutor extends OracleJdbcQueryExecutor {

    @Override
    public SqlQuery buildSql(QueryDefinition query, SchemaModel model, Map<String, Object> params) {
        return switch (query) {
            case Q1 -> buildDvQ1(params);
            case Q2 -> buildDvQ2(params);
            case Q3 -> buildDvQ3(params);
            case Q4 -> buildDvQ4(params);
            case Q5 -> buildDvQ5(params);
            case Q6 -> buildDvQ6(params);
            case Q7 -> buildDvQ7(params);
            case Q8 -> buildDvQ8(params);
            case Q9 -> buildDvQ9(params);
        };
    }

    private String tableName(QueryDefinition query) {
        return switch (query.embeddedCollection()) {
            case "bookRoleInvestor" -> "dv_book_role_investor";
            case "bookRoleGroup" -> "dv_book_role_group";
            case "account" -> "dv_account";
            case "advisor" -> "dv_advisor";
            default -> throw new IllegalArgumentException("Unknown collection: " + query.embeddedCollection());
        };
    }

    // Common investor json_value columns for Q1-Q4
    private void appendInvestorColumns(StringBuilder sql) {
        sql.append("  SELECT json_value(b.data, '$._id') AS id,\n");
        sql.append("         json_value(b.data, '$.investorFullName') AS investor_full_name,\n");
        sql.append("         json_value(b.data, '$.investorType') AS investor_type,\n");
        sql.append("         json_value(b.data, '$.investorLastName') AS investor_last_name,\n");
        sql.append("         json_value(b.data, '$.investorFirstName') AS investor_first_name,\n");
        sql.append("         json_value(b.data, '$.investorMiddleName') AS investor_middle_name,\n");
        sql.append("         json_value(b.data, '$.investorCity') AS investor_city,\n");
        sql.append("         json_value(b.data, '$.investorState') AS investor_state,\n");
        sql.append("         json_value(b.data, '$.investorZipCode') AS investor_zip_code,\n");
        sql.append("         json_value(b.data, '$.ssnTin') AS ssn_tin,\n");
        sql.append("         json_value(b.data, '$.partyRoleId' RETURNING NUMBER) AS party_role_id,\n");
        sql.append("         json_value(b.data, '$.partyId' RETURNING NUMBER) AS party_id,\n");
        sql.append("         json_value(b.data, '$.finInstId' RETURNING NUMBER) AS fin_inst_id,\n");
        sql.append("         json_value(b.data, '$.clientAccess') AS client_access,\n");
        sql.append("         json_value(b.data, '$.ETLUpdateTS') AS etl_update_ts,\n");
        sql.append("         jt.advisor_id, jt.viewable_mv, jt.viewable_accts AS no_of_viewable_accts,\n");
        sql.append("         COUNT(*) OVER () AS total_count\n");
    }

    // Common JSON_TABLE for advisors
    private void appendAdvisorJsonTable(StringBuilder sql, String table) {
        sql.append("  FROM ").append(table).append(" b,\n");
        sql.append("       JSON_TABLE(b.data, '$.advisors[*]' COLUMNS (\n");
        sql.append("           advisor_id    VARCHAR2(30) PATH '$.advisorId',\n");
        sql.append("           viewable_mv   NUMBER       PATH '$.viewableMarketValue',\n");
        sql.append("           viewable_accts NUMBER      PATH '$.noOfViewableAccts'\n");
        sql.append("       )) jt\n");
    }

    // --- Q1: BookRoleInvestor - Investor list by advisor ---
    private SqlQuery buildDvQ1(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        String table = tableName(QueryDefinition.Q1);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        appendInvestorColumns(sql);
        appendAdvisorJsonTable(sql, table);
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
    private SqlQuery buildDvQ2(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        Long partyRoleId = ((Number) params.get("partyRoleId")).longValue();
        String searchTerm = (String) params.get("searchTerm");
        String table = tableName(QueryDefinition.Q2);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        appendInvestorColumns(sql);
        appendAdvisorJsonTable(sql, table);
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
    private SqlQuery buildDvQ3(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String table = tableName(QueryDefinition.Q3);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        appendInvestorColumns(sql);
        appendAdvisorJsonTable(sql, table);
        sql.append("  WHERE json_exists(b.data, '$.advisors[*]?(@.advisorId == $aid)' PASSING ? AS \"aid\")\n");
        p.add(advisorId);
        sql.append("    AND json_exists(b.data, '$.advisoryContexts[*]?(@.advisoryContext == $ctx)' PASSING ? AS \"ctx\")\n");
        p.add(advisoryContext);
        sql.append("    AND json_value(b.data, '$.entDataOwnerPartyRoleId' RETURNING NUMBER) = ?\n");
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
    private SqlQuery buildDvQ4(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q4);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        appendInvestorColumns(sql);
        appendAdvisorJsonTable(sql, table);
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
    private SqlQuery buildDvQ5(Map<String, Object> params) {
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
        sql.append("  AND json_exists(b.data, '$.advisoryContexts[*]?(@.advisoryContext == $ctx)' PASSING ? AS \"ctx\")\n");
        p.add(advisoryContext);
        sql.append("  AND json_exists(b.data, '$.personaNms[*]?(@.personaNm == $pnm)' PASSING ? AS \"pnm\")\n");
        p.add(personaNm);
        sql.append("  AND NOT json_value(b.data, '$.visibleFlag') = 'N'\n");
        sql.append("  AND json_value(b.data, '$.totalViewableAcctsMarketValue' RETURNING NUMBER) BETWEEN ? AND ?\n");
        p.add(minMv);
        p.add(maxMv);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q6: BookRoleGroup - Entitlements + pxPartyRoleId ---
    private SqlQuery buildDvQ6(Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q6);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data RETURNING CLOB PRETTY)\n");
        sql.append("FROM ").append(table).append(" b\n");
        sql.append("WHERE json_exists(b.data, '$.advisoryContexts[*]?(@.advisoryContext == $ctx)' PASSING ? AS \"ctx\")\n");
        p.add(advisoryContext);
        sql.append("  AND json_exists(b.data, '$.partyRoleIds[*]?(@.partyRoleId == $uid)' PASSING ? AS \"uid\")\n");
        p.add(pxPartyRoleId);
        sql.append("  AND NOT json_value(b.data, '$.visibleFlag') = 'N'\n");
        sql.append("  AND json_value(b.data, '$.totalViewableAcctsMarketValue' RETURNING NUMBER) BETWEEN ? AND ?\n");
        p.add(minMv);
        p.add(maxMv);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q7: Account - Holdings fund ticker ---
    private SqlQuery buildDvQ7(Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        String fundTicker = (String) params.get("fundTicker");
        String table = tableName(QueryDefinition.Q7);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data RETURNING CLOB PRETTY)\n");
        sql.append("FROM ").append(table).append(" a\n");
        sql.append("WHERE json_exists(a.data, '$.partyRoleIds[*]?(@.partyRoleId == $uid)' PASSING ? AS \"uid\")\n");
        p.add(pxPartyRoleId);
        sql.append("  AND json_value(a.data, '$.viewableSource') = 'Y'\n");
        sql.append("  AND json_exists(a.data, '$.holdings[*]?(@.fundTicker == $ticker)' PASSING ? AS \"ticker\")\n");
        p.add(fundTicker);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q8: Advisor - Hierarchy path filter ---
    private SqlQuery buildDvQ8(Map<String, Object> params) {
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String partyNodePathValue = (String) params.get("partyNodePathValue");
        String table = tableName(QueryDefinition.Q8);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data RETURNING CLOB PRETTY)\n");
        sql.append("FROM ").append(table).append(" a\n");
        sql.append("WHERE json_value(a.data, '$.entDataOwnerPartyRoleId' RETURNING NUMBER) = ?\n");
        p.add(dataOwnerPartyRoleId);
        sql.append("  AND json_exists(a.data, '$.advisorHierarchy[*]?(@.partyNodePathValue == $val)' PASSING ? AS \"val\")\n");
        p.add(partyNodePathValue);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q9: Advisor - Market value range ---
    private SqlQuery buildDvQ9(Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q9);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data RETURNING CLOB PRETTY)\n");
        sql.append("FROM ").append(table).append(" a\n");
        sql.append("WHERE json_exists(a.data, '$.partyRoleIds[*]?(@.partyRoleId == $uid)' PASSING ? AS \"uid\")\n");
        p.add(pxPartyRoleId);
        sql.append("  AND json_value(a.data, '$.accountViewableMarketValue' RETURNING NUMBER) >= ?\n");
        p.add(minMv);
        sql.append("  AND json_value(a.data, '$.accountViewableMarketValue' RETURNING NUMBER) <= ?\n");
        p.add(maxMv);

        return new SqlQuery(sql.toString(), p);
    }
}
