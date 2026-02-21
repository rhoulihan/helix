package com.helix.benchmark.query;

import com.helix.benchmark.config.SchemaModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OracleJdbcQueryExecutor {

    public record SqlQuery(String sql, List<Object> parameters) {}

    public SqlQuery buildSql(QueryDefinition query, SchemaModel model, Map<String, Object> params) {
        return switch (query) {
            case Q1 -> buildQ1Sql(model, params);
            case Q2 -> buildQ2Sql(model, params);
            case Q3 -> buildQ3Sql(model, params);
            case Q4 -> buildQ4Sql(model, params);
            case Q5 -> buildQ5Sql(model, params);
            case Q6 -> buildQ6Sql(model, params);
            case Q7 -> buildQ7Sql(model, params);
            case Q8 -> buildQ8Sql(model, params);
            case Q9 -> buildQ9Sql(model, params);
        };
    }

    private String tableName(QueryDefinition query, SchemaModel model) {
        if (model.isNormalized()) return "helix";
        return switch (query.embeddedCollection()) {
            case "bookRoleInvestor" -> "book_role_investor";
            case "bookRoleGroup" -> "book_role_group";
            case "account" -> "account";
            case "advisor" -> "advisor";
            default -> throw new IllegalArgumentException("Unknown collection: " + query.embeddedCollection());
        };
    }

    private String typeFilter(QueryDefinition query, SchemaModel model) {
        if (!model.isNormalized()) return "";
        String type = switch (query.embeddedCollection()) {
            case "bookRoleInvestor" -> "BookRoleInvestor";
            case "bookRoleGroup" -> "BookRoleGroup";
            case "account" -> "Account";
            case "advisor" -> "Advisor";
            default -> throw new IllegalArgumentException("Unknown collection");
        };
        return "json_value(data, '$.type') = '" + type + "'";
    }

    // --- Q1: BookRoleInvestor - Investor list by advisor ---
    private SqlQuery buildQ1Sql(SchemaModel model, Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        String table = tableName(QueryDefinition.Q1, model);
        String advisorsPath = model.isNormalized() ? "$.advisorsMetadata[*]" : "$.advisors[*]";

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(matched.* PRETTY) FROM (\n");
        sql.append("  SELECT jt.*, COUNT(*) OVER () AS total_count\n");
        sql.append("  FROM ").append(table).append(" b,\n");
        sql.append("       JSON_TABLE(b.data, '").append(advisorsPath).append("' COLUMNS (\n");
        sql.append("           advisor_id    VARCHAR2(30) PATH '$.advisorId',\n");
        sql.append("           viewable_mv   NUMBER       PATH '$.viewableMarketValue',\n");
        sql.append("           viewable_accts NUMBER      PATH '$.noOfViewableAccts'\n");
        sql.append("       )) jt\n");
        sql.append("  WHERE json_value(b.data, '$.investorType') = 'Client'\n");
        sql.append("    AND json_value(b.data, '$.viewableSource') = 'Y'\n");
        if (model.isNormalized()) {
            sql.append("    AND ").append(typeFilter(QueryDefinition.Q1, model)).append("\n");
        }
        sql.append("    AND jt.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND jt.viewable_accts >= 1\n");
        sql.append("  ORDER BY jt.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(") matched");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q2: BookRoleInvestor - Search by name with regex ---
    private SqlQuery buildQ2Sql(SchemaModel model, Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        Long partyRoleId = ((Number) params.get("partyRoleId")).longValue();
        String searchTerm = (String) params.get("searchTerm");
        String table = tableName(QueryDefinition.Q2, model);
        String advisorsPath = model.isNormalized() ? "$.advisorsMetadata[*]" : "$.advisors[*]";

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(matched.* PRETTY) FROM (\n");
        sql.append("  SELECT jt.*, COUNT(*) OVER () AS total_count\n");
        sql.append("  FROM ").append(table).append(" b,\n");
        sql.append("       JSON_TABLE(b.data, '").append(advisorsPath).append("' COLUMNS (\n");
        sql.append("           advisor_id    VARCHAR2(30) PATH '$.advisorId',\n");
        sql.append("           viewable_mv   NUMBER       PATH '$.viewableMarketValue',\n");
        sql.append("           viewable_accts NUMBER      PATH '$.noOfViewableAccts'\n");
        sql.append("       )) jt\n");
        sql.append("  WHERE json_value(b.data, '$.investorType') = 'Client'\n");
        sql.append("    AND json_value(b.data, '$.viewableFlag') = 'Y'\n");
        sql.append("    AND json_value(b.data, '$.partyRoleId' RETURNING NUMBER) = ?\n");
        p.add(partyRoleId);
        sql.append("    AND UPPER(json_value(b.data, '$.investorFullName')) LIKE UPPER(?)\n");
        p.add("%" + searchTerm + "%");
        if (model.isNormalized()) {
            sql.append("    AND ").append(typeFilter(QueryDefinition.Q2, model)).append("\n");
        }
        sql.append("    AND jt.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND jt.viewable_accts >= 1\n");
        sql.append("  ORDER BY jt.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(") matched");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q3: BookRoleInvestor - Entitlements + advisor ---
    private SqlQuery buildQ3Sql(SchemaModel model, Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String table = tableName(QueryDefinition.Q3, model);
        String advisorsPath = model.isNormalized() ? "$.advisorsMetadata[*]" : "$.advisors[*]";

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(matched.* PRETTY) FROM (\n");
        sql.append("  SELECT jt.*, COUNT(*) OVER () AS total_count\n");
        sql.append("  FROM ").append(table).append(" b,\n");
        sql.append("       JSON_TABLE(b.data, '").append(advisorsPath).append("' COLUMNS (\n");
        sql.append("           advisor_id    VARCHAR2(30) PATH '$.advisorId',\n");
        sql.append("           viewable_mv   NUMBER       PATH '$.viewableMarketValue',\n");
        sql.append("           viewable_accts NUMBER      PATH '$.noOfViewableAccts'\n");
        sql.append("       )) jt\n");
        sql.append("  WHERE json_exists(b.data, '$.entitlements.advisoryContext[*]?(@ == $ctx)' PASSING ? AS \"ctx\")\n");
        p.add(advisoryContext);
        sql.append("    AND json_value(b.data, '$.entitlements.pxClient.dataOwnerPartyRoleId' RETURNING NUMBER) = ?\n");
        p.add(dataOwnerPartyRoleId);
        sql.append("    AND json_value(b.data, '$.investorType') = 'Client'\n");
        sql.append("    AND json_value(b.data, '$.viewableSource') = 'Y'\n");
        if (model.isNormalized()) {
            sql.append("    AND ").append(typeFilter(QueryDefinition.Q3, model)).append("\n");
        }
        sql.append("    AND jt.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND jt.viewable_accts >= 1\n");
        sql.append("  ORDER BY jt.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(") matched");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q4: BookRoleInvestor - Market value range ---
    private SqlQuery buildQ4Sql(SchemaModel model, Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q4, model);
        String advisorsPath = model.isNormalized() ? "$.advisorsMetadata[*]" : "$.advisors[*]";

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(matched.* PRETTY) FROM (\n");
        sql.append("  SELECT jt.*, COUNT(*) OVER () AS total_count\n");
        sql.append("  FROM ").append(table).append(" b,\n");
        sql.append("       JSON_TABLE(b.data, '").append(advisorsPath).append("' COLUMNS (\n");
        sql.append("           advisor_id    VARCHAR2(30) PATH '$.advisorId',\n");
        sql.append("           viewable_mv   NUMBER       PATH '$.viewableMarketValue',\n");
        sql.append("           viewable_accts NUMBER      PATH '$.noOfViewableAccts'\n");
        sql.append("       )) jt\n");
        sql.append("  WHERE json_value(b.data, '$.investorType') = 'Client'\n");
        sql.append("    AND json_value(b.data, '$.viewableSource') = 'Y'\n");
        if (model.isNormalized()) {
            sql.append("    AND ").append(typeFilter(QueryDefinition.Q4, model)).append("\n");
        }
        sql.append("    AND jt.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND jt.viewable_accts >= 1\n");
        sql.append("    AND jt.viewable_mv >= ?\n");
        p.add(minMv);
        sql.append("    AND jt.viewable_mv <= ?\n");
        p.add(maxMv);
        sql.append("  ORDER BY jt.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(") matched");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q5: BookRoleGroup - Entitlements, persona, market value ---
    private SqlQuery buildQ5Sql(SchemaModel model, Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String personaNm = (String) params.get("personaNm");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q5, model);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data PRETTY)\n");
        sql.append("FROM ").append(table).append(" b\n");
        sql.append("WHERE json_exists(b.data, '$.entitlements.advisoryContext[*]?(@ == $ctx)' PASSING ? AS \"ctx\")\n");
        p.add(advisoryContext);
        sql.append("  AND json_value(b.data, '$.dataOwnerPartyRoleId' RETURNING NUMBER) = ?\n");
        p.add(dataOwnerPartyRoleId);
        sql.append("  AND json_exists(b.data, '$.personaNm[*]?(@ == $pnm)' PASSING ? AS \"pnm\")\n");
        p.add(personaNm);
        sql.append("  AND NOT json_value(b.data, '$.visibleFlag') = 'N'\n");
        sql.append("  AND json_value(b.data, '$.totalViewableAccountsMarketValue' RETURNING NUMBER) BETWEEN ? AND ?\n");
        p.add(minMv);
        p.add(maxMv);
        if (model.isNormalized()) {
            sql.append("  AND ").append(typeFilter(QueryDefinition.Q5, model)).append("\n");
        }

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q6: BookRoleGroup - Entitlements + pxPartyRoleId ---
    private SqlQuery buildQ6Sql(SchemaModel model, Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q6, model);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data PRETTY)\n");
        sql.append("FROM ").append(table).append(" b\n");
        sql.append("WHERE json_exists(b.data, '$.entitlements.advisoryContext[*]?(@ == $ctx)' PASSING ? AS \"ctx\")\n");
        p.add(advisoryContext);
        sql.append("  AND json_exists(b.data, '$.entitlements.pxPartyRoleIdList[*]?(@ == $uid)' PASSING ? AS \"uid\")\n");
        p.add(pxPartyRoleId);
        sql.append("  AND NOT json_value(b.data, '$.visibleFlag') = 'N'\n");
        sql.append("  AND json_value(b.data, '$.totalViewableAccountsMarketValue' RETURNING NUMBER) BETWEEN ? AND ?\n");
        p.add(minMv);
        p.add(maxMv);
        if (model.isNormalized()) {
            sql.append("  AND ").append(typeFilter(QueryDefinition.Q6, model)).append("\n");
        }

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q7: Account - Holdings fund ticker ---
    private SqlQuery buildQ7Sql(SchemaModel model, Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        String fundTicker = (String) params.get("fundTicker");
        String table = tableName(QueryDefinition.Q7, model);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data PRETTY)\n");
        sql.append("FROM ").append(table).append(" a\n");
        sql.append("WHERE json_exists(a.data, '$.entitlements.pxPartyRoleIdList[*]?(@ == $uid)' PASSING ? AS \"uid\")\n");
        p.add(pxPartyRoleId);
        sql.append("  AND json_value(a.data, '$.viewableSource') = 'Y'\n");
        sql.append("  AND json_exists(a.data, '$.holdings[*]?(@.fundTicker == $ticker)' PASSING ? AS \"ticker\")\n");
        p.add(fundTicker);
        if (model.isNormalized()) {
            sql.append("  AND ").append(typeFilter(QueryDefinition.Q7, model)).append("\n");
        }

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q8: Advisor - Hierarchy path filter ---
    private SqlQuery buildQ8Sql(SchemaModel model, Map<String, Object> params) {
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String partyNodePathValue = (String) params.get("partyNodePathValue");
        String table = tableName(QueryDefinition.Q8, model);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data PRETTY)\n");
        sql.append("FROM ").append(table).append(" a\n");
        sql.append("WHERE json_value(a.data, '$.entitlements.pxClient.dataOwnerPartyRoleId' RETURNING NUMBER) = ?\n");
        p.add(dataOwnerPartyRoleId);
        sql.append("  AND json_exists(a.data, '$.advisorHierarchy[*]?(@.partyNodePathValue == $val)' PASSING ? AS \"val\")\n");
        p.add(partyNodePathValue);
        if (model.isNormalized()) {
            sql.append("  AND ").append(typeFilter(QueryDefinition.Q8, model)).append("\n");
        }

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q9: Advisor - Market value range ---
    private SqlQuery buildQ9Sql(SchemaModel model, Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String table = tableName(QueryDefinition.Q9, model);

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT json_serialize(data PRETTY)\n");
        sql.append("FROM ").append(table).append(" a\n");
        sql.append("WHERE json_exists(a.data, '$.entitlements.pxPartyRoleIdList[*]?(@ == $uid)' PASSING ? AS \"uid\")\n");
        p.add(pxPartyRoleId);
        sql.append("  AND json_value(a.data, '$.accountViewableMarketValue' RETURNING NUMBER) >= ?\n");
        p.add(minMv);
        sql.append("  AND json_value(a.data, '$.accountViewableMarketValue' RETURNING NUMBER) <= ?\n");
        p.add(maxMv);
        if (model.isNormalized()) {
            sql.append("  AND ").append(typeFilter(QueryDefinition.Q9, model)).append("\n");
        }

        return new SqlQuery(sql.toString(), p);
    }
}
