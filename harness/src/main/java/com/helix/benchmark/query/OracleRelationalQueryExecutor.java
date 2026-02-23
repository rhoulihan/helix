package com.helix.benchmark.query;

import com.helix.benchmark.config.SchemaModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OracleRelationalQueryExecutor extends OracleJdbcQueryExecutor {

    @Override
    public SqlQuery buildSql(QueryDefinition query, SchemaModel model, Map<String, Object> params) {
        return switch (query) {
            case Q1 -> buildRelQ1(params);
            case Q2 -> buildRelQ2(params);
            case Q3 -> buildRelQ3(params);
            case Q4 -> buildRelQ4(params);
            case Q5 -> buildRelQ5(params);
            case Q6 -> buildRelQ6(params);
            case Q7 -> buildRelQ7(params);
            case Q8 -> buildRelQ8(params);
            case Q9 -> buildRelQ9(params);
        };
    }

    // --- Q1: BookRoleInvestor - Investor list by advisor ---
    private SqlQuery buildRelQ1(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        sql.append("  SELECT b.id, b.investor_full_name, b.investor_type,\n");
        sql.append("         b.investor_last_name, b.investor_first_name, b.investor_middle_name,\n");
        sql.append("         b.investor_city, b.investor_state, b.investor_zip_code, b.ssn_tin,\n");
        sql.append("         b.party_role_id, b.party_id, b.fin_inst_id, b.client_access, b.etl_update_ts,\n");
        sql.append("         a.advisor_id, a.viewable_mv, a.no_of_viewable_accts,\n");
        sql.append("         COUNT(*) OVER () AS total_count\n");
        sql.append("  FROM rel_book_role_investor b\n");
        sql.append("  JOIN rel_bri_advisors a ON a.investor_id = b.id\n");
        sql.append("  WHERE a.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND b.investor_type = 'Client'\n");
        sql.append("    AND b.viewable_source = 'Y'\n");
        sql.append("    AND a.no_of_viewable_accts >= 1\n");
        sql.append("  ORDER BY a.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(")");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q2: BookRoleInvestor - Search by name with regex ---
    private SqlQuery buildRelQ2(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        Long partyRoleId = ((Number) params.get("partyRoleId")).longValue();
        String searchTerm = (String) params.get("searchTerm");

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        sql.append("  SELECT b.id, b.investor_full_name, b.investor_type,\n");
        sql.append("         b.investor_last_name, b.investor_first_name, b.investor_middle_name,\n");
        sql.append("         b.investor_city, b.investor_state, b.investor_zip_code, b.ssn_tin,\n");
        sql.append("         b.party_role_id, b.party_id, b.fin_inst_id, b.client_access, b.etl_update_ts,\n");
        sql.append("         a.advisor_id, a.viewable_mv, a.no_of_viewable_accts,\n");
        sql.append("         COUNT(*) OVER () AS total_count\n");
        sql.append("  FROM rel_book_role_investor b\n");
        sql.append("  JOIN rel_bri_advisors a ON a.investor_id = b.id\n");
        sql.append("  WHERE a.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND b.investor_type = 'Client'\n");
        sql.append("    AND b.viewable_flag = 'Y'\n");
        sql.append("    AND b.party_role_id = ?\n");
        p.add(partyRoleId);
        sql.append("    AND UPPER(b.investor_full_name) LIKE UPPER(?)\n");
        p.add("%" + searchTerm + "%");
        sql.append("    AND a.no_of_viewable_accts >= 1\n");
        sql.append("  ORDER BY a.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(")");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q3: BookRoleInvestor - Entitlements + advisor ---
    private SqlQuery buildRelQ3(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        sql.append("  SELECT b.id, b.investor_full_name, b.investor_type,\n");
        sql.append("         b.investor_last_name, b.investor_first_name, b.investor_middle_name,\n");
        sql.append("         b.investor_city, b.investor_state, b.investor_zip_code, b.ssn_tin,\n");
        sql.append("         b.party_role_id, b.party_id, b.fin_inst_id, b.client_access, b.etl_update_ts,\n");
        sql.append("         a.advisor_id, a.viewable_mv, a.no_of_viewable_accts,\n");
        sql.append("         COUNT(*) OVER () AS total_count\n");
        sql.append("  FROM rel_book_role_investor b\n");
        sql.append("  JOIN rel_bri_advisors a ON a.investor_id = b.id\n");
        sql.append("  WHERE a.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND EXISTS (SELECT 1 FROM rel_bri_advisory_ctx c WHERE c.investor_id = b.id AND c.advisory_context = ?)\n");
        p.add(advisoryContext);
        sql.append("    AND b.ent_data_owner_party_role_id = ?\n");
        p.add(dataOwnerPartyRoleId);
        sql.append("    AND b.investor_type = 'Client'\n");
        sql.append("    AND b.viewable_source = 'Y'\n");
        sql.append("    AND a.no_of_viewable_accts >= 1\n");
        sql.append("  ORDER BY a.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(")");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q4: BookRoleInvestor - Market value range ---
    private SqlQuery buildRelQ4(Map<String, Object> params) {
        String advisorId = (String) params.get("advisorId");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        sql.append("  SELECT b.id, b.investor_full_name, b.investor_type,\n");
        sql.append("         b.investor_last_name, b.investor_first_name, b.investor_middle_name,\n");
        sql.append("         b.investor_city, b.investor_state, b.investor_zip_code, b.ssn_tin,\n");
        sql.append("         b.party_role_id, b.party_id, b.fin_inst_id, b.client_access, b.etl_update_ts,\n");
        sql.append("         a.advisor_id, a.viewable_mv, a.no_of_viewable_accts,\n");
        sql.append("         COUNT(*) OVER () AS total_count\n");
        sql.append("  FROM rel_book_role_investor b\n");
        sql.append("  JOIN rel_bri_advisors a ON a.investor_id = b.id\n");
        sql.append("  WHERE a.advisor_id = ?\n");
        p.add(advisorId);
        sql.append("    AND b.investor_type = 'Client'\n");
        sql.append("    AND b.viewable_source = 'Y'\n");
        sql.append("    AND a.no_of_viewable_accts >= 1\n");
        sql.append("    AND a.viewable_mv >= ?\n");
        p.add(minMv);
        sql.append("    AND a.viewable_mv <= ?\n");
        p.add(maxMv);
        sql.append("  ORDER BY a.viewable_mv DESC\n");
        sql.append("  FETCH FIRST 50 ROWS ONLY\n");
        sql.append(")");

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q5: BookRoleGroup - Entitlements, persona, market value ---
    private SqlQuery buildRelQ5(Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String personaNm = (String) params.get("personaNm");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT JSON_OBJECT(\n");
        sql.append("    '_id' VALUE b.id,\n");
        sql.append("    'investorWriId' VALUE b.investor_wri_id,\n");
        sql.append("    'etlSourceGroup' VALUE b.etl_source_group,\n");
        sql.append("    'finInstId' VALUE b.fin_inst_id,\n");
        sql.append("    'entity' VALUE b.entity,\n");
        sql.append("    'dataOwnerPartyRoleId' VALUE b.data_owner_party_role_id,\n");
        sql.append("    'accountCount' VALUE b.account_count,\n");
        sql.append("    'totalMarketValue' VALUE b.total_market_value,\n");
        sql.append("    'totalViewableAccountCount' VALUE b.total_viewable_account_count,\n");
        sql.append("    'totalViewableAccountsMarketValue' VALUE b.total_viewable_accts_market_value,\n");
        sql.append("    'accountGroupName' VALUE b.account_group_name,\n");
        sql.append("    'accountGroupId' VALUE b.account_group_id,\n");
        sql.append("    'accountGroupType' VALUE b.account_group_type,\n");
        sql.append("    'visibleFlag' VALUE b.visible_flag,\n");
        sql.append("    'portfolioType' VALUE b.portfolio_type,\n");
        sql.append("    'ETLUpdateTS' VALUE b.etl_update_ts,\n");
        sql.append("    'personaNm' VALUE (SELECT JSON_ARRAYAGG(pn.persona_nm) FROM rel_brg_persona_nm pn WHERE pn.group_id = b.id),\n");
        sql.append("    'advisorHierarchy' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT('partyNodePathValue' VALUE h.party_node_path_value) RETURNING CLOB) FROM rel_brg_hierarchy h WHERE h.group_id = b.id),\n");
        sql.append("    'entitlements' VALUE JSON_OBJECT(\n");
        sql.append("        'pxPartyRoleIdList' VALUE (SELECT JSON_ARRAYAGG(pr.party_role_id) FROM rel_brg_party_role_ids pr WHERE pr.group_id = b.id),\n");
        sql.append("        'advisoryContext' VALUE (SELECT JSON_ARRAYAGG(c.advisory_context) FROM rel_brg_advisory_ctx c WHERE c.group_id = b.id),\n");
        sql.append("        'pxClient' VALUE JSON_OBJECT('pxClientId' VALUE b.px_client_id, 'pxClientName' VALUE b.px_client_name, 'Id' VALUE b.px_client_ref_id, 'dataOwnerPartyRoleId' VALUE b.data_owner_party_role_id)\n");
        sql.append("    ),\n");
        sql.append("    'advisors' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'advisorId' VALUE a.advisor_id, 'advisorTaxId' VALUE a.advisor_tax_id,\n");
        sql.append("        'finInstId' VALUE a.fin_inst_id, 'firstName' VALUE a.first_name,\n");
        sql.append("        'middleName' VALUE a.middle_name, 'lastName' VALUE a.last_name,\n");
        sql.append("        'advisorName' VALUE a.advisor_name, 'bookType' VALUE a.book_type,\n");
        sql.append("        'totalViewableAccountsMarketValue' VALUE a.total_viewable_accts_market_value,\n");
        sql.append("        'totalViewableAccountCount' VALUE a.total_viewable_account_count,\n");
        sql.append("        'noOfViewableAccts' VALUE a.no_of_viewable_accts,\n");
        sql.append("        'viewableMarketValue' VALUE a.viewable_market_value,\n");
        sql.append("        'status' VALUE a.status\n");
        sql.append("    ) RETURNING CLOB) FROM rel_brg_advisors a WHERE a.group_id = b.id)\n");
        sql.append("    RETURNING CLOB\n");
        sql.append(") AS doc\n");
        sql.append("FROM rel_book_role_group b\n");
        sql.append("WHERE b.data_owner_party_role_id = ?\n");
        p.add(dataOwnerPartyRoleId);
        sql.append("  AND EXISTS (SELECT 1 FROM rel_brg_advisory_ctx c WHERE c.group_id = b.id AND c.advisory_context = ?)\n");
        p.add(advisoryContext);
        sql.append("  AND EXISTS (SELECT 1 FROM rel_brg_persona_nm pn WHERE pn.group_id = b.id AND pn.persona_nm = ?)\n");
        p.add(personaNm);
        sql.append("  AND b.visible_flag != 'N'\n");
        sql.append("  AND b.total_viewable_accts_market_value BETWEEN ? AND ?\n");
        p.add(minMv);
        p.add(maxMv);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q6: BookRoleGroup - Entitlements + pxPartyRoleId ---
    private SqlQuery buildRelQ6(Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT JSON_OBJECT(\n");
        sql.append("    '_id' VALUE b.id,\n");
        sql.append("    'investorWriId' VALUE b.investor_wri_id,\n");
        sql.append("    'etlSourceGroup' VALUE b.etl_source_group,\n");
        sql.append("    'finInstId' VALUE b.fin_inst_id,\n");
        sql.append("    'entity' VALUE b.entity,\n");
        sql.append("    'dataOwnerPartyRoleId' VALUE b.data_owner_party_role_id,\n");
        sql.append("    'accountCount' VALUE b.account_count,\n");
        sql.append("    'totalMarketValue' VALUE b.total_market_value,\n");
        sql.append("    'totalViewableAccountCount' VALUE b.total_viewable_account_count,\n");
        sql.append("    'totalViewableAccountsMarketValue' VALUE b.total_viewable_accts_market_value,\n");
        sql.append("    'accountGroupName' VALUE b.account_group_name,\n");
        sql.append("    'accountGroupId' VALUE b.account_group_id,\n");
        sql.append("    'accountGroupType' VALUE b.account_group_type,\n");
        sql.append("    'visibleFlag' VALUE b.visible_flag,\n");
        sql.append("    'portfolioType' VALUE b.portfolio_type,\n");
        sql.append("    'ETLUpdateTS' VALUE b.etl_update_ts,\n");
        sql.append("    'personaNm' VALUE (SELECT JSON_ARRAYAGG(pn.persona_nm) FROM rel_brg_persona_nm pn WHERE pn.group_id = b.id),\n");
        sql.append("    'advisorHierarchy' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT('partyNodePathValue' VALUE h.party_node_path_value) RETURNING CLOB) FROM rel_brg_hierarchy h WHERE h.group_id = b.id),\n");
        sql.append("    'entitlements' VALUE JSON_OBJECT(\n");
        sql.append("        'pxPartyRoleIdList' VALUE (SELECT JSON_ARRAYAGG(pr.party_role_id) FROM rel_brg_party_role_ids pr WHERE pr.group_id = b.id),\n");
        sql.append("        'advisoryContext' VALUE (SELECT JSON_ARRAYAGG(c.advisory_context) FROM rel_brg_advisory_ctx c WHERE c.group_id = b.id),\n");
        sql.append("        'pxClient' VALUE JSON_OBJECT('pxClientId' VALUE b.px_client_id, 'pxClientName' VALUE b.px_client_name, 'Id' VALUE b.px_client_ref_id, 'dataOwnerPartyRoleId' VALUE b.data_owner_party_role_id)\n");
        sql.append("    ),\n");
        sql.append("    'advisors' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'advisorId' VALUE a.advisor_id, 'advisorTaxId' VALUE a.advisor_tax_id,\n");
        sql.append("        'finInstId' VALUE a.fin_inst_id, 'firstName' VALUE a.first_name,\n");
        sql.append("        'middleName' VALUE a.middle_name, 'lastName' VALUE a.last_name,\n");
        sql.append("        'advisorName' VALUE a.advisor_name, 'bookType' VALUE a.book_type,\n");
        sql.append("        'totalViewableAccountsMarketValue' VALUE a.total_viewable_accts_market_value,\n");
        sql.append("        'totalViewableAccountCount' VALUE a.total_viewable_account_count,\n");
        sql.append("        'noOfViewableAccts' VALUE a.no_of_viewable_accts,\n");
        sql.append("        'viewableMarketValue' VALUE a.viewable_market_value,\n");
        sql.append("        'status' VALUE a.status\n");
        sql.append("    ) RETURNING CLOB) FROM rel_brg_advisors a WHERE a.group_id = b.id)\n");
        sql.append("    RETURNING CLOB\n");
        sql.append(") AS doc\n");
        sql.append("FROM rel_book_role_group b\n");
        sql.append("WHERE EXISTS (SELECT 1 FROM rel_brg_advisory_ctx c WHERE c.group_id = b.id AND c.advisory_context = ?)\n");
        p.add(advisoryContext);
        sql.append("  AND EXISTS (SELECT 1 FROM rel_brg_party_role_ids pr WHERE pr.group_id = b.id AND pr.party_role_id = ?)\n");
        p.add(pxPartyRoleId);
        sql.append("  AND b.visible_flag != 'N'\n");
        sql.append("  AND b.total_viewable_accts_market_value BETWEEN ? AND ?\n");
        p.add(minMv);
        p.add(maxMv);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q7: Account - Holdings fund ticker ---
    private SqlQuery buildRelQ7(Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        String fundTicker = (String) params.get("fundTicker");

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT JSON_OBJECT(\n");
        sql.append("    '_id' VALUE a.id,\n");
        sql.append("    'accountid' VALUE a.account_id_field,\n");
        sql.append("    'ssnTin' VALUE a.ssn_tin,\n");
        sql.append("    'finInstId' VALUE a.fin_inst_id,\n");
        sql.append("    'clientName' VALUE a.client_name,\n");
        sql.append("    'clientId' VALUE a.client_id,\n");
        sql.append("    'finInstName' VALUE a.fin_inst_name,\n");
        sql.append("    'accountType' VALUE a.account_type,\n");
        sql.append("    'acctName' VALUE a.acct_name,\n");
        sql.append("    'viewable' VALUE a.viewable,\n");
        sql.append("    'viewableSource' VALUE a.viewable_source,\n");
        sql.append("    'setupTmst' VALUE a.setup_tmst,\n");
        sql.append("    'updateTmst' VALUE a.update_tmst,\n");
        sql.append("    'acctTitle' VALUE a.acct_title,\n");
        sql.append("    'category' VALUE a.category,\n");
        sql.append("    'ETLUpdateTS' VALUE a.etl_update_ts,\n");
        sql.append("    'holdings' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'fundId' VALUE h.fund_id, 'fundName' VALUE h.fund_name,\n");
        sql.append("        'fundTicker' VALUE h.fund_ticker, 'mgtName' VALUE h.mgt_name,\n");
        sql.append("        'dividendRate' VALUE h.dividend_rate\n");
        sql.append("    ) RETURNING CLOB) FROM rel_acct_holdings h WHERE h.account_id = a.id),\n");
        sql.append("    'advisors' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'advisorId' VALUE ad.advisor_id, 'advisorName' VALUE ad.advisor_name,\n");
        sql.append("        'advisorTaxId' VALUE ad.advisor_tax_id, 'finInstId' VALUE ad.advisor_fin_inst_id,\n");
        sql.append("        'lastName' VALUE ad.last_name, 'firstName' VALUE ad.first_name,\n");
        sql.append("        'middleName' VALUE ad.middle_name, 'state' VALUE ad.state,\n");
        sql.append("        'city' VALUE ad.city, 'zipCode' VALUE ad.zip_code,\n");
        sql.append("        'country' VALUE ad.country, 'businessPhone' VALUE ad.business_phone,\n");
        sql.append("        'bookType' VALUE ad.book_type, 'marketValue' VALUE ad.market_value,\n");
        sql.append("        'noOfAccts' VALUE ad.no_of_accts, 'noOfViewableAccts' VALUE ad.no_of_viewable_accts,\n");
        sql.append("        'viewableMarketValue' VALUE ad.viewable_mv, 'status' VALUE ad.status,\n");
        sql.append("        'isPrimary' VALUE ad.is_primary, 'email' VALUE ad.email\n");
        sql.append("    ) RETURNING CLOB) FROM rel_acct_advisors ad WHERE ad.account_id = a.id),\n");
        sql.append("    'repCodes' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'advisorRepNumber' VALUE rc.advisor_rep_number, 'intType' VALUE rc.int_type,\n");
        sql.append("        'repcodeSource' VALUE rc.repcode_source\n");
        sql.append("    ) RETURNING CLOB) FROM rel_acct_rep_codes rc WHERE rc.account_id = a.id),\n");
        sql.append("    'advisorHierarchy' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'partyNodePathNm' VALUE ah.party_node_path_nm,\n");
        sql.append("        'partyNodePathValue' VALUE ah.party_node_path_value\n");
        sql.append("    ) RETURNING CLOB) FROM rel_acct_hierarchy ah WHERE ah.account_id = a.id),\n");
        sql.append("    'entitlements' VALUE JSON_OBJECT(\n");
        sql.append("        'pxPartyRoleIdList' VALUE (SELECT JSON_ARRAYAGG(pr.party_role_id) FROM rel_acct_party_role_ids pr WHERE pr.account_id = a.id),\n");
        sql.append("        'advisoryContext' VALUE (SELECT JSON_ARRAYAGG(c.advisory_context) FROM rel_acct_advisory_ctx c WHERE c.account_id = a.id),\n");
        sql.append("        'pxClient' VALUE JSON_OBJECT('pxClientId' VALUE a.px_client_id, 'pxClientName' VALUE a.px_client_name, 'Id' VALUE a.px_client_ref_id, 'dataOwnerPartyRoleId' VALUE a.fin_inst_id),\n");
        sql.append("        'pxInvestorEntitlements' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("            'partyRoleId' VALUE ie.party_role_id, 'accountRole' VALUE ie.account_role,\n");
        sql.append("            'accountSource' VALUE ie.account_source, 'accountAccessStatus' VALUE ie.account_access_status,\n");
        sql.append("            'investorId' VALUE ie.investor_id, 'accountRoleCode' VALUE ie.account_role_code\n");
        sql.append("        ) RETURNING CLOB) FROM rel_acct_ent_inv_entitlements ie WHERE ie.account_id = a.id)\n");
        sql.append("    )\n");
        sql.append("    RETURNING CLOB\n");
        sql.append(") AS doc\n");
        sql.append("FROM rel_account a\n");
        sql.append("WHERE EXISTS (SELECT 1 FROM rel_acct_party_role_ids pr WHERE pr.account_id = a.id AND pr.party_role_id = ?)\n");
        p.add(pxPartyRoleId);
        sql.append("  AND a.viewable_source = 'Y'\n");
        sql.append("  AND EXISTS (SELECT 1 FROM rel_acct_holdings h WHERE h.account_id = a.id AND h.fund_ticker = ?)\n");
        p.add(fundTicker);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q8: Advisor - Hierarchy path filter ---
    private SqlQuery buildRelQ8(Map<String, Object> params) {
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String partyNodePathValue = (String) params.get("partyNodePathValue");

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT JSON_OBJECT(\n");
        sql.append("    '_id' VALUE a.id,\n");
        sql.append("    'advisorName' VALUE a.advisor_name,\n");
        sql.append("    'pxId' VALUE a.px_id,\n");
        sql.append("    'partyNodeLabelId' VALUE a.party_node_label_id,\n");
        sql.append("    'advisorTaxId' VALUE a.advisor_tax_id,\n");
        sql.append("    'userType' VALUE a.user_type,\n");
        sql.append("    'finInstId' VALUE a.fin_inst_id,\n");
        sql.append("    'advState' VALUE a.adv_state,\n");
        sql.append("    'advisorFullName' VALUE a.advisor_full_name,\n");
        sql.append("    'advSetupTmst' VALUE a.adv_setup_tmst,\n");
        sql.append("    'advUpdateTmst' VALUE a.adv_update_tmst,\n");
        sql.append("    'advAcctMethod' VALUE a.adv_acct_method,\n");
        sql.append("    'advMethodFlag' VALUE a.adv_method_flag,\n");
        sql.append("    'riaIarQuestion' VALUE a.ria_iar_question,\n");
        sql.append("    'dbaQuestion' VALUE a.dba_question,\n");
        sql.append("    'noOfSegments' VALUE a.no_of_segments,\n");
        sql.append("    'finInstName' VALUE a.fin_inst_name,\n");
        sql.append("    'finLastName' VALUE a.fin_last_name,\n");
        sql.append("    'finFirstName' VALUE a.fin_first_name,\n");
        sql.append("    'accountViewableMarketValue' VALUE a.account_viewable_market_value,\n");
        sql.append("    'viewableInvestorCount' VALUE a.viewable_investor_count,\n");
        sql.append("    'accountViewableCount' VALUE a.account_viewable_count,\n");
        sql.append("    'state' VALUE a.state,\n");
        sql.append("    'city' VALUE a.city,\n");
        sql.append("    'zip' VALUE a.zip,\n");
        sql.append("    'country' VALUE a.country,\n");
        sql.append("    'status' VALUE a.status,\n");
        sql.append("    'viewableSource' VALUE a.viewable_source,\n");
        sql.append("    'ETLUpdateTS' VALUE a.etl_update_ts,\n");
        sql.append("    'advisorHierarchy' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'partyNodePathNm' VALUE h.party_node_path_nm,\n");
        sql.append("        'partyNodePathValue' VALUE h.party_node_path_value\n");
        sql.append("    ) RETURNING CLOB) FROM rel_adv_hierarchy h WHERE h.advisor_id = a.id),\n");
        sql.append("    'holdings' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'fundId' VALUE hd.fund_id, 'fundName' VALUE hd.fund_name,\n");
        sql.append("        'fundTicker' VALUE hd.fund_ticker, 'mgtName' VALUE hd.mgt_name,\n");
        sql.append("        'dividendRate' VALUE hd.dividend_rate\n");
        sql.append("    ) RETURNING CLOB) FROM rel_adv_holdings hd WHERE hd.advisor_id = a.id),\n");
        sql.append("    'repCodes' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'advisorRepNumber' VALUE rc.advisor_rep_number, 'intType' VALUE rc.int_type,\n");
        sql.append("        'repcodeSource' VALUE rc.repcode_source\n");
        sql.append("    ) RETURNING CLOB) FROM rel_adv_rep_codes rc WHERE rc.advisor_id = a.id),\n");
        sql.append("    'entitlements' VALUE JSON_OBJECT(\n");
        sql.append("        'pxPartyRoleIdList' VALUE (SELECT JSON_ARRAYAGG(pr.party_role_id) FROM rel_adv_party_role_ids pr WHERE pr.advisor_id = a.id),\n");
        sql.append("        'advisoryContext' VALUE (SELECT JSON_ARRAYAGG(c.advisory_context) FROM rel_adv_advisory_ctx c WHERE c.advisor_id = a.id),\n");
        sql.append("        'pxClient' VALUE JSON_OBJECT('pxClientId' VALUE a.px_client_id, 'pxClientName' VALUE a.px_client_name, 'Id' VALUE a.px_client_ref_id, 'dataOwnerPartyRoleId' VALUE a.ent_data_owner_party_role_id)\n");
        sql.append("    )\n");
        sql.append("    RETURNING CLOB\n");
        sql.append(") AS doc\n");
        sql.append("FROM rel_advisor a\n");
        sql.append("WHERE a.ent_data_owner_party_role_id = ?\n");
        p.add(dataOwnerPartyRoleId);
        sql.append("  AND EXISTS (SELECT 1 FROM rel_adv_hierarchy h WHERE h.advisor_id = a.id AND h.party_node_path_value = ?)\n");
        p.add(partyNodePathValue);

        return new SqlQuery(sql.toString(), p);
    }

    // --- Q9: Advisor - Market value range ---
    private SqlQuery buildRelQ9(Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        List<Object> p = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT JSON_OBJECT(\n");
        sql.append("    '_id' VALUE a.id,\n");
        sql.append("    'advisorName' VALUE a.advisor_name,\n");
        sql.append("    'pxId' VALUE a.px_id,\n");
        sql.append("    'partyNodeLabelId' VALUE a.party_node_label_id,\n");
        sql.append("    'advisorTaxId' VALUE a.advisor_tax_id,\n");
        sql.append("    'userType' VALUE a.user_type,\n");
        sql.append("    'finInstId' VALUE a.fin_inst_id,\n");
        sql.append("    'advState' VALUE a.adv_state,\n");
        sql.append("    'advisorFullName' VALUE a.advisor_full_name,\n");
        sql.append("    'advSetupTmst' VALUE a.adv_setup_tmst,\n");
        sql.append("    'advUpdateTmst' VALUE a.adv_update_tmst,\n");
        sql.append("    'advAcctMethod' VALUE a.adv_acct_method,\n");
        sql.append("    'advMethodFlag' VALUE a.adv_method_flag,\n");
        sql.append("    'riaIarQuestion' VALUE a.ria_iar_question,\n");
        sql.append("    'dbaQuestion' VALUE a.dba_question,\n");
        sql.append("    'noOfSegments' VALUE a.no_of_segments,\n");
        sql.append("    'finInstName' VALUE a.fin_inst_name,\n");
        sql.append("    'finLastName' VALUE a.fin_last_name,\n");
        sql.append("    'finFirstName' VALUE a.fin_first_name,\n");
        sql.append("    'accountViewableMarketValue' VALUE a.account_viewable_market_value,\n");
        sql.append("    'viewableInvestorCount' VALUE a.viewable_investor_count,\n");
        sql.append("    'accountViewableCount' VALUE a.account_viewable_count,\n");
        sql.append("    'state' VALUE a.state,\n");
        sql.append("    'city' VALUE a.city,\n");
        sql.append("    'zip' VALUE a.zip,\n");
        sql.append("    'country' VALUE a.country,\n");
        sql.append("    'status' VALUE a.status,\n");
        sql.append("    'viewableSource' VALUE a.viewable_source,\n");
        sql.append("    'ETLUpdateTS' VALUE a.etl_update_ts,\n");
        sql.append("    'advisorHierarchy' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'partyNodePathNm' VALUE h.party_node_path_nm,\n");
        sql.append("        'partyNodePathValue' VALUE h.party_node_path_value\n");
        sql.append("    ) RETURNING CLOB) FROM rel_adv_hierarchy h WHERE h.advisor_id = a.id),\n");
        sql.append("    'holdings' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'fundId' VALUE hd.fund_id, 'fundName' VALUE hd.fund_name,\n");
        sql.append("        'fundTicker' VALUE hd.fund_ticker, 'mgtName' VALUE hd.mgt_name,\n");
        sql.append("        'dividendRate' VALUE hd.dividend_rate\n");
        sql.append("    ) RETURNING CLOB) FROM rel_adv_holdings hd WHERE hd.advisor_id = a.id),\n");
        sql.append("    'repCodes' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT(\n");
        sql.append("        'advisorRepNumber' VALUE rc.advisor_rep_number, 'intType' VALUE rc.int_type,\n");
        sql.append("        'repcodeSource' VALUE rc.repcode_source\n");
        sql.append("    ) RETURNING CLOB) FROM rel_adv_rep_codes rc WHERE rc.advisor_id = a.id),\n");
        sql.append("    'entitlements' VALUE JSON_OBJECT(\n");
        sql.append("        'pxPartyRoleIdList' VALUE (SELECT JSON_ARRAYAGG(pr.party_role_id) FROM rel_adv_party_role_ids pr WHERE pr.advisor_id = a.id),\n");
        sql.append("        'advisoryContext' VALUE (SELECT JSON_ARRAYAGG(c.advisory_context) FROM rel_adv_advisory_ctx c WHERE c.advisor_id = a.id),\n");
        sql.append("        'pxClient' VALUE JSON_OBJECT('pxClientId' VALUE a.px_client_id, 'pxClientName' VALUE a.px_client_name, 'Id' VALUE a.px_client_ref_id, 'dataOwnerPartyRoleId' VALUE a.ent_data_owner_party_role_id)\n");
        sql.append("    )\n");
        sql.append("    RETURNING CLOB\n");
        sql.append(") AS doc\n");
        sql.append("FROM rel_advisor a\n");
        sql.append("WHERE EXISTS (SELECT 1 FROM rel_adv_party_role_ids pr WHERE pr.advisor_id = a.id AND pr.party_role_id = ?)\n");
        p.add(pxPartyRoleId);
        sql.append("  AND a.account_viewable_market_value >= ?\n");
        p.add(minMv);
        sql.append("  AND a.account_viewable_market_value <= ?\n");
        p.add(maxMv);

        return new SqlQuery(sql.toString(), p);
    }
}
