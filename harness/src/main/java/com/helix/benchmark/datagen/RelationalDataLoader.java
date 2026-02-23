package com.helix.benchmark.datagen;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.List;

public class RelationalDataLoader {
    private static final Logger log = LoggerFactory.getLogger(RelationalDataLoader.class);

    public void loadToRelational(DataSource ds, String collectionType,
                                  List<Document> docs, int batchSize) throws Exception {
        switch (collectionType) {
            case "bookRoleInvestor" -> loadBookRoleInvestors(ds, docs, batchSize);
            case "bookRoleGroup" -> loadBookRoleGroups(ds, docs, batchSize);
            case "account" -> loadAccounts(ds, docs, batchSize);
            case "advisor" -> loadAdvisors(ds, docs, batchSize);
            default -> throw new IllegalArgumentException("Unknown collection type: " + collectionType);
        }
    }

    private void loadBookRoleInvestors(DataSource ds, List<Document> docs, int batchSize) throws Exception {
        String parentSql = """
                INSERT INTO rel_book_role_investor (id, party_role_id, investor_type, investor_full_name,
                    viewable_flag, viewable_source, ent_data_owner_party_role_id,
                    party_id, conversion_in_progress, data_owner_party_role_id, investor_id_field, entity,
                    total_market_value, total_accounts, total_viewable_accts_market_value, total_viewable_account_count,
                    ssn_tin, fin_inst_id, investor_last_name, investor_first_name, investor_middle_name,
                    investor_party_role_id, investor_city, investor_state, investor_zip_code,
                    investor_birthdate, client_access, trust_flag, update_tmst, setup_tmst, etl_update_ts,
                    px_client_id, px_client_name, px_client_ref_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""";
        String advisorSql = """
                INSERT INTO rel_bri_advisors (investor_id, advisor_id, viewable_mv, no_of_viewable_accts,
                    advisor_name, advisor_tax_id, advisor_fin_inst_id, last_name, first_name, middle_name,
                    state, city, zip_code, country, business_phone, book_type,
                    market_value, no_of_accts, status, is_primary, email)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""";
        String ctxSql = "INSERT INTO rel_bri_advisory_ctx (investor_id, advisory_context) VALUES (?, ?)";
        String bookRoleSql = "INSERT INTO rel_bri_adv_book_roles (investor_id, advisor_id, book_role) VALUES (?, ?, ?)";
        String hierSql = "INSERT INTO rel_bri_investor_hierarchy (investor_id, party_node_path_nm, party_node_path_value) VALUES (?, ?, ?)";
        String personaSql = "INSERT INTO rel_bri_persona_nm (investor_id, persona_nm) VALUES (?, ?)";
        String synSql = "INSERT INTO rel_bri_synonyms (investor_id, synonym_type_cd, synonym_str) VALUES (?, ?, ?)";

        try (Connection conn = ds.getConnection();
             PreparedStatement parentPs = conn.prepareStatement(parentSql);
             PreparedStatement advisorPs = conn.prepareStatement(advisorSql);
             PreparedStatement ctxPs = conn.prepareStatement(ctxSql);
             PreparedStatement bookRolePs = conn.prepareStatement(bookRoleSql);
             PreparedStatement hierPs = conn.prepareStatement(hierSql);
             PreparedStatement personaPs = conn.prepareStatement(personaSql);
             PreparedStatement synPs = conn.prepareStatement(synSql)) {

            int loaded = 0;
            for (Document doc : docs) {
                String id = doc.getString("_id");
                Number entOwner = extractEntDataOwnerPartyRoleId(doc);
                Document pxClient = extractPxClient(doc);

                int col = 1;
                parentPs.setString(col++, id);
                parentPs.setObject(col++, doc.get("partyRoleId"));
                parentPs.setString(col++, doc.getString("investorType"));
                parentPs.setString(col++, doc.getString("investorFullName"));
                parentPs.setString(col++, doc.getString("viewableFlag"));
                parentPs.setString(col++, doc.getString("viewableSource"));
                parentPs.setObject(col++, entOwner);
                parentPs.setObject(col++, doc.get("partyId"));
                parentPs.setString(col++, boolToString(doc.get("conversionInProgress")));
                parentPs.setObject(col++, doc.get("dataOwnerPartyRoleId"));
                parentPs.setString(col++, doc.getString("investorId"));
                parentPs.setString(col++, doc.getString("entity"));
                parentPs.setObject(col++, doc.get("totalMarketValue"));
                parentPs.setObject(col++, doc.get("totalAccounts"));
                parentPs.setObject(col++, doc.get("totalViewableAccountsMarketValue"));
                parentPs.setObject(col++, doc.get("totalViewableAccountCount"));
                parentPs.setString(col++, doc.getString("ssnTin"));
                parentPs.setObject(col++, doc.get("finInstId"));
                parentPs.setString(col++, doc.getString("investorLastName"));
                parentPs.setString(col++, doc.getString("investorFirstName"));
                parentPs.setString(col++, doc.getString("investorMiddleName"));
                parentPs.setObject(col++, doc.get("investorpartyRoleId"));
                parentPs.setString(col++, doc.getString("investorCity"));
                parentPs.setString(col++, doc.getString("investorState"));
                parentPs.setString(col++, doc.getString("investorZipCode"));
                parentPs.setObject(col++, toSqlDate(doc.get("investorBirthdate")));
                parentPs.setString(col++, doc.getString("clientAccess"));
                parentPs.setString(col++, doc.getString("trustFlag"));
                parentPs.setObject(col++, toSqlDate(doc.get("updateTmst")));
                parentPs.setObject(col++, toSqlDate(doc.get("setupTmst")));
                parentPs.setString(col++, doc.getString("ETLUpdateTS"));
                parentPs.setString(col++, pxClient != null ? pxClient.getString("pxClientId") : null);
                parentPs.setString(col++, pxClient != null ? pxClient.getString("pxClientName") : null);
                parentPs.setString(col++, pxClient != null ? pxClient.getString("Id") : null);
                parentPs.addBatch();

                // Child: advisors + book roles
                List<Document> advisors = doc.getList("advisors", Document.class);
                if (advisors != null) {
                    for (Document adv : advisors) {
                        String advId = adv.getString("advisorId");
                        int ac = 1;
                        advisorPs.setString(ac++, id);
                        advisorPs.setString(ac++, advId);
                        advisorPs.setObject(ac++, adv.get("viewableMarketValue"));
                        advisorPs.setObject(ac++, adv.get("noOfViewableAccts"));
                        advisorPs.setString(ac++, adv.getString("advisorName"));
                        advisorPs.setString(ac++, adv.getString("advisorTaxId"));
                        advisorPs.setObject(ac++, adv.get("finInstId"));
                        advisorPs.setString(ac++, adv.getString("lastName"));
                        advisorPs.setString(ac++, adv.getString("firstName"));
                        advisorPs.setString(ac++, adv.getString("middleName"));
                        advisorPs.setString(ac++, adv.getString("state"));
                        advisorPs.setString(ac++, adv.getString("city"));
                        advisorPs.setString(ac++, adv.getString("zipCode"));
                        advisorPs.setString(ac++, adv.getString("country"));
                        advisorPs.setString(ac++, adv.getString("businessPhone"));
                        advisorPs.setString(ac++, adv.getString("bookType"));
                        advisorPs.setObject(ac++, adv.get("marketValue"));
                        advisorPs.setObject(ac++, adv.get("noOfAccts"));
                        advisorPs.setString(ac++, adv.getString("status"));
                        advisorPs.setString(ac++, boolToString(adv.get("isPrimary")));
                        advisorPs.setString(ac++, adv.getString("email"));
                        advisorPs.addBatch();

                        // Nested: bookRoles[]
                        List<String> bookRoles = adv.getList("bookRoles", String.class);
                        if (bookRoles != null) {
                            for (String role : bookRoles) {
                                bookRolePs.setString(1, id);
                                bookRolePs.setString(2, advId);
                                bookRolePs.setString(3, role);
                                bookRolePs.addBatch();
                            }
                        }
                    }
                }

                // Child: advisory contexts
                Document entitlements = doc.get("entitlements", Document.class);
                if (entitlements != null) {
                    List<String> contexts = entitlements.getList("advisoryContext", String.class);
                    if (contexts != null) {
                        for (String ctx : contexts) {
                            ctxPs.setString(1, id);
                            ctxPs.setString(2, ctx);
                            ctxPs.addBatch();
                        }
                    }
                }

                // Child: advisor hierarchy
                List<Document> hierarchy = doc.getList("advisorHierarchy", Document.class);
                if (hierarchy != null) {
                    for (Document h : hierarchy) {
                        hierPs.setString(1, id);
                        hierPs.setString(2, h.getString("partyNodePathNm"));
                        hierPs.setString(3, h.getString("partyNodePathValue"));
                        hierPs.addBatch();
                    }
                }

                // Child: persona names
                List<String> personas = doc.getList("personaNm", String.class);
                if (personas != null) {
                    for (String persona : personas) {
                        personaPs.setString(1, id);
                        personaPs.setString(2, persona);
                        personaPs.addBatch();
                    }
                }

                // Child: synonyms
                List<Document> synonyms = doc.getList("synonyms", Document.class);
                if (synonyms != null) {
                    for (Document s : synonyms) {
                        synPs.setString(1, id);
                        synPs.setString(2, s.getString("partySynonymTypeCd"));
                        synPs.setString(3, s.getString("partySynonymStr"));
                        synPs.addBatch();
                    }
                }

                loaded++;
                if (loaded % batchSize == 0) {
                    parentPs.executeBatch();
                    advisorPs.executeBatch();
                    ctxPs.executeBatch();
                    bookRolePs.executeBatch();
                    hierPs.executeBatch();
                    personaPs.executeBatch();
                    synPs.executeBatch();
                    conn.commit();
                    if (loaded % 10000 == 0) {
                        log.info("Loaded {} / {} bookRoleInvestors to relational tables", loaded, docs.size());
                    }
                }
            }
            parentPs.executeBatch();
            advisorPs.executeBatch();
            ctxPs.executeBatch();
            bookRolePs.executeBatch();
            hierPs.executeBatch();
            personaPs.executeBatch();
            synPs.executeBatch();
            conn.commit();
        }
        log.info("Completed loading {} bookRoleInvestors to relational tables", docs.size());
    }

    private void loadBookRoleGroups(DataSource ds, List<Document> docs, int batchSize) throws Exception {
        String parentSql = """
                INSERT INTO rel_book_role_group (id, data_owner_party_role_id, visible_flag,
                    total_viewable_accts_market_value, investor_wri_id, etl_source_group, fin_inst_id,
                    entity, account_count, total_market_value, total_viewable_account_count,
                    account_group_name, account_group_id, account_group_type, portfolio_type,
                    etl_update_ts, px_client_id, px_client_name, px_client_ref_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""";
        String ctxSql = "INSERT INTO rel_brg_advisory_ctx (group_id, advisory_context) VALUES (?, ?)";
        String personaSql = "INSERT INTO rel_brg_persona_nm (group_id, persona_nm) VALUES (?, ?)";
        String partySql = "INSERT INTO rel_brg_party_role_ids (group_id, party_role_id) VALUES (?, ?)";
        String advSql = """
                INSERT INTO rel_brg_advisors (group_id, advisor_id, advisor_tax_id, fin_inst_id,
                    first_name, middle_name, last_name, advisor_name, book_type,
                    total_viewable_accts_market_value, total_viewable_account_count,
                    no_of_viewable_accts, viewable_market_value, status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""";
        String bookRoleSql = "INSERT INTO rel_brg_adv_book_roles (group_id, advisor_id, book_role) VALUES (?, ?, ?)";
        String investorSql = "INSERT INTO rel_brg_adv_investors (group_id, advisor_id, investor_id) VALUES (?, ?, ?)";
        String hierSql = "INSERT INTO rel_brg_hierarchy (group_id, party_node_path_value) VALUES (?, ?)";

        try (Connection conn = ds.getConnection();
             PreparedStatement parentPs = conn.prepareStatement(parentSql);
             PreparedStatement ctxPs = conn.prepareStatement(ctxSql);
             PreparedStatement personaPs = conn.prepareStatement(personaSql);
             PreparedStatement partyPs = conn.prepareStatement(partySql);
             PreparedStatement advPs = conn.prepareStatement(advSql);
             PreparedStatement bookRolePs = conn.prepareStatement(bookRoleSql);
             PreparedStatement investorPs = conn.prepareStatement(investorSql);
             PreparedStatement hierPs = conn.prepareStatement(hierSql)) {

            int loaded = 0;
            for (Document doc : docs) {
                String id = doc.getString("_id");
                Document pxClient = extractPxClient(doc);

                int col = 1;
                parentPs.setString(col++, id);
                parentPs.setObject(col++, doc.get("dataOwnerPartyRoleId"));
                parentPs.setString(col++, doc.getString("visibleFlag"));
                parentPs.setObject(col++, doc.get("totalViewableAccountsMarketValue"));
                parentPs.setString(col++, doc.getString("investorWriId"));
                parentPs.setString(col++, doc.getString("etlSourceGroup"));
                parentPs.setObject(col++, doc.get("finInstId"));
                parentPs.setString(col++, doc.getString("entity"));
                parentPs.setObject(col++, doc.get("accountCount"));
                parentPs.setObject(col++, doc.get("totalMarketValue"));
                parentPs.setObject(col++, doc.get("totalViewableAccountCount"));
                parentPs.setString(col++, doc.getString("accountGroupName"));
                parentPs.setString(col++, doc.getString("accountGroupId"));
                parentPs.setString(col++, doc.getString("accountGroupType"));
                parentPs.setString(col++, doc.getString("portfolioType"));
                parentPs.setString(col++, doc.getString("ETLUpdateTS"));
                parentPs.setString(col++, pxClient != null ? pxClient.getString("pxClientId") : null);
                parentPs.setString(col++, pxClient != null ? pxClient.getString("pxClientName") : null);
                parentPs.setString(col++, pxClient != null ? pxClient.getString("Id") : null);
                parentPs.addBatch();

                // Child: advisory contexts + party role IDs
                Document entitlements = doc.get("entitlements", Document.class);
                if (entitlements != null) {
                    List<String> contexts = entitlements.getList("advisoryContext", String.class);
                    if (contexts != null) {
                        for (String ctx : contexts) {
                            ctxPs.setString(1, id);
                            ctxPs.setString(2, ctx);
                            ctxPs.addBatch();
                        }
                    }
                    List<Number> partyRoleIds = entitlements.getList("pxPartyRoleIdList", Number.class);
                    if (partyRoleIds != null) {
                        for (Number prId : partyRoleIds) {
                            partyPs.setString(1, id);
                            partyPs.setObject(2, prId);
                            partyPs.addBatch();
                        }
                    }
                }

                // Child: persona names
                List<String> personas = doc.getList("personaNm", String.class);
                if (personas != null) {
                    for (String persona : personas) {
                        personaPs.setString(1, id);
                        personaPs.setString(2, persona);
                        personaPs.addBatch();
                    }
                }

                // Child: advisors + their book roles + their investors
                List<Document> advisors = doc.getList("advisors", Document.class);
                if (advisors != null) {
                    for (Document adv : advisors) {
                        String advId = adv.getString("advisorId");
                        int ac = 1;
                        advPs.setString(ac++, id);
                        advPs.setString(ac++, advId);
                        advPs.setString(ac++, adv.getString("advisorTaxId"));
                        advPs.setObject(ac++, adv.get("finInstId"));
                        advPs.setString(ac++, adv.getString("firstName"));
                        advPs.setString(ac++, adv.getString("middleName"));
                        advPs.setString(ac++, adv.getString("lastName"));
                        advPs.setString(ac++, adv.getString("advisorName"));
                        advPs.setString(ac++, adv.getString("bookType"));
                        advPs.setObject(ac++, adv.get("totalViewableAccountsMarketValue"));
                        advPs.setObject(ac++, adv.get("totalViewableAccountCount"));
                        advPs.setObject(ac++, adv.get("noOfViewableAccts"));
                        advPs.setObject(ac++, adv.get("viewableMarketValue"));
                        advPs.setString(ac++, adv.getString("status"));
                        advPs.addBatch();

                        List<String> bookRoles = adv.getList("bookRoles", String.class);
                        if (bookRoles != null) {
                            for (String role : bookRoles) {
                                bookRolePs.setString(1, id);
                                bookRolePs.setString(2, advId);
                                bookRolePs.setString(3, role);
                                bookRolePs.addBatch();
                            }
                        }

                        List<Document> investors = adv.getList("investors", Document.class);
                        if (investors != null) {
                            for (Document inv : investors) {
                                investorPs.setString(1, id);
                                investorPs.setString(2, advId);
                                investorPs.setString(3, inv.getString("investorId"));
                                investorPs.addBatch();
                            }
                        }
                    }
                }

                // Child: advisor hierarchy (short form - no partyNodePathNm)
                List<Document> hierarchy = doc.getList("advisorHierarchy", Document.class);
                if (hierarchy != null) {
                    for (Document h : hierarchy) {
                        hierPs.setString(1, id);
                        hierPs.setString(2, h.getString("partyNodePathValue"));
                        hierPs.addBatch();
                    }
                }

                loaded++;
                if (loaded % batchSize == 0) {
                    parentPs.executeBatch();
                    ctxPs.executeBatch();
                    personaPs.executeBatch();
                    partyPs.executeBatch();
                    advPs.executeBatch();
                    bookRolePs.executeBatch();
                    investorPs.executeBatch();
                    hierPs.executeBatch();
                    conn.commit();
                    if (loaded % 10000 == 0) {
                        log.info("Loaded {} / {} bookRoleGroups to relational tables", loaded, docs.size());
                    }
                }
            }
            parentPs.executeBatch();
            ctxPs.executeBatch();
            personaPs.executeBatch();
            partyPs.executeBatch();
            advPs.executeBatch();
            bookRolePs.executeBatch();
            investorPs.executeBatch();
            hierPs.executeBatch();
            conn.commit();
        }
        log.info("Completed loading {} bookRoleGroups to relational tables", docs.size());
    }

    private void loadAccounts(DataSource ds, List<Document> docs, int batchSize) throws Exception {
        String parentSql = """
                INSERT INTO rel_account (id, viewable_source, account_id_field, ssn_tin, fin_inst_id,
                    client_name, client_id, fin_inst_name, account_type, acct_name, viewable,
                    setup_tmst, update_tmst, acct_title, category, etl_update_ts,
                    px_client_id, px_client_name, px_client_ref_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""";
        String partySql = "INSERT INTO rel_acct_party_role_ids (account_id, party_role_id) VALUES (?, ?)";
        String holdingSql = """
                INSERT INTO rel_acct_holdings (account_id, fund_ticker, fund_name, fund_id, mgt_name, dividend_rate)
                VALUES (?, ?, ?, ?, ?, ?)""";
        String advisorSql = """
                INSERT INTO rel_acct_advisors (account_id, advisor_id, viewable_mv, no_of_viewable_accts,
                    advisor_name, advisor_tax_id, advisor_fin_inst_id, last_name, first_name, middle_name,
                    state, city, zip_code, country, business_phone, book_type,
                    market_value, no_of_accts, status, is_primary, email)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""";
        String bookRoleSql = "INSERT INTO rel_acct_adv_book_roles (account_id, advisor_id, book_role) VALUES (?, ?, ?)";
        String repCodeSql = "INSERT INTO rel_acct_rep_codes (account_id, advisor_rep_number, int_type, repcode_source) VALUES (?, ?, ?, ?)";
        String hierSql = "INSERT INTO rel_acct_hierarchy (account_id, party_node_path_nm, party_node_path_value) VALUES (?, ?, ?)";
        String invEntSql = """
                INSERT INTO rel_acct_ent_inv_entitlements (account_id, party_role_id, account_role,
                    account_source, account_access_status, investor_id, account_role_code)
                VALUES (?, ?, ?, ?, ?, ?, ?)""";
        String ctxSql = "INSERT INTO rel_acct_advisory_ctx (account_id, advisory_context) VALUES (?, ?)";

        try (Connection conn = ds.getConnection();
             PreparedStatement parentPs = conn.prepareStatement(parentSql);
             PreparedStatement partyPs = conn.prepareStatement(partySql);
             PreparedStatement holdingPs = conn.prepareStatement(holdingSql);
             PreparedStatement advisorPs = conn.prepareStatement(advisorSql);
             PreparedStatement bookRolePs = conn.prepareStatement(bookRoleSql);
             PreparedStatement repCodePs = conn.prepareStatement(repCodeSql);
             PreparedStatement hierPs = conn.prepareStatement(hierSql);
             PreparedStatement invEntPs = conn.prepareStatement(invEntSql);
             PreparedStatement ctxPs = conn.prepareStatement(ctxSql)) {

            int loaded = 0;
            for (Document doc : docs) {
                String id = doc.getString("_id");
                Document pxClient = extractPxClient(doc);

                int col = 1;
                parentPs.setString(col++, id);
                parentPs.setString(col++, doc.getString("viewableSource"));
                parentPs.setString(col++, doc.getString("accountid"));
                parentPs.setString(col++, doc.getString("ssnTin"));
                parentPs.setObject(col++, doc.get("finInstId"));
                parentPs.setString(col++, doc.getString("clientName"));
                parentPs.setString(col++, doc.getString("clientId"));
                parentPs.setString(col++, doc.getString("finInstName"));
                parentPs.setString(col++, doc.getString("accountType"));
                parentPs.setString(col++, doc.getString("acctName"));
                parentPs.setString(col++, boolToString(doc.get("viewable")));
                parentPs.setObject(col++, toSqlDate(doc.get("setupTmst")));
                parentPs.setObject(col++, toSqlDate(doc.get("updateTmst")));
                parentPs.setString(col++, doc.getString("acctTitle"));
                parentPs.setString(col++, doc.getString("category"));
                parentPs.setString(col++, doc.getString("ETLUpdateTS"));
                parentPs.setString(col++, pxClient != null ? pxClient.getString("pxClientId") : null);
                parentPs.setString(col++, pxClient != null ? pxClient.getString("pxClientName") : null);
                parentPs.setString(col++, pxClient != null ? pxClient.getString("Id") : null);
                parentPs.addBatch();

                // Child: entitlements
                Document entitlements = doc.get("entitlements", Document.class);
                if (entitlements != null) {
                    List<Number> partyRoleIds = entitlements.getList("pxPartyRoleIdList", Number.class);
                    if (partyRoleIds != null) {
                        for (Number prId : partyRoleIds) {
                            partyPs.setString(1, id);
                            partyPs.setObject(2, prId);
                            partyPs.addBatch();
                        }
                    }
                    List<String> contexts = entitlements.getList("advisoryContext", String.class);
                    if (contexts != null) {
                        for (String ctx : contexts) {
                            ctxPs.setString(1, id);
                            ctxPs.setString(2, ctx);
                            ctxPs.addBatch();
                        }
                    }
                    List<Document> invEnts = entitlements.getList("pxInvestorEntitlements", Document.class);
                    if (invEnts != null) {
                        for (Document ie : invEnts) {
                            invEntPs.setString(1, id);
                            invEntPs.setObject(2, ie.get("partyRoleId"));
                            invEntPs.setString(3, ie.getString("accountRole"));
                            invEntPs.setString(4, ie.getString("accountSource"));
                            invEntPs.setString(5, ie.getString("accountAccessStatus"));
                            invEntPs.setString(6, ie.getString("investorId"));
                            invEntPs.setString(7, ie.getString("accountRoleCode"));
                            invEntPs.addBatch();
                        }
                    }
                }

                // Child: holdings
                List<Document> holdings = doc.getList("holdings", Document.class);
                if (holdings != null) {
                    for (Document h : holdings) {
                        holdingPs.setString(1, id);
                        holdingPs.setString(2, h.getString("fundTicker"));
                        holdingPs.setString(3, h.getString("fundName"));
                        holdingPs.setString(4, h.getString("fundId"));
                        holdingPs.setString(5, h.getString("mgtName"));
                        holdingPs.setObject(6, h.get("dividendRate"));
                        holdingPs.addBatch();
                    }
                }

                // Child: advisors + book roles
                List<Document> advisors = doc.getList("advisors", Document.class);
                if (advisors != null) {
                    for (Document adv : advisors) {
                        String advId = adv.getString("advisorId");
                        int ac = 1;
                        advisorPs.setString(ac++, id);
                        advisorPs.setString(ac++, advId);
                        advisorPs.setObject(ac++, adv.get("viewableMarketValue"));
                        advisorPs.setObject(ac++, adv.get("noOfViewableAccts"));
                        advisorPs.setString(ac++, adv.getString("advisorName"));
                        advisorPs.setString(ac++, adv.getString("advisorTaxId"));
                        advisorPs.setObject(ac++, adv.get("finInstId"));
                        advisorPs.setString(ac++, adv.getString("lastName"));
                        advisorPs.setString(ac++, adv.getString("firstName"));
                        advisorPs.setString(ac++, adv.getString("middleName"));
                        advisorPs.setString(ac++, adv.getString("state"));
                        advisorPs.setString(ac++, adv.getString("city"));
                        advisorPs.setString(ac++, adv.getString("zipCode"));
                        advisorPs.setString(ac++, adv.getString("country"));
                        advisorPs.setString(ac++, adv.getString("businessPhone"));
                        advisorPs.setString(ac++, adv.getString("bookType"));
                        advisorPs.setObject(ac++, adv.get("marketValue"));
                        advisorPs.setObject(ac++, adv.get("noOfAccts"));
                        advisorPs.setString(ac++, adv.getString("status"));
                        advisorPs.setString(ac++, boolToString(adv.get("isPrimary")));
                        advisorPs.setString(ac++, adv.getString("email"));
                        advisorPs.addBatch();

                        List<String> bookRoles = adv.getList("bookRoles", String.class);
                        if (bookRoles != null) {
                            for (String role : bookRoles) {
                                bookRolePs.setString(1, id);
                                bookRolePs.setString(2, advId);
                                bookRolePs.setString(3, role);
                                bookRolePs.addBatch();
                            }
                        }
                    }
                }

                // Child: rep codes
                List<Document> repCodes = doc.getList("repCodes", Document.class);
                if (repCodes != null) {
                    for (Document rc : repCodes) {
                        repCodePs.setString(1, id);
                        repCodePs.setString(2, rc.getString("advisorRepNumber"));
                        repCodePs.setObject(3, rc.get("intType"));
                        repCodePs.setString(4, rc.getString("repcodeSource"));
                        repCodePs.addBatch();
                    }
                }

                // Child: advisor hierarchy
                List<Document> hierarchy = doc.getList("advisorHierarchy", Document.class);
                if (hierarchy != null) {
                    for (Document h : hierarchy) {
                        hierPs.setString(1, id);
                        hierPs.setString(2, h.getString("partyNodePathNm"));
                        hierPs.setString(3, h.getString("partyNodePathValue"));
                        hierPs.addBatch();
                    }
                }

                loaded++;
                if (loaded % batchSize == 0) {
                    parentPs.executeBatch();
                    partyPs.executeBatch();
                    holdingPs.executeBatch();
                    advisorPs.executeBatch();
                    bookRolePs.executeBatch();
                    repCodePs.executeBatch();
                    hierPs.executeBatch();
                    invEntPs.executeBatch();
                    ctxPs.executeBatch();
                    conn.commit();
                    if (loaded % 10000 == 0) {
                        log.info("Loaded {} / {} accounts to relational tables", loaded, docs.size());
                    }
                }
            }
            parentPs.executeBatch();
            partyPs.executeBatch();
            holdingPs.executeBatch();
            advisorPs.executeBatch();
            bookRolePs.executeBatch();
            repCodePs.executeBatch();
            hierPs.executeBatch();
            invEntPs.executeBatch();
            ctxPs.executeBatch();
            conn.commit();
        }
        log.info("Completed loading {} accounts to relational tables", docs.size());
    }

    private void loadAdvisors(DataSource ds, List<Document> docs, int batchSize) throws Exception {
        String parentSql = """
                INSERT INTO rel_advisor (id, account_viewable_market_value, ent_data_owner_party_role_id,
                    advisor_name, px_id, party_node_label_id, advisor_tax_id, user_type, fin_inst_id,
                    adv_state, advisor_full_name, adv_setup_tmst, adv_update_tmst,
                    adv_acct_method, adv_method_flag, ria_iar_question, dba_question, no_of_segments,
                    fin_inst_name, fin_last_name, fin_first_name,
                    viewable_investor_count, account_viewable_count,
                    state, city, zip, country, status, viewable_source, etl_update_ts,
                    px_client_id, px_client_name, px_client_ref_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""";
        String hierSql = "INSERT INTO rel_adv_hierarchy (advisor_id, party_node_path_nm, party_node_path_value) VALUES (?, ?, ?)";
        String partySql = "INSERT INTO rel_adv_party_role_ids (advisor_id, party_role_id) VALUES (?, ?)";
        String holdingSql = """
                INSERT INTO rel_adv_holdings (advisor_id, fund_id, fund_name, fund_ticker, mgt_name, dividend_rate)
                VALUES (?, ?, ?, ?, ?, ?)""";
        String repCodeSql = "INSERT INTO rel_adv_rep_codes (advisor_id, advisor_rep_number, int_type, repcode_source) VALUES (?, ?, ?, ?)";
        String ctxSql = "INSERT INTO rel_adv_advisory_ctx (advisor_id, advisory_context) VALUES (?, ?)";

        try (Connection conn = ds.getConnection();
             PreparedStatement parentPs = conn.prepareStatement(parentSql);
             PreparedStatement hierPs = conn.prepareStatement(hierSql);
             PreparedStatement partyPs = conn.prepareStatement(partySql);
             PreparedStatement holdingPs = conn.prepareStatement(holdingSql);
             PreparedStatement repCodePs = conn.prepareStatement(repCodeSql);
             PreparedStatement ctxPs = conn.prepareStatement(ctxSql)) {

            int loaded = 0;
            for (Document doc : docs) {
                String id = doc.getString("_id");
                Number entOwner = extractEntDataOwnerPartyRoleId(doc);
                Document pxClient = extractPxClient(doc);

                int col = 1;
                parentPs.setString(col++, id);
                parentPs.setObject(col++, doc.get("accountViewableMarketValue"));
                parentPs.setObject(col++, entOwner);
                parentPs.setString(col++, doc.getString("advisorName"));
                parentPs.setString(col++, doc.getString("pxId"));
                parentPs.setString(col++, doc.getString("partyNodeLabelId"));
                parentPs.setString(col++, doc.getString("advisorTaxId"));
                parentPs.setString(col++, doc.getString("userType"));
                parentPs.setObject(col++, doc.get("finInstId"));
                parentPs.setString(col++, doc.getString("advState"));
                parentPs.setString(col++, doc.getString("advisorFullName"));
                parentPs.setObject(col++, toSqlDate(doc.get("advSetupTmst")));
                parentPs.setObject(col++, toSqlDate(doc.get("advUpdateTmst")));
                parentPs.setString(col++, doc.getString("advAcctMethod"));
                parentPs.setString(col++, doc.getString("advMethodFlag"));
                parentPs.setString(col++, doc.getString("riaIarQuestion"));
                parentPs.setString(col++, doc.getString("dbaQuestion"));
                parentPs.setString(col++, doc.getString("noOfSegments"));
                parentPs.setString(col++, doc.getString("finInstName"));
                parentPs.setString(col++, doc.getString("finLastName"));
                parentPs.setString(col++, doc.getString("finFirstName"));
                parentPs.setObject(col++, doc.get("viewableInvestorCount"));
                parentPs.setObject(col++, doc.get("accountViewableCount"));
                parentPs.setString(col++, doc.getString("state"));
                parentPs.setString(col++, doc.getString("city"));
                parentPs.setString(col++, doc.getString("zip"));
                parentPs.setString(col++, doc.getString("country"));
                parentPs.setString(col++, doc.getString("status"));
                parentPs.setString(col++, doc.getString("viewableSource"));
                parentPs.setString(col++, doc.getString("ETLUpdateTS"));
                parentPs.setString(col++, pxClient != null ? pxClient.getString("pxClientId") : null);
                parentPs.setString(col++, pxClient != null ? pxClient.getString("pxClientName") : null);
                parentPs.setString(col++, pxClient != null ? pxClient.getString("Id") : null);
                parentPs.addBatch();

                // Child: advisor hierarchy
                List<Document> hierarchy = doc.getList("advisorHierarchy", Document.class);
                if (hierarchy != null) {
                    for (Document h : hierarchy) {
                        hierPs.setString(1, id);
                        hierPs.setString(2, h.getString("partyNodePathNm"));
                        hierPs.setString(3, h.getString("partyNodePathValue"));
                        hierPs.addBatch();
                    }
                }

                // Child: party role IDs + advisory contexts
                Document entitlements = doc.get("entitlements", Document.class);
                if (entitlements != null) {
                    List<Number> partyRoleIds = entitlements.getList("pxPartyRoleIdList", Number.class);
                    if (partyRoleIds != null) {
                        for (Number prId : partyRoleIds) {
                            partyPs.setString(1, id);
                            partyPs.setObject(2, prId);
                            partyPs.addBatch();
                        }
                    }
                    List<String> contexts = entitlements.getList("advisoryContext", String.class);
                    if (contexts != null) {
                        for (String ctx : contexts) {
                            ctxPs.setString(1, id);
                            ctxPs.setString(2, ctx);
                            ctxPs.addBatch();
                        }
                    }
                }

                // Child: holdings
                List<Document> holdings = doc.getList("holdings", Document.class);
                if (holdings != null) {
                    for (Document h : holdings) {
                        holdingPs.setString(1, id);
                        holdingPs.setString(2, h.getString("fundId"));
                        holdingPs.setString(3, h.getString("fundName"));
                        holdingPs.setString(4, h.getString("fundTicker"));
                        holdingPs.setString(5, h.getString("mgtName"));
                        holdingPs.setObject(6, h.get("dividendRate"));
                        holdingPs.addBatch();
                    }
                }

                // Child: rep codes
                List<Document> repCodes = doc.getList("repCodes", Document.class);
                if (repCodes != null) {
                    for (Document rc : repCodes) {
                        repCodePs.setString(1, id);
                        repCodePs.setString(2, rc.getString("advisorRepNumber"));
                        repCodePs.setObject(3, rc.get("intType"));
                        repCodePs.setString(4, rc.getString("repcodeSource"));
                        repCodePs.addBatch();
                    }
                }

                loaded++;
                if (loaded % batchSize == 0) {
                    parentPs.executeBatch();
                    hierPs.executeBatch();
                    partyPs.executeBatch();
                    holdingPs.executeBatch();
                    repCodePs.executeBatch();
                    ctxPs.executeBatch();
                    conn.commit();
                    if (loaded % 10000 == 0) {
                        log.info("Loaded {} / {} advisors to relational tables", loaded, docs.size());
                    }
                }
            }
            parentPs.executeBatch();
            hierPs.executeBatch();
            partyPs.executeBatch();
            holdingPs.executeBatch();
            repCodePs.executeBatch();
            ctxPs.executeBatch();
            conn.commit();
        }
        log.info("Completed loading {} advisors to relational tables", docs.size());
    }

    static Number extractEntDataOwnerPartyRoleId(Document doc) {
        Document pxClient = extractPxClient(doc);
        if (pxClient != null) {
            return (Number) pxClient.get("dataOwnerPartyRoleId");
        }
        return null;
    }

    private static Document extractPxClient(Document doc) {
        Document entitlements = doc.get("entitlements", Document.class);
        if (entitlements != null) {
            return entitlements.get("pxClient", Document.class);
        }
        return null;
    }

    private static String boolToString(Object val) {
        if (val == null) return null;
        if (val instanceof Boolean b) return b ? "Y" : "N";
        return String.valueOf(val);
    }

    private static java.sql.Date toSqlDate(Object val) {
        if (val instanceof Date d) {
            return new java.sql.Date(d.getTime());
        }
        return null;
    }
}
