package com.helix.benchmark.schema;

import com.helix.benchmark.config.SchemaModel;

import java.util.ArrayList;
import java.util.List;

public class OracleSchemaManager implements SchemaManager {

    @Override
    public List<String> getCollectionNames(SchemaModel model) {
        return switch (model) {
            case EMBEDDED -> List.of("account", "advisor", "book_role_group", "book_role_investor");
            case NORMALIZED -> List.of("helix");
        };
    }

    public List<String> getTableNames(SchemaModel model) {
        return getCollectionNames(model);
    }

    public List<String> getCreateTableStatements(SchemaModel model) {
        return getTableNames(model).stream()
                .map(t -> "CREATE JSON COLLECTION TABLE " + t)
                .toList();
    }

    public List<String> getIndexStatements(SchemaModel model) {
        return switch (model) {
            case EMBEDDED -> embeddedIndexes();
            case NORMALIZED -> normalizedIndexes();
        };
    }

    private List<String> embeddedIndexes() {
        List<String> indexes = new ArrayList<>();

        // BookRoleInvestor indexes (Q1-Q4)
        indexes.add("""
                CREATE INDEX idx_bri_inv_type ON book_role_investor (
                    json_value(data, '$.investorType' RETURNING VARCHAR2(20)),
                    json_value(data, '$.viewableSource' RETURNING VARCHAR2(5)))""");
        indexes.add("CREATE MULTIVALUE INDEX idx_bri_adv_id ON book_role_investor b (b.data.advisors[*].advisorId.string())");
        indexes.add("CREATE MULTIVALUE INDEX idx_bri_adv_ctx ON book_role_investor b (b.data.entitlements.advisoryContext[*].string())");
        indexes.add("CREATE MULTIVALUE INDEX idx_bri_party ON book_role_investor b (b.data.entitlements.pxPartyRoleIdList[*].number())");
        indexes.add("CREATE INDEX idx_bri_party_role ON book_role_investor (json_value(data, '$.partyRoleId' RETURNING NUMBER))");

        // BookRoleGroup indexes (Q5-Q6)
        indexes.add("CREATE MULTIVALUE INDEX idx_brg_adv_ctx ON book_role_group b (b.data.entitlements.advisoryContext[*].string())");
        indexes.add("CREATE INDEX idx_brg_owner ON book_role_group (json_value(data, '$.dataOwnerPartyRoleId' RETURNING NUMBER))");
        indexes.add("CREATE MULTIVALUE INDEX idx_brg_persona ON book_role_group b (b.data.personaNm[*].string())");
        indexes.add("""
                CREATE INDEX idx_brg_mkt_val ON book_role_group (
                    json_value(data, '$.totalViewableAccountsMarketValue' RETURNING NUMBER))""");
        indexes.add("CREATE MULTIVALUE INDEX idx_brg_party ON book_role_group b (b.data.entitlements.pxPartyRoleIdList[*].number())");

        // Account indexes (Q7)
        indexes.add("CREATE MULTIVALUE INDEX idx_acct_party ON account a (a.data.entitlements.pxPartyRoleIdList[*].number())");
        indexes.add("CREATE INDEX idx_acct_viewable ON account (json_value(data, '$.viewableSource' RETURNING VARCHAR2(5)))");
        indexes.add("CREATE MULTIVALUE INDEX idx_acct_ticker ON account a (a.data.holdings[*].fundTicker.string())");

        // Advisor indexes (Q8-Q9)
        indexes.add("CREATE INDEX idx_adv_owner ON advisor (json_value(data, '$.entitlements.pxClient.dataOwnerPartyRoleId' RETURNING NUMBER))");
        indexes.add("CREATE MULTIVALUE INDEX idx_adv_hier ON advisor a (a.data.advisorHierarchy[*].partyNodePathValue.string())");
        indexes.add("CREATE MULTIVALUE INDEX idx_adv_party ON advisor a (a.data.entitlements.pxPartyRoleIdList[*].number())");
        indexes.add("CREATE INDEX idx_adv_mkt_val ON advisor (json_value(data, '$.accountViewableMarketValue' RETURNING NUMBER))");

        return indexes;
    }

    private List<String> normalizedIndexes() {
        List<String> indexes = new ArrayList<>();

        indexes.add("CREATE INDEX idx_helix_type ON helix (json_value(data, '$.type' RETURNING VARCHAR2(30)))");
        indexes.add("""
                CREATE INDEX idx_helix_type_advid ON helix (
                    json_value(data, '$.type' RETURNING VARCHAR2(30)),
                    json_value(data, '$.advisorId' RETURNING VARCHAR2(30)))""");
        indexes.add("""
                CREATE INDEX idx_helix_inv_filter ON helix (
                    json_value(data, '$.type' RETURNING VARCHAR2(30)),
                    json_value(data, '$.investorType' RETURNING VARCHAR2(20)),
                    json_value(data, '$.viewableSource' RETURNING VARCHAR2(5)))""");
        indexes.add("CREATE MULTIVALUE INDEX idx_helix_advisor_ids ON helix h (h.data.advisorIds[*].string())");
        indexes.add("CREATE MULTIVALUE INDEX idx_helix_adv_ctx ON helix h (h.data.entitlements.advisoryContext[*].string())");
        indexes.add("CREATE MULTIVALUE INDEX idx_helix_party_roles ON helix h (h.data.entitlements.pxPartyRoleIdList[*].number())");
        indexes.add("CREATE MULTIVALUE INDEX idx_helix_hier_nm ON helix h (h.data.advisorHierarchy[*].partyNodePathNm.string())");
        indexes.add("CREATE MULTIVALUE INDEX idx_helix_hier_val ON helix h (h.data.advisorHierarchy[*].partyNodePathValue.string())");
        indexes.add("""
                CREATE INDEX idx_helix_mkt_val ON helix (
                    json_value(data, '$.type' RETURNING VARCHAR2(30)),
                    json_value(data, '$.accountViewableMarketValue' RETURNING NUMBER))""");
        indexes.add("CREATE MULTIVALUE INDEX idx_helix_persona ON helix h (h.data.personaNm[*].string())");
        indexes.add("CREATE MULTIVALUE INDEX idx_helix_fund_ticker ON helix h (h.data.holdings[*].fundTicker.string())");

        return indexes;
    }
}
