package com.helix.benchmark.schema;

import com.helix.benchmark.config.SchemaModel;

import java.util.ArrayList;
import java.util.List;

public class OracleSchemaManager implements SchemaManager {

    @Override
    public List<String> getCollectionNames(SchemaModel model) {
        return getTableNames(model);
    }

    public List<String> getTableNames(SchemaModel model) {
        return List.of("jdbc_account", "jdbc_advisor", "jdbc_book_role_group", "jdbc_book_role_investor");
    }

    public List<String> getCreateTableStatements(SchemaModel model) {
        return getTableNames(model).stream()
                .map(t -> "CREATE JSON COLLECTION TABLE " + t)
                .toList();
    }

    public List<String> getIndexStatements(SchemaModel model) {
        return embeddedIndexes();
    }

    private List<String> embeddedIndexes() {
        List<String> indexes = new ArrayList<>();

        // BookRoleInvestor indexes (Q1-Q4)
        indexes.add("""
                CREATE INDEX idx_bri_inv_type ON jdbc_book_role_investor (
                    json_value(data, '$.investorType' RETURNING VARCHAR2(20)),
                    json_value(data, '$.viewableSource' RETURNING VARCHAR2(5)))""");
        // Compound multivalue index must be created via MongoDB API:
        // db.jdbc_book_role_investor.createIndex({"advisors.advisorId": 1, "advisors.noOfViewableAccts": 1})
        indexes.add("CREATE MULTIVALUE INDEX idx_bri_adv_id ON jdbc_book_role_investor b (b.data.advisors[*].advisorId.string())");
        indexes.add("CREATE MULTIVALUE INDEX idx_bri_adv_ctx ON jdbc_book_role_investor b (b.data.entitlements.advisoryContext[*].string())");
        indexes.add("CREATE MULTIVALUE INDEX idx_bri_party ON jdbc_book_role_investor b (b.data.entitlements.pxPartyRoleIdList[*].number())");
        indexes.add("CREATE INDEX idx_bri_party_role ON jdbc_book_role_investor (json_value(data, '$.partyRoleId' RETURNING NUMBER))");


        // BookRoleGroup indexes (Q5-Q6)
        indexes.add("CREATE MULTIVALUE INDEX idx_brg_adv_ctx ON jdbc_book_role_group b (b.data.entitlements.advisoryContext[*].string())");
        indexes.add("CREATE INDEX idx_brg_owner ON jdbc_book_role_group (json_value(data, '$.dataOwnerPartyRoleId' RETURNING NUMBER))");
        indexes.add("CREATE MULTIVALUE INDEX idx_brg_persona ON jdbc_book_role_group b (b.data.personaNm[*].string())");
        indexes.add("""
                CREATE INDEX idx_brg_mkt_val ON jdbc_book_role_group (
                    json_value(data, '$.totalViewableAccountsMarketValue' RETURNING NUMBER))""");
        indexes.add("CREATE MULTIVALUE INDEX idx_brg_party ON jdbc_book_role_group b (b.data.entitlements.pxPartyRoleIdList[*].number())");

        // Account indexes (Q7)
        indexes.add("CREATE MULTIVALUE INDEX idx_acct_party ON jdbc_account a (a.data.entitlements.pxPartyRoleIdList[*].number())");
        indexes.add("CREATE INDEX idx_acct_viewable ON jdbc_account (json_value(data, '$.viewableSource' RETURNING VARCHAR2(5)))");
        indexes.add("CREATE MULTIVALUE INDEX idx_acct_ticker ON jdbc_account a (a.data.holdings[*].fundTicker.string())");

        // Advisor indexes (Q8-Q9)
        indexes.add("CREATE INDEX idx_adv_owner ON jdbc_advisor (json_value(data, '$.entitlements.pxClient.dataOwnerPartyRoleId' RETURNING NUMBER))");
        indexes.add("CREATE MULTIVALUE INDEX idx_adv_hier ON jdbc_advisor a (a.data.advisorHierarchy[*].partyNodePathValue.string())");
        indexes.add("CREATE MULTIVALUE INDEX idx_adv_party ON jdbc_advisor a (a.data.entitlements.pxPartyRoleIdList[*].number())");
        indexes.add("CREATE INDEX idx_adv_mkt_val ON jdbc_advisor (json_value(data, '$.accountViewableMarketValue' RETURNING NUMBER))");

        return indexes;
    }
}
