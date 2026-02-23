package com.helix.benchmark.schema;

import com.helix.benchmark.config.SchemaModel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MongoSchemaManager implements SchemaManager {

    public record IndexDefinition(String collection, Map<String, Object> keys) {}

    @Override
    public List<String> getCollectionNames(SchemaModel model) {
        return model.collectionNames();
    }

    public List<IndexDefinition> getIndexDefinitions(SchemaModel model) {
        return embeddedIndexes();
    }

    private List<IndexDefinition> embeddedIndexes() {
        List<IndexDefinition> indexes = new ArrayList<>();

        // BookRoleInvestor (Q1-Q4)
        indexes.add(idx("bookRoleInvestor", "investorType", 1, "viewableSource", 1));
        indexes.add(idx("bookRoleInvestor", "advisors.advisorId", 1, "advisors.noOfViewableAccts", 1));
        indexes.add(idx("bookRoleInvestor", "entitlements.advisoryContext", 1));
        indexes.add(idx("bookRoleInvestor", "entitlements.pxClient.dataOwnerPartyRoleId", 1));
        indexes.add(idx("bookRoleInvestor", "partyRoleId", 1));
        indexes.add(idx("bookRoleInvestor", "entitlements.pxPartyRoleIdList", 1));
        indexes.add(idx("bookRoleInvestor", "advisorHierarchy.partyNodePathNm", 1, "advisorHierarchy.partyNodePathValue", 1));

        // BookRoleGroup (Q5-Q6)
        indexes.add(idx("bookRoleGroup", "entitlements.advisoryContext", 1));
        indexes.add(idx("bookRoleGroup", "dataOwnerPartyRoleId", 1, "personaNm", 1));
        indexes.add(idx("bookRoleGroup", "totalViewableAccountsMarketValue", 1));
        indexes.add(idx("bookRoleGroup", "entitlements.pxPartyRoleIdList", 1));
        indexes.add(idx("bookRoleGroup", "advisorHierarchy.partyNodePathNm", 1, "advisorHierarchy.partyNodePathValue", 1));

        // Account (Q7)
        indexes.add(idx("account", "entitlements.pxPartyRoleIdList", 1, "viewableSource", 1));
        indexes.add(idx("account", "holdings.fundTicker", 1));
        indexes.add(idx("account", "advisorHierarchy.partyNodepathNm", 1, "advisorHierarchy.partyNodePathValue", 1));

        // Advisor (Q8-Q9)
        indexes.add(idx("advisor", "entitlements.pxClient.dataOwnerPartyRoleId", 1));
        indexes.add(idx("advisor", "advisorHierarchy.partyNodePathValue", 1));
        indexes.add(idx("advisor", "entitlements.pxPartyRoleIdList", 1));
        indexes.add(idx("advisor", "accountViewableMarketValue", 1));
        indexes.add(idx("advisor", "advisorHierarchy.partyNodePathNm", 1, "advisorHierarchy.partyNodePathValue", 1));

        return indexes;
    }

    private IndexDefinition idx(String collection, Object... keysAndValues) {
        Map<String, Object> keys = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            keys.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return new IndexDefinition(collection, keys);
    }
}
