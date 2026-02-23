package com.helix.benchmark.config;

import java.util.List;

public enum SchemaModel {
    EMBEDDED("Embedded (4 collections)", 4,
            List.of("account", "advisor", "bookRoleGroup", "bookRoleInvestor"));

    private final String displayName;
    private final int collectionCount;
    private final List<String> collectionNames;

    SchemaModel(String displayName, int collectionCount, List<String> collectionNames) {
        this.displayName = displayName;
        this.collectionCount = collectionCount;
        this.collectionNames = collectionNames;
    }

    public String displayName() {
        return displayName;
    }

    public int collectionCount() {
        return collectionCount;
    }

    public List<String> collectionNames() {
        return collectionNames;
    }
}
