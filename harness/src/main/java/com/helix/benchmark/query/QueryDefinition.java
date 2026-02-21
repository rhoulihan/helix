package com.helix.benchmark.query;

public enum QueryDefinition {
    Q1("Q1", "BookRoleInvestor: Investor list by advisor",
            "bookRoleInvestor", true),
    Q2("Q2", "BookRoleInvestor: Investor search by name with regex",
            "bookRoleInvestor", true),
    Q3("Q3", "BookRoleInvestor: Investor list by entitlements + advisor",
            "bookRoleInvestor", true),
    Q4("Q4", "BookRoleInvestor: Investor list by advisor with market value range",
            "bookRoleInvestor", true),
    Q5("Q5", "BookRoleGroup: Filter by entitlements, persona, market value",
            "bookRoleGroup", false),
    Q6("Q6", "BookRoleGroup: Filter by entitlements + pxPartyRoleId + market value",
            "bookRoleGroup", false),
    Q7("Q7", "Account: Holdings fund ticker filter",
            "account", false),
    Q8("Q8", "Advisor: Hierarchy path filter",
            "advisor", false),
    Q9("Q9", "Advisor: Market value range filter",
            "advisor", false);

    private final String queryName;
    private final String description;
    private final String embeddedCollection;
    private final boolean isAggregation;

    QueryDefinition(String queryName, String description, String embeddedCollection, boolean isAggregation) {
        this.queryName = queryName;
        this.description = description;
        this.embeddedCollection = embeddedCollection;
        this.isAggregation = isAggregation;
    }

    public String queryName() {
        return queryName;
    }

    public String description() {
        return description;
    }

    public String embeddedCollection() {
        return embeddedCollection;
    }

    public boolean isAggregation() {
        return isAggregation;
    }
}
