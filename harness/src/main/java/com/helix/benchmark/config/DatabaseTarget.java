package com.helix.benchmark.config;

public enum DatabaseTarget {
    MONGO_NATIVE("Native MongoDB 8.2", true, false),
    ORACLE_JDBC("Oracle 26ai JDBC", false, true),
    ORACLE_MONGO_API("Oracle 26ai MongoDB API", true, false);

    private final String displayName;
    private final boolean usesMongoDriver;
    private final boolean usesJdbc;

    DatabaseTarget(String displayName, boolean usesMongoDriver, boolean usesJdbc) {
        this.displayName = displayName;
        this.usesMongoDriver = usesMongoDriver;
        this.usesJdbc = usesJdbc;
    }

    public String displayName() {
        return displayName;
    }

    public boolean usesMongoDriver() {
        return usesMongoDriver;
    }

    public boolean usesJdbc() {
        return usesJdbc;
    }
}
