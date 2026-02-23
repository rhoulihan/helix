package com.helix.benchmark.benchmark;

public record QueryDetail(
        String queryName,
        String configurationId,
        String queryText,
        String explainPlan,
        String sqlId,
        String ordsBaseUrl
) {
    public String activeSqlMonitorUrl() {
        if (sqlId == null || ordsBaseUrl == null) return null;
        return ordsBaseUrl + "/_/sql/" + sqlId;
    }
}
