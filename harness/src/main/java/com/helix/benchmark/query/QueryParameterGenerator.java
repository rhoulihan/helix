package com.helix.benchmark.query;

import com.helix.benchmark.datagen.ReferenceRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class QueryParameterGenerator {
    private final ReferenceRegistry registry;

    private static final String[] SEARCH_TERMS = {
            "smith", "johnson", "williams", "brown", "jones", "garcia", "miller",
            "davis", "rodriguez", "martinez", "wilson", "anderson", "thomas"
    };

    public QueryParameterGenerator(ReferenceRegistry registry) {
        this.registry = registry;
    }

    public Map<String, Object> generate(QueryDefinition query) {
        return switch (query) {
            case Q1 -> generateQ1();
            case Q2 -> generateQ2();
            case Q3 -> generateQ3();
            case Q4 -> generateQ4();
            case Q5 -> generateQ5();
            case Q6 -> generateQ6();
            case Q7 -> generateQ7();
            case Q8 -> generateQ8();
            case Q9 -> generateQ9();
        };
    }

    private Map<String, Object> generateQ1() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisorId", registry.randomAdvisorId());
        return params;
    }

    private Map<String, Object> generateQ2() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisorId", registry.randomAdvisorId());
        params.put("partyRoleId", registry.randomPartyRoleId());
        params.put("searchTerm", SEARCH_TERMS[ThreadLocalRandom.current().nextInt(SEARCH_TERMS.length)]);
        return params;
    }

    private Map<String, Object> generateQ3() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisorId", registry.randomAdvisorId());
        params.put("advisoryContext", registry.randomAdvisoryContextId());
        params.put("dataOwnerPartyRoleId", registry.randomFinInstId());
        return params;
    }

    private Map<String, Object> generateQ4() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisorId", registry.randomAdvisorId());
        double min = ThreadLocalRandom.current().nextDouble(1.0, 1000.0);
        double max = min + ThreadLocalRandom.current().nextDouble(1000.0, 50000.0);
        params.put("minMarketValue", min);
        params.put("maxMarketValue", max);
        return params;
    }

    private Map<String, Object> generateQ5() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisoryContext", registry.randomAdvisoryContextId());
        params.put("dataOwnerPartyRoleId", registry.randomFinInstId());
        params.put("personaNm", "Home Office");
        double min = ThreadLocalRandom.current().nextDouble(0.0, 1000.0);
        double max = min + ThreadLocalRandom.current().nextDouble(10000.0, 100000.0);
        params.put("minMarketValue", min);
        params.put("maxMarketValue", max);
        return params;
    }

    private Map<String, Object> generateQ6() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisoryContext", registry.randomAdvisoryContextId());
        params.put("pxPartyRoleId", registry.randomPartyRoleId());
        double min = ThreadLocalRandom.current().nextDouble(0.0, 100.0);
        double max = min + ThreadLocalRandom.current().nextDouble(100.0, 10000.0);
        params.put("minMarketValue", min);
        params.put("maxMarketValue", max);
        return params;
    }

    private Map<String, Object> generateQ7() {
        Map<String, Object> params = new HashMap<>();
        params.put("pxPartyRoleId", registry.randomPartyRoleId());
        params.put("fundTicker", registry.randomFundTicker());
        return params;
    }

    private Map<String, Object> generateQ8() {
        Map<String, Object> params = new HashMap<>();
        params.put("dataOwnerPartyRoleId", registry.randomFinInstId());
        params.put("partyNodePathValue", registry.randomHierarchyPathValue());
        return params;
    }

    private Map<String, Object> generateQ9() {
        Map<String, Object> params = new HashMap<>();
        params.put("pxPartyRoleId", registry.randomPartyRoleId());
        double min = ThreadLocalRandom.current().nextDouble(0.0, 1_000_000.0);
        double max = min + ThreadLocalRandom.current().nextDouble(1_000_000.0, 40_000_000.0);
        params.put("minMarketValue", min);
        params.put("maxMarketValue", max);
        return params;
    }
}
