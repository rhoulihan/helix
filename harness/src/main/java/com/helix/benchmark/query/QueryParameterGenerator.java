package com.helix.benchmark.query;

import com.helix.benchmark.datagen.ReferenceRegistry;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class QueryParameterGenerator {
    private static final Logger log = LoggerFactory.getLogger(QueryParameterGenerator.class);
    private final ReferenceRegistry registry;

    // Pre-sampled parameter sets from actual data (populated by initFromData)
    private final Map<QueryDefinition, List<Map<String, Object>>> sampledParams = new EnumMap<>(QueryDefinition.class);

    public QueryParameterGenerator(ReferenceRegistry registry) {
        this.registry = registry;
    }

    /**
     * Pre-sample parameter combinations from actual MongoDB data.
     * This ensures that generated parameters will match existing documents.
     */
    public void initFromData(MongoDatabase db, int samplesPerQuery) {
        log.info("Sampling query parameters from actual data ({} per query)...", samplesPerQuery);

        sampleQ1Params(db, samplesPerQuery);
        sampleQ2Params(db, samplesPerQuery);
        sampleQ3Params(db, samplesPerQuery);
        sampleQ4Params(db, samplesPerQuery);
        sampleQ5Params(db, samplesPerQuery);
        sampleQ6Params(db, samplesPerQuery);
        sampleQ7Params(db, samplesPerQuery);
        sampleQ8Params(db, samplesPerQuery);
        sampleQ9Params(db, samplesPerQuery);

        for (QueryDefinition q : QueryDefinition.values()) {
            int count = sampledParams.getOrDefault(q, List.of()).size();
            log.info("  {} — {} parameter sets sampled", q.queryName(), count);
        }
    }

    public Map<String, Object> generate(QueryDefinition query) {
        List<Map<String, Object>> sampled = sampledParams.get(query);
        if (sampled != null && !sampled.isEmpty()) {
            return sampled.get(ThreadLocalRandom.current().nextInt(sampled.size()));
        }
        // Fallback to random generation if no sampled data
        return generateRandom(query);
    }

    private Map<String, Object> generateRandom(QueryDefinition query) {
        return switch (query) {
            case Q1 -> generateQ1Random();
            case Q2 -> generateQ2Random();
            case Q3 -> generateQ3Random();
            case Q4 -> generateQ4Random();
            case Q5 -> generateQ5Random();
            case Q6 -> generateQ6Random();
            case Q7 -> generateQ7Random();
            case Q8 -> generateQ8Random();
            case Q9 -> generateQ9Random();
        };
    }

    // === Pre-sampling from actual MongoDB data ===

    private void sampleQ1Params(MongoDatabase db, int count) {
        // Q1: advisorId — sample investors that have advisors with noOfViewableAccts >= 1
        List<Map<String, Object>> params = new ArrayList<>();
        try {
            List<Document> docs = db.getCollection("bookRoleInvestor")
                    .aggregate(List.of(
                            Aggregates.match(new Document("investorType", "Client")
                                    .append("viewableSource", "Y")),
                            Aggregates.sample(count * 2)
                    )).into(new ArrayList<>());
            Set<String> seen = new HashSet<>();
            for (Document doc : docs) {
                List<Document> advisors = doc.getList("advisors", Document.class);
                if (advisors != null) {
                    for (Document adv : advisors) {
                        Number accts = (Number) adv.get("noOfViewableAccts");
                        if (accts != null && accts.longValue() >= 1) {
                            String advisorId = adv.getString("advisorId");
                            if (advisorId != null && seen.add(advisorId)) {
                                Map<String, Object> p = new HashMap<>();
                                p.put("advisorId", advisorId);
                                params.add(p);
                                if (params.size() >= count) break;
                            }
                        }
                    }
                }
                if (params.size() >= count) break;
            }
        } catch (Exception e) {
            log.warn("Failed to sample Q1 params: {}", e.getMessage());
        }
        sampledParams.put(QueryDefinition.Q1, params);
    }

    private void sampleQ2Params(MongoDatabase db, int count) {
        // Q2: partyRoleId (exact) + investorFullName regex + advisorId
        // Sample investors that match investorType=Client, viewableFlag=Y
        List<Map<String, Object>> params = new ArrayList<>();
        try {
            List<Document> docs = db.getCollection("bookRoleInvestor")
                    .aggregate(List.of(
                            Aggregates.match(new Document("investorType", "Client")
                                    .append("viewableFlag", "Y")),
                            Aggregates.sample(count * 3)
                    )).into(new ArrayList<>());
            for (Document doc : docs) {
                Number partyRoleId = (Number) doc.get("partyRoleId");
                String fullName = doc.getString("investorFullName");
                String lastName = doc.getString("investorLastName");
                List<Document> advisors = doc.getList("advisors", Document.class);
                if (partyRoleId != null && fullName != null && advisors != null && !advisors.isEmpty()) {
                    for (Document adv : advisors) {
                        Number accts = (Number) adv.get("noOfViewableAccts");
                        if (accts != null && accts.longValue() >= 1) {
                            Map<String, Object> p = new HashMap<>();
                            p.put("advisorId", adv.getString("advisorId"));
                            p.put("partyRoleId", partyRoleId.longValue());
                            // Use the actual last name as the search term (case-insensitive regex)
                            p.put("searchTerm", lastName != null ? lastName.toLowerCase() : fullName.split(" ")[1].toLowerCase());
                            params.add(p);
                            break; // one param set per document
                        }
                    }
                }
                if (params.size() >= count) break;
            }
        } catch (Exception e) {
            log.warn("Failed to sample Q2 params: {}", e.getMessage());
        }
        sampledParams.put(QueryDefinition.Q2, params);
    }

    private void sampleQ3Params(MongoDatabase db, int count) {
        // Q3: advisoryContext + dataOwnerPartyRoleId (entitlements.pxClient) + advisorId
        List<Map<String, Object>> params = new ArrayList<>();
        try {
            List<Document> docs = db.getCollection("bookRoleInvestor")
                    .aggregate(List.of(
                            Aggregates.match(new Document("investorType", "Client")
                                    .append("viewableSource", "Y")),
                            Aggregates.sample(count * 3)
                    )).into(new ArrayList<>());
            for (Document doc : docs) {
                Document entitlements = doc.get("entitlements", Document.class);
                List<Document> advisors = doc.getList("advisors", Document.class);
                if (entitlements != null && advisors != null && !advisors.isEmpty()) {
                    List<String> contexts = entitlements.getList("advisoryContext", String.class);
                    Document pxClient = entitlements.get("pxClient", Document.class);
                    if (contexts != null && !contexts.isEmpty() && pxClient != null) {
                        Number dataOwnerPrId = (Number) pxClient.get("dataOwnerPartyRoleId");
                        if (dataOwnerPrId != null) {
                            for (Document adv : advisors) {
                                Number accts = (Number) adv.get("noOfViewableAccts");
                                if (accts != null && accts.longValue() >= 1) {
                                    Map<String, Object> p = new HashMap<>();
                                    p.put("advisorId", adv.getString("advisorId"));
                                    p.put("advisoryContext", contexts.get(0));
                                    p.put("dataOwnerPartyRoleId", dataOwnerPrId.longValue());
                                    params.add(p);
                                    break;
                                }
                            }
                        }
                    }
                }
                if (params.size() >= count) break;
            }
        } catch (Exception e) {
            log.warn("Failed to sample Q3 params: {}", e.getMessage());
        }
        sampledParams.put(QueryDefinition.Q3, params);
    }

    private void sampleQ4Params(MongoDatabase db, int count) {
        // Q4: advisorId + market value range around actual advisor's viewableMarketValue
        List<Map<String, Object>> params = new ArrayList<>();
        try {
            List<Document> docs = db.getCollection("bookRoleInvestor")
                    .aggregate(List.of(
                            Aggregates.match(new Document("investorType", "Client")
                                    .append("viewableSource", "Y")),
                            Aggregates.sample(count * 2)
                    )).into(new ArrayList<>());
            for (Document doc : docs) {
                List<Document> advisors = doc.getList("advisors", Document.class);
                if (advisors != null) {
                    for (Document adv : advisors) {
                        Number accts = (Number) adv.get("noOfViewableAccts");
                        Number mv = (Number) adv.get("viewableMarketValue");
                        if (accts != null && accts.longValue() >= 1 && mv != null) {
                            double marketValue = mv.doubleValue();
                            Map<String, Object> p = new HashMap<>();
                            p.put("advisorId", adv.getString("advisorId"));
                            // Create a range around the actual value to get multiple results
                            p.put("minMarketValue", Math.max(0, marketValue - 5_000_000));
                            p.put("maxMarketValue", marketValue + 5_000_000);
                            params.add(p);
                            break;
                        }
                    }
                }
                if (params.size() >= count) break;
            }
        } catch (Exception e) {
            log.warn("Failed to sample Q4 params: {}", e.getMessage());
        }
        sampledParams.put(QueryDefinition.Q4, params);
    }

    private void sampleQ5Params(MongoDatabase db, int count) {
        // Q5: advisoryContext + dataOwnerPartyRoleId + personaNm + market value range
        List<Map<String, Object>> params = new ArrayList<>();
        try {
            List<Document> docs = db.getCollection("bookRoleGroup")
                    .aggregate(List.of(
                            Aggregates.match(new Document("visibleFlag", new Document("$ne", "N"))),
                            Aggregates.sample(count * 3)
                    )).into(new ArrayList<>());
            for (Document doc : docs) {
                Document entitlements = doc.get("entitlements", Document.class);
                Number dataOwnerPrId = (Number) doc.get("dataOwnerPartyRoleId");
                List<String> personas = doc.getList("personaNm", String.class);
                Number mvNum = (Number) doc.get("totalViewableAccountsMarketValue");
                if (entitlements != null && dataOwnerPrId != null && personas != null && !personas.isEmpty() && mvNum != null) {
                    List<String> contexts = entitlements.getList("advisoryContext", String.class);
                    if (contexts != null && !contexts.isEmpty()) {
                        double mv = mvNum.doubleValue();
                        Map<String, Object> p = new HashMap<>();
                        p.put("advisoryContext", contexts.get(0));
                        p.put("dataOwnerPartyRoleId", dataOwnerPrId.longValue());
                        p.put("personaNm", personas.get(0));
                        p.put("minMarketValue", Math.max(0, mv - 10_000_000));
                        p.put("maxMarketValue", mv + 10_000_000);
                        params.add(p);
                    }
                }
                if (params.size() >= count) break;
            }
        } catch (Exception e) {
            log.warn("Failed to sample Q5 params: {}", e.getMessage());
        }
        sampledParams.put(QueryDefinition.Q5, params);
    }

    private void sampleQ6Params(MongoDatabase db, int count) {
        // Q6: advisoryContext + pxPartyRoleId + market value range
        List<Map<String, Object>> params = new ArrayList<>();
        try {
            List<Document> docs = db.getCollection("bookRoleGroup")
                    .aggregate(List.of(
                            Aggregates.match(new Document("visibleFlag", new Document("$ne", "N"))),
                            Aggregates.sample(count * 3)
                    )).into(new ArrayList<>());
            for (Document doc : docs) {
                Document entitlements = doc.get("entitlements", Document.class);
                Number mvNum = (Number) doc.get("totalViewableAccountsMarketValue");
                if (entitlements != null && mvNum != null) {
                    List<String> contexts = entitlements.getList("advisoryContext", String.class);
                    List<Number> partyRoleIds = entitlements.getList("pxPartyRoleIdList", Number.class);
                    if (contexts != null && !contexts.isEmpty() && partyRoleIds != null && !partyRoleIds.isEmpty()) {
                        double mv = mvNum.doubleValue();
                        Map<String, Object> p = new HashMap<>();
                        p.put("advisoryContext", contexts.get(0));
                        p.put("pxPartyRoleId", partyRoleIds.get(0).longValue());
                        p.put("minMarketValue", Math.max(0, mv - 5_000_000));
                        p.put("maxMarketValue", mv + 5_000_000);
                        params.add(p);
                    }
                }
                if (params.size() >= count) break;
            }
        } catch (Exception e) {
            log.warn("Failed to sample Q6 params: {}", e.getMessage());
        }
        sampledParams.put(QueryDefinition.Q6, params);
    }

    private void sampleQ7Params(MongoDatabase db, int count) {
        // Q7: pxPartyRoleId + fundTicker — from actual account documents
        List<Map<String, Object>> params = new ArrayList<>();
        try {
            List<Document> docs = db.getCollection("account")
                    .aggregate(List.of(
                            Aggregates.match(new Document("viewableSource", "Y")),
                            Aggregates.sample(count * 3)
                    )).into(new ArrayList<>());
            for (Document doc : docs) {
                Document entitlements = doc.get("entitlements", Document.class);
                List<Document> holdings = doc.getList("holdings", Document.class);
                if (entitlements != null && holdings != null && !holdings.isEmpty()) {
                    List<Number> partyRoleIds = entitlements.getList("pxPartyRoleIdList", Number.class);
                    if (partyRoleIds != null && !partyRoleIds.isEmpty()) {
                        String ticker = holdings.get(0).getString("fundTicker");
                        if (ticker != null) {
                            Map<String, Object> p = new HashMap<>();
                            p.put("pxPartyRoleId", partyRoleIds.get(0).longValue());
                            p.put("fundTicker", ticker);
                            params.add(p);
                        }
                    }
                }
                if (params.size() >= count) break;
            }
        } catch (Exception e) {
            log.warn("Failed to sample Q7 params: {}", e.getMessage());
        }
        sampledParams.put(QueryDefinition.Q7, params);
    }

    private void sampleQ8Params(MongoDatabase db, int count) {
        // Q8: dataOwnerPartyRoleId + partyNodePathValue — from actual advisor documents
        List<Map<String, Object>> params = new ArrayList<>();
        try {
            List<Document> docs = db.getCollection("advisor")
                    .aggregate(List.of(Aggregates.sample(count * 2)))
                    .into(new ArrayList<>());
            for (Document doc : docs) {
                Document entitlements = doc.get("entitlements", Document.class);
                List<Document> hierarchy = doc.getList("advisorHierarchy", Document.class);
                if (entitlements != null && hierarchy != null && !hierarchy.isEmpty()) {
                    Document pxClient = entitlements.get("pxClient", Document.class);
                    if (pxClient != null) {
                        Number dataOwnerPrId = (Number) pxClient.get("dataOwnerPartyRoleId");
                        String pathValue = hierarchy.get(0).getString("partyNodePathValue");
                        if (dataOwnerPrId != null && pathValue != null) {
                            Map<String, Object> p = new HashMap<>();
                            p.put("dataOwnerPartyRoleId", dataOwnerPrId.longValue());
                            p.put("partyNodePathValue", pathValue);
                            params.add(p);
                        }
                    }
                }
                if (params.size() >= count) break;
            }
        } catch (Exception e) {
            log.warn("Failed to sample Q8 params: {}", e.getMessage());
        }
        sampledParams.put(QueryDefinition.Q8, params);
    }

    private void sampleQ9Params(MongoDatabase db, int count) {
        // Q9: pxPartyRoleId + market value range — from actual advisor documents
        List<Map<String, Object>> params = new ArrayList<>();
        try {
            List<Document> docs = db.getCollection("advisor")
                    .aggregate(List.of(Aggregates.sample(count * 2)))
                    .into(new ArrayList<>());
            for (Document doc : docs) {
                Document entitlements = doc.get("entitlements", Document.class);
                Number mvNum = (Number) doc.get("accountViewableMarketValue");
                if (entitlements != null && mvNum != null) {
                    List<Number> partyRoleIds = entitlements.getList("pxPartyRoleIdList", Number.class);
                    if (partyRoleIds != null && !partyRoleIds.isEmpty()) {
                        double mv = mvNum.doubleValue();
                        Map<String, Object> p = new HashMap<>();
                        p.put("pxPartyRoleId", partyRoleIds.get(0).longValue());
                        p.put("minMarketValue", Math.max(0, mv - 10_000_000));
                        p.put("maxMarketValue", mv + 10_000_000);
                        params.add(p);
                    }
                }
                if (params.size() >= count) break;
            }
        } catch (Exception e) {
            log.warn("Failed to sample Q9 params: {}", e.getMessage());
        }
        sampledParams.put(QueryDefinition.Q9, params);
    }

    // === Fallback: random generation (original logic) ===

    private static final String[] SEARCH_TERMS = {
            "smith", "johnson", "williams", "brown", "jones", "garcia", "miller",
            "davis", "rodriguez", "martinez", "wilson", "anderson", "thomas"
    };

    private Map<String, Object> generateQ1Random() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisorId", registry.randomAdvisorId());
        return params;
    }

    private Map<String, Object> generateQ2Random() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisorId", registry.randomAdvisorId());
        params.put("partyRoleId", registry.randomPartyRoleId());
        params.put("searchTerm", SEARCH_TERMS[ThreadLocalRandom.current().nextInt(SEARCH_TERMS.length)]);
        return params;
    }

    private Map<String, Object> generateQ3Random() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisorId", registry.randomAdvisorId());
        params.put("advisoryContext", registry.randomAdvisoryContextId());
        params.put("dataOwnerPartyRoleId", registry.randomFinInstId());
        return params;
    }

    private Map<String, Object> generateQ4Random() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisorId", registry.randomAdvisorId());
        double min = ThreadLocalRandom.current().nextDouble(1.0, 1000.0);
        double max = min + ThreadLocalRandom.current().nextDouble(1000.0, 50000.0);
        params.put("minMarketValue", min);
        params.put("maxMarketValue", max);
        return params;
    }

    private Map<String, Object> generateQ5Random() {
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

    private Map<String, Object> generateQ6Random() {
        Map<String, Object> params = new HashMap<>();
        params.put("advisoryContext", registry.randomAdvisoryContextId());
        params.put("pxPartyRoleId", registry.randomPartyRoleId());
        double min = ThreadLocalRandom.current().nextDouble(0.0, 100.0);
        double max = min + ThreadLocalRandom.current().nextDouble(100.0, 10000.0);
        params.put("minMarketValue", min);
        params.put("maxMarketValue", max);
        return params;
    }

    private Map<String, Object> generateQ7Random() {
        Map<String, Object> params = new HashMap<>();
        params.put("pxPartyRoleId", registry.randomPartyRoleId());
        params.put("fundTicker", registry.randomFundTicker());
        return params;
    }

    private Map<String, Object> generateQ8Random() {
        Map<String, Object> params = new HashMap<>();
        params.put("dataOwnerPartyRoleId", registry.randomFinInstId());
        params.put("partyNodePathValue", registry.randomHierarchyPathValue());
        return params;
    }

    private Map<String, Object> generateQ9Random() {
        Map<String, Object> params = new HashMap<>();
        params.put("pxPartyRoleId", registry.randomPartyRoleId());
        double min = ThreadLocalRandom.current().nextDouble(0.0, 1_000_000.0);
        double max = min + ThreadLocalRandom.current().nextDouble(1_000_000.0, 40_000_000.0);
        params.put("minMarketValue", min);
        params.put("maxMarketValue", max);
        return params;
    }
}
