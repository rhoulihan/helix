package com.helix.benchmark.query;

import com.helix.benchmark.benchmark.QueryDetail;
import com.helix.benchmark.config.DatabaseTarget;
import com.helix.benchmark.config.SchemaModel;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class MongoQueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(MongoQueryExecutor.class);

    private static final JsonWriterSettings PRETTY_JSON = JsonWriterSettings.builder()
            .indent(true).build();

    public String getCollectionName(QueryDefinition query, SchemaModel model, DatabaseTarget target) {
        if (target == DatabaseTarget.ORACLE_MONGO_API_DV) {
            return switch (query.embeddedCollection()) {
                case "bookRoleInvestor" -> "dv_book_role_investor";
                case "bookRoleGroup" -> "dv_book_role_group";
                case "account" -> "dv_account";
                case "advisor" -> "dv_advisor";
                default -> query.embeddedCollection();
            };
        }
        return query.embeddedCollection();
    }

    public List<Bson> buildAggregationPipeline(QueryDefinition query, SchemaModel model,
                                                Map<String, Object> params, DatabaseTarget target) {
        return switch (query) {
            case Q1 -> buildQ1Pipeline(params, target);
            case Q2 -> buildQ2Pipeline(params, target);
            case Q3 -> buildQ3Pipeline(params, target);
            case Q4 -> buildQ4Pipeline(params, target);
            default -> throw new IllegalArgumentException(query + " is not an aggregation query");
        };
    }

    public Bson buildFindFilter(QueryDefinition query, SchemaModel model,
                                 Map<String, Object> params, DatabaseTarget target) {
        boolean isDvMongo = target == DatabaseTarget.ORACLE_MONGO_API_DV;
        return switch (query) {
            case Q5 -> isDvMongo ? buildQ5FilterDv(params) : buildQ5Filter(params);
            case Q6 -> isDvMongo ? buildQ6FilterDv(params) : buildQ6Filter(params);
            case Q7 -> isDvMongo ? buildQ7FilterDv(params) : buildQ7Filter(params);
            case Q8 -> isDvMongo ? buildQ8FilterDv(params) : buildQ8Filter(params);
            case Q9 -> isDvMongo ? buildQ9FilterDv(params) : buildQ9Filter(params);
            default -> throw new IllegalArgumentException(query + " is not a find query");
        };
    }

    /** @deprecated Use {@link #buildFindFilter(QueryDefinition, SchemaModel, Map, DatabaseTarget)} */
    @Deprecated
    public Bson buildFindFilter(QueryDefinition query, SchemaModel model, Map<String, Object> params) {
        return buildFindFilter(query, model, params, DatabaseTarget.MONGO_NATIVE);
    }

    public long executeAggregation(MongoCollection<Document> collection, QueryDefinition query,
                                    SchemaModel model, Map<String, Object> params, DatabaseTarget target) {
        List<Bson> pipeline = buildAggregationPipeline(query, model, params, target);
        var agg = collection.aggregate(pipeline);
        Bson hint = getAggregationHint(query, target);
        if (hint != null) {
            agg = agg.hint(hint);
        }
        List<Document> results = agg.into(new ArrayList<>());
        return results.size();
    }

    private Bson getAggregationHint(QueryDefinition query, DatabaseTarget target) {
        if (target != DatabaseTarget.ORACLE_MONGO_API) return null;
        // Oracle optimizer picks investorType+viewableSource index by default;
        // the advisorId compound index is far more selective for Q1-Q4
        return new Document("advisors.advisorId", 1).append("advisors.noOfViewableAccts", 1);
    }

    public long executeFind(MongoCollection<Document> collection, QueryDefinition query,
                             SchemaModel model, Map<String, Object> params, DatabaseTarget target) {
        Bson filter = buildFindFilter(query, model, params, target);
        List<Document> results = collection.find(filter).into(new ArrayList<>());
        return results.size();
    }

    /** @deprecated Use {@link #executeFind(MongoCollection, QueryDefinition, SchemaModel, Map, DatabaseTarget)} */
    @Deprecated
    public long executeFind(MongoCollection<Document> collection, QueryDefinition query,
                             SchemaModel model, Map<String, Object> params) {
        return executeFind(collection, query, model, params, DatabaseTarget.MONGO_NATIVE);
    }

    // --- Q1: BookRoleInvestor - Investor list by advisor ---
    private List<Bson> buildQ1Pipeline(Map<String, Object> params, DatabaseTarget target) {
        String advisorId = (String) params.get("advisorId");

        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(Aggregates.match(Filters.and(
                Filters.elemMatch("advisors",
                        Filters.and(Filters.eq("advisorId", advisorId), Filters.gte("noOfViewableAccts", 1))),
                Filters.eq("investorType", "Client"),
                Filters.eq("viewableSource", "Y")
        )));
        pipeline.add(filterAdvisorsStage(advisorId));
        pipeline.add(Aggregates.unwind("$advisors"));
        pipeline.add(matchUnwoundAdvisor(advisorId));
        pipeline.add(Aggregates.project(buildEmbeddedInvestorProjection()));
        if (target == DatabaseTarget.MONGO_NATIVE) {
            pipeline.add(setWindowFieldsCount());
        }
        pipeline.add(Aggregates.sort(Sorts.descending("viewableMarketValue")));
        pipeline.add(Aggregates.limit(50));
        return pipeline;
    }

    // --- Q2: BookRoleInvestor - Search by name with regex ---
    private List<Bson> buildQ2Pipeline(Map<String, Object> params, DatabaseTarget target) {
        String advisorId = (String) params.get("advisorId");
        Long partyRoleId = ((Number) params.get("partyRoleId")).longValue();
        String searchTerm = (String) params.get("searchTerm");

        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(Aggregates.match(Filters.and(
                Filters.elemMatch("advisors",
                        Filters.and(Filters.eq("advisorId", advisorId), Filters.gte("noOfViewableAccts", 1))),
                Filters.eq("investorType", "Client"),
                Filters.eq("viewableFlag", "Y"),
                Filters.eq("partyRoleId", partyRoleId),
                Filters.regex("investorFullName", Pattern.compile(searchTerm, Pattern.CASE_INSENSITIVE))
        )));
        pipeline.add(filterAdvisorsStage(advisorId));
        pipeline.add(Aggregates.unwind("$advisors"));
        pipeline.add(matchUnwoundAdvisor(advisorId));
        pipeline.add(Aggregates.project(buildEmbeddedInvestorProjection()));
        if (target == DatabaseTarget.MONGO_NATIVE) {
            pipeline.add(setWindowFieldsCount());
        }
        pipeline.add(Aggregates.sort(Sorts.descending("viewableMarketValue")));
        pipeline.add(Aggregates.limit(50));
        return pipeline;
    }

    // --- Q3: BookRoleInvestor - Filter by entitlements + advisor ---
    private List<Bson> buildQ3Pipeline(Map<String, Object> params, DatabaseTarget target) {
        String advisorId = (String) params.get("advisorId");
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();

        boolean isDv = target == DatabaseTarget.ORACLE_MONGO_API_DV;

        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(Aggregates.match(Filters.and(
                Filters.elemMatch("advisors",
                        Filters.and(Filters.eq("advisorId", advisorId), Filters.gte("noOfViewableAccts", 1))),
                isDv ? Filters.elemMatch("advisoryContexts", Filters.eq("advisoryContext", advisoryContext))
                     : Filters.eq("entitlements.advisoryContext", advisoryContext),
                Filters.eq(isDv ? "entDataOwnerPartyRoleId" : "entitlements.pxClient.dataOwnerPartyRoleId", dataOwnerPartyRoleId),
                Filters.eq("investorType", "Client"),
                Filters.eq("viewableSource", "Y")
        )));
        pipeline.add(filterAdvisorsStage(advisorId));
        pipeline.add(Aggregates.unwind("$advisors"));
        pipeline.add(matchUnwoundAdvisor(advisorId));
        pipeline.add(Aggregates.project(buildEmbeddedInvestorProjection()));
        if (target == DatabaseTarget.MONGO_NATIVE) {
            pipeline.add(setWindowFieldsCount());
        }
        pipeline.add(Aggregates.sort(Sorts.descending("viewableMarketValue")));
        pipeline.add(Aggregates.limit(50));
        return pipeline;
    }

    // --- Q4: BookRoleInvestor - Advisor with market value range ---
    private List<Bson> buildQ4Pipeline(Map<String, Object> params, DatabaseTarget target) {
        String advisorId = (String) params.get("advisorId");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(Aggregates.match(Filters.and(
                Filters.elemMatch("advisors",
                        Filters.and(Filters.eq("advisorId", advisorId), Filters.gte("noOfViewableAccts", 1))),
                Filters.eq("investorType", "Client"),
                Filters.eq("viewableSource", "Y")
        )));
        pipeline.add(filterAdvisorsStage(advisorId));
        pipeline.add(Aggregates.unwind("$advisors"));
        pipeline.add(Aggregates.match(Filters.and(
                Filters.eq("advisors.advisorId", advisorId),
                Filters.gte("advisors.noOfViewableAccts", 1),
                Filters.gte("advisors.viewableMarketValue", minMv),
                Filters.lte("advisors.viewableMarketValue", maxMv)
        )));
        pipeline.add(Aggregates.project(buildEmbeddedInvestorProjection()));
        if (target == DatabaseTarget.MONGO_NATIVE) {
            pipeline.add(setWindowFieldsCount());
        }
        pipeline.add(Aggregates.sort(Sorts.descending("viewableMarketValue")));
        pipeline.add(Aggregates.limit(50));
        return pipeline;
    }

    // =============================================
    // Q5-Q9 Native MongoDB filters
    // =============================================

    // --- Q5: BookRoleGroup - Filter by entitlements, persona, market value ---
    private Bson buildQ5Filter(Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String personaNm = (String) params.get("personaNm");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        return Filters.and(
                Filters.eq("entitlements.advisoryContext", advisoryContext),
                Filters.eq("dataOwnerPartyRoleId", dataOwnerPartyRoleId),
                Filters.eq("personaNm", personaNm),
                Filters.ne("visibleFlag", "N"),
                Filters.gte("totalViewableAccountsMarketValue", minMv),
                Filters.lte("totalViewableAccountsMarketValue", maxMv)
        );
    }

    // --- Q6: BookRoleGroup - Filter by entitlements + pxPartyRoleId ---
    private Bson buildQ6Filter(Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        return Filters.and(
                Filters.eq("entitlements.advisoryContext", advisoryContext),
                Filters.eq("entitlements.pxPartyRoleIdList", pxPartyRoleId),
                Filters.ne("visibleFlag", "N"),
                Filters.gte("totalViewableAccountsMarketValue", minMv),
                Filters.lte("totalViewableAccountsMarketValue", maxMv)
        );
    }

    // --- Q7: Account - Holdings fund ticker ---
    private Bson buildQ7Filter(Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        String fundTicker = (String) params.get("fundTicker");

        return Filters.and(
                Filters.eq("entitlements.pxPartyRoleIdList", pxPartyRoleId),
                Filters.eq("viewableSource", "Y"),
                Filters.in("holdings.fundTicker", fundTicker)
        );
    }

    // --- Q8: Advisor - Hierarchy path filter ---
    private Bson buildQ8Filter(Map<String, Object> params) {
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String partyNodePathValue = (String) params.get("partyNodePathValue");

        return Filters.and(
                Filters.eq("entitlements.pxClient.dataOwnerPartyRoleId", dataOwnerPartyRoleId),
                Filters.in("advisorHierarchy.partyNodePathValue", partyNodePathValue)
        );
    }

    // --- Q9: Advisor - Market value range ---
    private Bson buildQ9Filter(Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        return Filters.and(
                Filters.eq("entitlements.pxPartyRoleIdList", pxPartyRoleId),
                Filters.gte("accountViewableMarketValue", minMv),
                Filters.lte("accountViewableMarketValue", maxMv)
        );
    }

    // =============================================
    // Q5-Q9 DV-Mongo filters (translated field paths)
    // DV flattens entitlements: no "entitlements" wrapper,
    // arrays become top-level: advisoryContexts[], partyRoleIds[], personaNms[]
    // =============================================

    private Bson buildQ5FilterDv(Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String personaNm = (String) params.get("personaNm");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        return Filters.and(
                Filters.elemMatch("advisoryContexts", Filters.eq("advisoryContext", advisoryContext)),
                Filters.eq("dataOwnerPartyRoleId", dataOwnerPartyRoleId),
                Filters.elemMatch("personaNms", Filters.eq("personaNm", personaNm)),
                Filters.ne("visibleFlag", "N"),
                Filters.gte("totalViewableAccountsMarketValue", minMv),
                Filters.lte("totalViewableAccountsMarketValue", maxMv)
        );
    }

    private Bson buildQ6FilterDv(Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        return Filters.and(
                Filters.elemMatch("advisoryContexts", Filters.eq("advisoryContext", advisoryContext)),
                Filters.elemMatch("partyRoleIds", Filters.eq("partyRoleId", pxPartyRoleId)),
                Filters.ne("visibleFlag", "N"),
                Filters.gte("totalViewableAccountsMarketValue", minMv),
                Filters.lte("totalViewableAccountsMarketValue", maxMv)
        );
    }

    private Bson buildQ7FilterDv(Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        String fundTicker = (String) params.get("fundTicker");

        return Filters.and(
                Filters.elemMatch("partyRoleIds", Filters.eq("partyRoleId", pxPartyRoleId)),
                Filters.eq("viewableSource", "Y"),
                Filters.elemMatch("holdings", Filters.eq("fundTicker", fundTicker))
        );
    }

    private Bson buildQ8FilterDv(Map<String, Object> params) {
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String partyNodePathValue = (String) params.get("partyNodePathValue");

        return Filters.and(
                Filters.eq("entDataOwnerPartyRoleId", dataOwnerPartyRoleId),
                Filters.elemMatch("advisorHierarchy", Filters.eq("partyNodePathValue", partyNodePathValue))
        );
    }

    private Bson buildQ9FilterDv(Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        return Filters.and(
                Filters.elemMatch("partyRoleIds", Filters.eq("partyRoleId", pxPartyRoleId)),
                Filters.gte("accountViewableMarketValue", minMv),
                Filters.lte("accountViewableMarketValue", maxMv)
        );
    }

    // Post-unwind match to ensure only the target advisor row survives
    private Bson matchUnwoundAdvisor(String advisorId) {
        return Aggregates.match(Filters.and(
                Filters.eq("advisors.advisorId", advisorId),
                Filters.gte("advisors.noOfViewableAccts", 1)
        ));
    }

    // Pre-filter advisors array to only the matching advisor before $unwind
    private Bson filterAdvisorsStage(String advisorId) {
        return new Document("$addFields", new Document("advisors",
                new Document("$filter", new Document("input", "$advisors")
                        .append("as", "a")
                        .append("cond", new Document("$and", List.of(
                                new Document("$eq", List.of("$$a.advisorId", advisorId)),
                                new Document("$gte", List.of("$$a.noOfViewableAccts", 1))
                        ))))));
    }

    // Projection for embedded model Q1-Q4: maps advisor fields from $advisors subdoc
    private Bson buildEmbeddedInvestorProjection() {
        return Projections.fields(
                Projections.include("_id", "investorFullName", "investorType", "investorLastName",
                        "investorFirstName", "investorMiddleName", "investorCity", "investorState",
                        "investorZipCode", "investorCountry", "ssnTin", "partyRoleId", "partyId",
                        "finInstId", "clientAccess", "ETLUpdateTS"),
                Projections.computed("advisorId", "$advisors.advisorId"),
                Projections.computed("viewableMarketValue", "$advisors.viewableMarketValue"),
                Projections.computed("noOfViewableAccts", "$advisors.noOfViewableAccts")
        );
    }

    private Bson setWindowFieldsCount() {
        return new Document("$setWindowFields",
                new Document("output",
                        new Document("totalCount",
                                new Document("$count", new Document()))));
    }

    // --- Explain plan capture ---

    public QueryDetail captureQueryDetail(MongoCollection<Document> collection,
                                           QueryDefinition query, SchemaModel model,
                                           Map<String, Object> params, DatabaseTarget target,
                                           String configId) {
        String queryText;
        String explainPlan;
        CodecRegistry registry = collection.getCodecRegistry();

        try {
            if (query.isAggregation()) {
                List<Bson> pipeline = buildAggregationPipeline(query, model, params, target);
                queryText = serializePipeline(pipeline, registry);
                try {
                    var agg = collection.aggregate(pipeline);
                    Bson hint = getAggregationHint(query, target);
                    if (hint != null) agg = agg.hint(hint);
                    Document explainResult = agg
                            .explain(com.mongodb.ExplainVerbosity.EXECUTION_STATS);
                    explainPlan = explainResult.toJson(PRETTY_JSON);
                } catch (Exception e) {
                    explainPlan = "Explain not supported: " + e.getMessage();
                }
            } else {
                Bson filter = buildFindFilter(query, model, params, target);
                queryText = serializeFilter(filter, registry);
                try {
                    Document explainResult = collection.find(filter)
                            .explain(com.mongodb.ExplainVerbosity.EXECUTION_STATS);
                    explainPlan = explainResult.toJson(PRETTY_JSON);
                } catch (Exception e) {
                    explainPlan = "Explain not supported: " + e.getMessage();
                }
            }
        } catch (Exception e) {
            log.warn("Failed to capture query detail for {} on {}: {}", query.queryName(), configId, e.getMessage());
            queryText = "Error building query: " + e.getMessage();
            explainPlan = "N/A";
        }

        return new QueryDetail(query.queryName(), configId, queryText, explainPlan, null, null);
    }

    public String serializePipeline(List<Bson> pipeline, CodecRegistry registry) {
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < pipeline.size(); i++) {
            BsonDocument bsonDoc = pipeline.get(i).toBsonDocument(Document.class, registry);
            sb.append("  ").append(bsonDoc.toJson(PRETTY_JSON));
            if (i < pipeline.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("]");
        return sb.toString();
    }

    public String serializeFilter(Bson filter, CodecRegistry registry) {
        BsonDocument bsonDoc = filter.toBsonDocument(Document.class, registry);
        return bsonDoc.toJson(PRETTY_JSON);
    }
}
