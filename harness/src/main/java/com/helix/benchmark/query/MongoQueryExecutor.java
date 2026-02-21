package com.helix.benchmark.query;

import com.helix.benchmark.config.DatabaseTarget;
import com.helix.benchmark.config.SchemaModel;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class MongoQueryExecutor {

    public String getCollectionName(QueryDefinition query, SchemaModel model) {
        return model.isNormalized() ? "helix" : query.embeddedCollection();
    }

    public List<Bson> buildAggregationPipeline(QueryDefinition query, SchemaModel model,
                                                Map<String, Object> params, DatabaseTarget target) {
        return switch (query) {
            case Q1 -> buildQ1Pipeline(model, params, target);
            case Q2 -> buildQ2Pipeline(model, params, target);
            case Q3 -> buildQ3Pipeline(model, params, target);
            case Q4 -> buildQ4Pipeline(model, params, target);
            default -> throw new IllegalArgumentException(query + " is not an aggregation query");
        };
    }

    public Bson buildFindFilter(QueryDefinition query, SchemaModel model, Map<String, Object> params) {
        return switch (query) {
            case Q5 -> buildQ5Filter(model, params);
            case Q6 -> buildQ6Filter(model, params);
            case Q7 -> buildQ7Filter(model, params);
            case Q8 -> buildQ8Filter(model, params);
            case Q9 -> buildQ9Filter(model, params);
            default -> throw new IllegalArgumentException(query + " is not a find query");
        };
    }

    public long executeAggregation(MongoCollection<Document> collection, QueryDefinition query,
                                    SchemaModel model, Map<String, Object> params, DatabaseTarget target) {
        List<Bson> pipeline = buildAggregationPipeline(query, model, params, target);
        List<Document> results = collection.aggregate(pipeline).into(new ArrayList<>());
        return results.size();
    }

    public long executeFind(MongoCollection<Document> collection, QueryDefinition query,
                             SchemaModel model, Map<String, Object> params) {
        Bson filter = buildFindFilter(query, model, params);
        List<Document> results = collection.find(filter).into(new ArrayList<>());
        return results.size();
    }

    // --- Q1: BookRoleInvestor - Investor list by advisor ---
    private List<Bson> buildQ1Pipeline(SchemaModel model, Map<String, Object> params, DatabaseTarget target) {
        String advisorId = (String) params.get("advisorId");
        String advisorsField = model.isNormalized() ? "advisorsMetadata" : "advisors";
        String advisorIdField = model.isNormalized() ? "advisorsMetadata.advisorId" : "advisors.advisorId";
        String acctField = model.isNormalized() ? "advisorsMetadata.noOfViewableAccts" : "advisors.noOfViewableAccts";

        List<Bson> pipeline = new ArrayList<>();

        // Initial match
        List<Bson> matchFilters = new ArrayList<>();
        if (model.isNormalized()) matchFilters.add(Filters.eq("type", "BookRoleInvestor"));
        matchFilters.add(Filters.eq("investorType", "Client"));
        matchFilters.add(Filters.eq("viewableSource", "Y"));
        if (!model.isNormalized()) {
            matchFilters.add(Filters.elemMatch("advisors",
                    Filters.and(Filters.eq("advisorId", advisorId), Filters.gte("noOfViewableAccts", 1))));
        } else {
            matchFilters.add(Filters.elemMatch("advisorsMetadata",
                    Filters.and(Filters.eq("advisorId", advisorId), Filters.gte("noOfViewableAccts", 1))));
        }
        pipeline.add(Aggregates.match(Filters.and(matchFilters)));

        // Unwind advisors
        pipeline.add(Aggregates.unwind("$" + advisorsField));

        // Post-unwind match
        pipeline.add(Aggregates.match(Filters.and(
                Filters.eq(advisorIdField, advisorId),
                Filters.gte(acctField, 1.0)
        )));

        // Project
        pipeline.add(Aggregates.project(buildInvestorProjection(model)));

        // $setWindowFields (only for native MongoDB)
        if (target == DatabaseTarget.MONGO_NATIVE) {
            pipeline.add(setWindowFieldsCount());
        }

        // Sort + Limit
        String mvField = model.isNormalized() ? "advisor.viewableMarketValue" : "advisor.viewableMarketValue";
        pipeline.add(Aggregates.sort(Sorts.descending(mvField)));
        pipeline.add(Aggregates.limit(50));

        return pipeline;
    }

    // --- Q2: BookRoleInvestor - Search by name with regex ---
    private List<Bson> buildQ2Pipeline(SchemaModel model, Map<String, Object> params, DatabaseTarget target) {
        String advisorId = (String) params.get("advisorId");
        Long partyRoleId = ((Number) params.get("partyRoleId")).longValue();
        String searchTerm = (String) params.get("searchTerm");
        String advisorsField = model.isNormalized() ? "advisorsMetadata" : "advisors";
        String advisorIdField = model.isNormalized() ? "advisorsMetadata.advisorId" : "advisors.advisorId";
        String acctField = model.isNormalized() ? "advisorsMetadata.noOfViewableAccts" : "advisors.noOfViewableAccts";

        List<Bson> pipeline = new ArrayList<>();

        List<Bson> matchFilters = new ArrayList<>();
        if (model.isNormalized()) matchFilters.add(Filters.eq("type", "BookRoleInvestor"));
        matchFilters.add(Filters.eq("investorType", "Client"));
        matchFilters.add(Filters.eq("viewableFlag", "Y"));
        matchFilters.add(Filters.eq("partyRoleId", partyRoleId));
        matchFilters.add(Filters.regex("investorFullName", Pattern.compile(searchTerm, Pattern.CASE_INSENSITIVE)));
        pipeline.add(Aggregates.match(Filters.and(matchFilters)));

        pipeline.add(Aggregates.unwind("$" + advisorsField));
        pipeline.add(Aggregates.match(Filters.and(
                Filters.eq(advisorIdField, advisorId),
                Filters.gte(acctField, 1.0)
        )));
        pipeline.add(Aggregates.project(buildInvestorProjection(model)));
        if (target == DatabaseTarget.MONGO_NATIVE) {
            pipeline.add(setWindowFieldsCount());
        }
        String mvField = model.isNormalized() ? "advisor.viewableMarketValue" : "advisor.viewableMarketValue";
        pipeline.add(Aggregates.sort(Sorts.descending(mvField)));
        pipeline.add(Aggregates.limit(50));

        return pipeline;
    }

    // --- Q3: BookRoleInvestor - Filter by entitlements + advisor ---
    private List<Bson> buildQ3Pipeline(SchemaModel model, Map<String, Object> params, DatabaseTarget target) {
        String advisorId = (String) params.get("advisorId");
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String advisorsField = model.isNormalized() ? "advisorsMetadata" : "advisors";
        String advisorIdField = model.isNormalized() ? "advisorsMetadata.advisorId" : "advisors.advisorId";
        String acctField = model.isNormalized() ? "advisorsMetadata.noOfViewableAccts" : "advisors.noOfViewableAccts";

        List<Bson> pipeline = new ArrayList<>();

        List<Bson> matchFilters = new ArrayList<>();
        if (model.isNormalized()) matchFilters.add(Filters.eq("type", "BookRoleInvestor"));
        matchFilters.add(Filters.eq("entitlements.advisoryContext", advisoryContext));
        matchFilters.add(Filters.eq("entitlements.pxClient.dataOwnerPartyRoleId", dataOwnerPartyRoleId));
        matchFilters.add(Filters.eq("investorType", "Client"));
        matchFilters.add(Filters.eq("viewableSource", "Y"));
        if (!model.isNormalized()) {
            matchFilters.add(Filters.elemMatch("advisors",
                    Filters.and(Filters.eq("advisorId", advisorId), Filters.gte("noOfViewableAccts", 1))));
        } else {
            matchFilters.add(Filters.elemMatch("advisorsMetadata",
                    Filters.and(Filters.eq("advisorId", advisorId), Filters.gte("noOfViewableAccts", 1))));
        }
        pipeline.add(Aggregates.match(Filters.and(matchFilters)));

        pipeline.add(Aggregates.unwind("$" + advisorsField));
        pipeline.add(Aggregates.match(Filters.and(
                Filters.eq(advisorIdField, advisorId),
                Filters.gte(acctField, 1.0)
        )));
        pipeline.add(Aggregates.project(buildInvestorProjection(model)));
        if (target == DatabaseTarget.MONGO_NATIVE) {
            pipeline.add(setWindowFieldsCount());
        }
        String mvField = model.isNormalized() ? "advisor.viewableMarketValue" : "advisor.viewableMarketValue";
        pipeline.add(Aggregates.sort(Sorts.descending(mvField)));
        pipeline.add(Aggregates.limit(50));

        return pipeline;
    }

    // --- Q4: BookRoleInvestor - Advisor with market value range ---
    private List<Bson> buildQ4Pipeline(SchemaModel model, Map<String, Object> params, DatabaseTarget target) {
        String advisorId = (String) params.get("advisorId");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();
        String advisorsField = model.isNormalized() ? "advisorsMetadata" : "advisors";
        String advisorIdField = model.isNormalized() ? "advisorsMetadata.advisorId" : "advisors.advisorId";
        String acctField = model.isNormalized() ? "advisorsMetadata.noOfViewableAccts" : "advisors.noOfViewableAccts";
        String mvFilterField = model.isNormalized() ? "advisorsMetadata.viewableMarketValue" : "advisors.viewableMarketValue";

        List<Bson> pipeline = new ArrayList<>();

        List<Bson> matchFilters = new ArrayList<>();
        if (model.isNormalized()) matchFilters.add(Filters.eq("type", "BookRoleInvestor"));
        matchFilters.add(Filters.eq("investorType", "Client"));
        matchFilters.add(Filters.eq("viewableSource", "Y"));
        if (!model.isNormalized()) {
            matchFilters.add(Filters.elemMatch("advisors",
                    Filters.and(Filters.eq("advisorId", advisorId), Filters.gte("noOfViewableAccts", 1))));
        } else {
            matchFilters.add(Filters.elemMatch("advisorsMetadata",
                    Filters.and(Filters.eq("advisorId", advisorId), Filters.gte("noOfViewableAccts", 1))));
        }
        pipeline.add(Aggregates.match(Filters.and(matchFilters)));

        pipeline.add(Aggregates.unwind("$" + advisorsField));
        pipeline.add(Aggregates.match(Filters.and(
                Filters.eq(advisorIdField, advisorId),
                Filters.gte(acctField, 1.0),
                Filters.gte(mvFilterField, minMv),
                Filters.lte(mvFilterField, maxMv)
        )));
        pipeline.add(Aggregates.project(buildInvestorProjection(model)));
        if (target == DatabaseTarget.MONGO_NATIVE) {
            pipeline.add(setWindowFieldsCount());
        }
        String mvField = model.isNormalized() ? "advisor.viewableMarketValue" : "advisor.viewableMarketValue";
        pipeline.add(Aggregates.sort(Sorts.descending(mvField)));
        pipeline.add(Aggregates.limit(50));

        return pipeline;
    }

    // --- Q5: BookRoleGroup - Filter by entitlements, persona, market value ---
    private Bson buildQ5Filter(SchemaModel model, Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String personaNm = (String) params.get("personaNm");
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        List<Bson> filters = new ArrayList<>();
        if (model.isNormalized()) filters.add(Filters.eq("type", "BookRoleGroup"));
        filters.add(Filters.eq("entitlements.advisoryContext", advisoryContext));
        filters.add(Filters.eq("dataOwnerPartyRoleId", dataOwnerPartyRoleId));
        filters.add(Filters.eq("personaNm", personaNm));
        filters.add(Filters.ne("visibleFlag", "N"));
        filters.add(Filters.gte("totalViewableAccountsMarketValue", minMv));
        filters.add(Filters.lte("totalViewableAccountsMarketValue", maxMv));

        return Filters.and(filters);
    }

    // --- Q6: BookRoleGroup - Filter by entitlements + pxPartyRoleId ---
    private Bson buildQ6Filter(SchemaModel model, Map<String, Object> params) {
        String advisoryContext = (String) params.get("advisoryContext");
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        List<Bson> filters = new ArrayList<>();
        if (model.isNormalized()) filters.add(Filters.eq("type", "BookRoleGroup"));
        filters.add(Filters.eq("entitlements.advisoryContext", advisoryContext));
        filters.add(Filters.eq("entitlements.pxPartyRoleIdList", pxPartyRoleId));
        filters.add(Filters.ne("visibleFlag", "N"));
        filters.add(Filters.gte("totalViewableAccountsMarketValue", minMv));
        filters.add(Filters.lte("totalViewableAccountsMarketValue", maxMv));

        return Filters.and(filters);
    }

    // --- Q7: Account - Holdings fund ticker ---
    private Bson buildQ7Filter(SchemaModel model, Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        String fundTicker = (String) params.get("fundTicker");

        List<Bson> filters = new ArrayList<>();
        if (model.isNormalized()) filters.add(Filters.eq("type", "Account"));
        filters.add(Filters.eq("entitlements.pxPartyRoleIdList", pxPartyRoleId));
        filters.add(Filters.eq("viewableSource", "Y"));
        filters.add(Filters.in("holdings.fundTicker", fundTicker));

        return Filters.and(filters);
    }

    // --- Q8: Advisor - Hierarchy path filter ---
    private Bson buildQ8Filter(SchemaModel model, Map<String, Object> params) {
        Long dataOwnerPartyRoleId = ((Number) params.get("dataOwnerPartyRoleId")).longValue();
        String partyNodePathValue = (String) params.get("partyNodePathValue");

        List<Bson> filters = new ArrayList<>();
        if (model.isNormalized()) filters.add(Filters.eq("type", "Advisor"));
        filters.add(Filters.eq("entitlements.pxClient.dataOwnerPartyRoleId", dataOwnerPartyRoleId));
        filters.add(Filters.in("advisorHierarchy.partyNodePathValue", partyNodePathValue));

        return Filters.and(filters);
    }

    // --- Q9: Advisor - Market value range ---
    private Bson buildQ9Filter(SchemaModel model, Map<String, Object> params) {
        Long pxPartyRoleId = ((Number) params.get("pxPartyRoleId")).longValue();
        double minMv = ((Number) params.get("minMarketValue")).doubleValue();
        double maxMv = ((Number) params.get("maxMarketValue")).doubleValue();

        List<Bson> filters = new ArrayList<>();
        if (model.isNormalized()) filters.add(Filters.eq("type", "Advisor"));
        filters.add(Filters.eq("entitlements.pxPartyRoleIdList", pxPartyRoleId));
        filters.add(Filters.gte("accountViewableMarketValue", minMv));
        filters.add(Filters.lte("accountViewableMarketValue", maxMv));

        return Filters.and(filters);
    }

    private Bson buildInvestorProjection(SchemaModel model) {
        String prefix = model.isNormalized() ? "$advisorsMetadata" : "$advisors";
        return Projections.fields(
                Projections.include("_id", "investorFullName", "investorType", "investorLastName",
                        "investorFirstName", "investorMiddleName", "investorCity", "investorState",
                        "investorZipCode", "investorCountry", "ssnTin", "partyRoleId", "partyId",
                        "finInstId", "clientAccess", "ETLUpdateTS"),
                Projections.computed("advisor.advisorId", prefix + ".advisorId"),
                Projections.computed("advisor.viewableMarketValue", prefix + ".viewableMarketValue"),
                Projections.computed("advisor.noOfViewableAccts", prefix + ".noOfViewableAccts")
        );
    }

    private Bson setWindowFieldsCount() {
        return new Document("$setWindowFields",
                new Document("output",
                        new Document("totalCount",
                                new Document("$count", new Document()))));
    }
}
