package com.helix.benchmark.query;

import com.helix.benchmark.config.DatabaseTarget;
import com.helix.benchmark.config.SchemaModel;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MongoQueryExecutorTest {

    private final MongoQueryExecutor executor = new MongoQueryExecutor();

    @ParameterizedTest
    @EnumSource(QueryDefinition.class)
    void shouldBuildPipelineForAllQueries(QueryDefinition query) {
        Map<String, Object> params = stubParams(query);
        if (query.isAggregation()) {
            List<Bson> pipeline = executor.buildAggregationPipeline(query, SchemaModel.EMBEDDED, params, DatabaseTarget.MONGO_NATIVE);
            assertThat(pipeline).isNotEmpty();
        } else {
            Bson filter = executor.buildFindFilter(query, SchemaModel.EMBEDDED, params);
            assertThat(filter).isNotNull();
        }
    }

    @Test
    void q1PipelineShouldHaveMatchUnwindMatchProjectSortLimit() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        List<Bson> pipeline = executor.buildAggregationPipeline(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params, DatabaseTarget.MONGO_NATIVE);
        // $match, $unwind, $match, $project, $setWindowFields, $sort, $limit
        assertThat(pipeline.size()).isGreaterThanOrEqualTo(6);
    }

    @Test
    void q1OracleApiShouldNotHaveSetWindowFields() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        List<Bson> pipeline = executor.buildAggregationPipeline(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params, DatabaseTarget.ORACLE_MONGO_API);
        // Should have one fewer stage ($setWindowFields removed)
        List<Bson> nativePipeline = executor.buildAggregationPipeline(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params, DatabaseTarget.MONGO_NATIVE);
        assertThat(pipeline.size()).isEqualTo(nativePipeline.size() - 1);
    }

    @Test
    void shouldReturnCollectionNameForEmbedded() {
        assertThat(executor.getCollectionName(QueryDefinition.Q1, SchemaModel.EMBEDDED, DatabaseTarget.MONGO_NATIVE))
                .isEqualTo("bookRoleInvestor");
        assertThat(executor.getCollectionName(QueryDefinition.Q7, SchemaModel.EMBEDDED, DatabaseTarget.MONGO_NATIVE))
                .isEqualTo("account");
    }

    @Test
    void shouldReturnDvCollectionNameForOracleMongoApiDv() {
        assertThat(executor.getCollectionName(QueryDefinition.Q1, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API_DV))
                .isEqualTo("dv_book_role_investor");
        assertThat(executor.getCollectionName(QueryDefinition.Q5, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API_DV))
                .isEqualTo("dv_book_role_group");
        assertThat(executor.getCollectionName(QueryDefinition.Q7, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API_DV))
                .isEqualTo("dv_account");
        assertThat(executor.getCollectionName(QueryDefinition.Q8, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API_DV))
                .isEqualTo("dv_advisor");
    }

    @Test
    void shouldReturnOriginalCollectionNameForOracleMongoApi() {
        assertThat(executor.getCollectionName(QueryDefinition.Q1, SchemaModel.EMBEDDED, DatabaseTarget.ORACLE_MONGO_API))
                .isEqualTo("bookRoleInvestor");
    }

    @Test
    void q1OracleMongoApiDvShouldNotHaveSetWindowFields() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        List<Bson> pipeline = executor.buildAggregationPipeline(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params, DatabaseTarget.ORACLE_MONGO_API_DV);
        List<Bson> nativePipeline = executor.buildAggregationPipeline(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params, DatabaseTarget.MONGO_NATIVE);
        assertThat(pipeline.size()).isEqualTo(nativePipeline.size() - 1);
    }

    @Test
    void serializePipelineShouldProduceJsonArray() {
        Map<String, Object> params = Map.of("advisorId", "ADV001");
        List<Bson> pipeline = executor.buildAggregationPipeline(
                QueryDefinition.Q1, SchemaModel.EMBEDDED, params, DatabaseTarget.MONGO_NATIVE);
        CodecRegistry registry = CodecRegistries.fromRegistries(
                com.mongodb.MongoClientSettings.getDefaultCodecRegistry());
        String json = executor.serializePipeline(pipeline, registry);
        assertThat(json).startsWith("[");
        assertThat(json).endsWith("]");
        assertThat(json).contains("$match");
    }

    @Test
    void serializeFilterShouldProduceJson() {
        Map<String, Object> params = Map.of("pxPartyRoleId", 123L, "fundTicker", "VTI");
        Bson filter = executor.buildFindFilter(QueryDefinition.Q7, SchemaModel.EMBEDDED, params);
        CodecRegistry registry = CodecRegistries.fromRegistries(
                com.mongodb.MongoClientSettings.getDefaultCodecRegistry());
        String json = executor.serializeFilter(filter, registry);
        assertThat(json).contains("pxPartyRoleIdList");
        assertThat(json).contains("fundTicker");
    }

    private Map<String, Object> stubParams(QueryDefinition query) {
        return switch (query) {
            case Q1 -> Map.of("advisorId", "ADV001");
            case Q2 -> Map.of("advisorId", "ADV001", "partyRoleId", 123L, "searchTerm", "smith");
            case Q3 -> Map.of("advisorId", "ADV001", "advisoryContext", "CTX001", "dataOwnerPartyRoleId", 100L);
            case Q4 -> Map.of("advisorId", "ADV001", "minMarketValue", 1.0, "maxMarketValue", 1000.0);
            case Q5 -> Map.of("advisoryContext", "CTX001", "dataOwnerPartyRoleId", 100L,
                    "personaNm", "Home Office", "minMarketValue", 1.0, "maxMarketValue", 10000.0);
            case Q6 -> Map.of("advisoryContext", "CTX001", "pxPartyRoleId", 123L,
                    "minMarketValue", 0.0, "maxMarketValue", 100.0);
            case Q7 -> Map.of("pxPartyRoleId", 123L, "fundTicker", "VTI");
            case Q8 -> Map.of("dataOwnerPartyRoleId", 100L, "partyNodePathValue", "1-Region");
            case Q9 -> Map.of("pxPartyRoleId", 123L, "minMarketValue", 0.0, "maxMarketValue", 40000000.0);
        };
    }
}
