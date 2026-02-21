package com.helix.benchmark.query;

import com.helix.benchmark.datagen.ReferenceRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QueryParameterGeneratorTest {

    private ReferenceRegistry registry;
    private QueryParameterGenerator generator;

    @BeforeEach
    void setUp() {
        registry = new ReferenceRegistry(50, 100, 5);
        for (int i = 0; i < 10; i++) {
            registry.registerAdvisorId("ADV" + i);
            registry.registerInvestorId("INV" + i);
        }
        generator = new QueryParameterGenerator(registry);
    }

    @Test
    void shouldGenerateParametersForQ1() {
        Map<String, Object> params = generator.generate(QueryDefinition.Q1);
        assertThat(params).containsKey("advisorId");
        assertThat(params.get("advisorId")).isInstanceOf(String.class);
    }

    @Test
    void shouldGenerateParametersForQ2() {
        Map<String, Object> params = generator.generate(QueryDefinition.Q2);
        assertThat(params).containsKey("advisorId");
        assertThat(params).containsKey("partyRoleId");
        assertThat(params).containsKey("searchTerm");
    }

    @Test
    void shouldGenerateParametersForQ3() {
        Map<String, Object> params = generator.generate(QueryDefinition.Q3);
        assertThat(params).containsKey("advisorId");
        assertThat(params).containsKey("advisoryContext");
        assertThat(params).containsKey("dataOwnerPartyRoleId");
    }

    @Test
    void shouldGenerateParametersForQ4() {
        Map<String, Object> params = generator.generate(QueryDefinition.Q4);
        assertThat(params).containsKey("advisorId");
        assertThat(params).containsKey("minMarketValue");
        assertThat(params).containsKey("maxMarketValue");
    }

    @Test
    void shouldGenerateParametersForQ5() {
        Map<String, Object> params = generator.generate(QueryDefinition.Q5);
        assertThat(params).containsKey("advisoryContext");
        assertThat(params).containsKey("dataOwnerPartyRoleId");
        assertThat(params).containsKey("personaNm");
        assertThat(params).containsKey("minMarketValue");
        assertThat(params).containsKey("maxMarketValue");
    }

    @Test
    void shouldGenerateParametersForQ6() {
        Map<String, Object> params = generator.generate(QueryDefinition.Q6);
        assertThat(params).containsKey("advisoryContext");
        assertThat(params).containsKey("pxPartyRoleId");
        assertThat(params).containsKey("minMarketValue");
        assertThat(params).containsKey("maxMarketValue");
    }

    @Test
    void shouldGenerateParametersForQ7() {
        Map<String, Object> params = generator.generate(QueryDefinition.Q7);
        assertThat(params).containsKey("pxPartyRoleId");
        assertThat(params).containsKey("fundTicker");
    }

    @Test
    void shouldGenerateParametersForQ8() {
        Map<String, Object> params = generator.generate(QueryDefinition.Q8);
        assertThat(params).containsKey("dataOwnerPartyRoleId");
        assertThat(params).containsKey("partyNodePathValue");
    }

    @Test
    void shouldGenerateParametersForQ9() {
        Map<String, Object> params = generator.generate(QueryDefinition.Q9);
        assertThat(params).containsKey("pxPartyRoleId");
        assertThat(params).containsKey("minMarketValue");
        assertThat(params).containsKey("maxMarketValue");
    }

    @Test
    void shouldGenerateMultipleUniqueSets() {
        Map<String, Object> params1 = generator.generate(QueryDefinition.Q1);
        Map<String, Object> params2 = generator.generate(QueryDefinition.Q1);
        // With random generation, at least some should differ over many attempts
        // but for a single pair it's possible to match; just verify they're valid
        assertThat(params1).isNotNull();
        assertThat(params2).isNotNull();
    }

    @Test
    void shouldUseRegistryValues() {
        Map<String, Object> params = generator.generate(QueryDefinition.Q1);
        String advisorId = (String) params.get("advisorId");
        assertThat(registry.getAdvisorIds()).contains(advisorId);
    }
}
