package com.helix.benchmark.datagen;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReferenceRegistryTest {

    private ReferenceRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ReferenceRegistry(
                /* advisoryContextPoolSize */ 100,
                /* partyRoleIdPoolSize */ 200,
                /* finInstIdPoolSize */ 10
        );
    }

    @Test
    void shouldInitializeWithEmptyAdvisorIds() {
        assertThat(registry.getAdvisorIds()).isEmpty();
    }

    @Test
    void shouldInitializeWithEmptyInvestorIds() {
        assertThat(registry.getInvestorIds()).isEmpty();
    }

    @Test
    void shouldPreGenerateAdvisoryContextPool() {
        assertThat(registry.getAdvisoryContextIds()).hasSize(100);
        assertThat(registry.getAdvisoryContextIds()).allMatch(id -> !id.isBlank());
    }

    @Test
    void shouldPreGeneratePartyRoleIdPool() {
        assertThat(registry.getPartyRoleIds()).hasSize(200);
        assertThat(registry.getPartyRoleIds()).allMatch(id -> id > 0);
    }

    @Test
    void shouldPreGenerateFinInstIdPool() {
        assertThat(registry.getFinInstIds()).hasSize(10);
        assertThat(registry.getFinInstIds()).allMatch(id -> id > 0);
    }

    @Test
    void shouldRegisterAdvisorId() {
        registry.registerAdvisorId("ADV001");
        assertThat(registry.getAdvisorIds()).containsExactly("ADV001");
    }

    @Test
    void shouldRegisterInvestorId() {
        registry.registerInvestorId("INV001");
        assertThat(registry.getInvestorIds()).containsExactly("INV001");
    }

    @Test
    void shouldReturnRandomAdvisorId() {
        registry.registerAdvisorId("ADV001");
        registry.registerAdvisorId("ADV002");
        assertThat(registry.randomAdvisorId())
                .isIn("ADV001", "ADV002");
    }

    @Test
    void shouldReturnRandomInvestorId() {
        registry.registerInvestorId("INV001");
        registry.registerInvestorId("INV002");
        assertThat(registry.randomInvestorId())
                .isIn("INV001", "INV002");
    }

    @Test
    void shouldReturnRandomAdvisoryContextId() {
        assertThat(registry.randomAdvisoryContextId())
                .isIn(registry.getAdvisoryContextIds().toArray());
    }

    @Test
    void shouldReturnRandomPartyRoleId() {
        assertThat(registry.randomPartyRoleId())
                .isIn(registry.getPartyRoleIds().toArray());
    }

    @Test
    void shouldReturnRandomFinInstId() {
        assertThat(registry.randomFinInstId())
                .isIn(registry.getFinInstIds().toArray());
    }

    @Test
    void shouldGenerateUniqueAdvisoryContextIds() {
        assertThat(registry.getAdvisoryContextIds())
                .doesNotHaveDuplicates();
    }

    @Test
    void shouldReturnRandomSubsetOfAdvisorIds() {
        for (int i = 0; i < 10; i++) {
            registry.registerAdvisorId("ADV" + i);
        }
        var subset = registry.randomAdvisorIds(3);
        assertThat(subset).hasSize(3);
        assertThat(subset).allMatch(id -> registry.getAdvisorIds().contains(id));
    }

    @Test
    void shouldReturnRandomSubsetOfInvestorIds() {
        for (int i = 0; i < 10; i++) {
            registry.registerInvestorId("INV" + i);
        }
        var subset = registry.randomInvestorIds(3);
        assertThat(subset).hasSize(3);
        assertThat(subset).allMatch(id -> registry.getInvestorIds().contains(id));
    }

    @Test
    void shouldReturnRandomFundTicker() {
        String ticker = registry.randomFundTicker();
        assertThat(ticker).isNotBlank();
        assertThat(ticker).hasSizeBetween(2, 6);
    }

    @Test
    void shouldReturnRandomHierarchyPathValue() {
        String value = registry.randomHierarchyPathValue();
        assertThat(value).isNotBlank();
    }
}
