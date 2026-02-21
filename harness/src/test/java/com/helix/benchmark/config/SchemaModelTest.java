package com.helix.benchmark.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaModelTest {

    @Test
    void shouldHaveTwoModels() {
        assertThat(SchemaModel.values()).hasSize(2);
    }

    @Test
    void embeddedShouldUseFourCollections() {
        assertThat(SchemaModel.EMBEDDED.collectionCount()).isEqualTo(4);
        assertThat(SchemaModel.EMBEDDED.isNormalized()).isFalse();
    }

    @Test
    void normalizedShouldUseOneCollection() {
        assertThat(SchemaModel.NORMALIZED.collectionCount()).isEqualTo(1);
        assertThat(SchemaModel.NORMALIZED.isNormalized()).isTrue();
    }

    @Test
    void embeddedCollectionNamesShouldMatchSchemas() {
        assertThat(SchemaModel.EMBEDDED.collectionNames())
                .containsExactlyInAnyOrder("account", "advisor", "bookRoleGroup", "bookRoleInvestor");
    }

    @Test
    void normalizedCollectionNameShouldBeHelix() {
        assertThat(SchemaModel.NORMALIZED.collectionNames())
                .containsExactly("helix");
    }

    @Test
    void shouldHaveDisplayNames() {
        assertThat(SchemaModel.EMBEDDED.displayName()).isEqualTo("Model A: Embedded (4 collections)");
        assertThat(SchemaModel.NORMALIZED.displayName()).isEqualTo("Model B: Normalized (1 collection)");
    }
}
