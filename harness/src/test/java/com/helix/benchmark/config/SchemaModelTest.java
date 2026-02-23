package com.helix.benchmark.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaModelTest {

    @Test
    void shouldHaveOneModel() {
        assertThat(SchemaModel.values()).hasSize(1);
    }

    @Test
    void embeddedShouldUseFourCollections() {
        assertThat(SchemaModel.EMBEDDED.collectionCount()).isEqualTo(4);
    }

    @Test
    void embeddedCollectionNamesShouldMatchSchemas() {
        assertThat(SchemaModel.EMBEDDED.collectionNames())
                .containsExactlyInAnyOrder("account", "advisor", "bookRoleGroup", "bookRoleInvestor");
    }

    @Test
    void shouldHaveDisplayName() {
        assertThat(SchemaModel.EMBEDDED.displayName()).isEqualTo("Embedded (4 collections)");
    }
}
