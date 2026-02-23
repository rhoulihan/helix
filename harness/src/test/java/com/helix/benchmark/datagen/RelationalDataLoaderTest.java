package com.helix.benchmark.datagen;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RelationalDataLoaderTest {

    @Test
    void shouldExtractEntDataOwnerPartyRoleId() {
        Document doc = new Document()
                .append("entitlements", new Document()
                        .append("pxClient", new Document()
                                .append("dataOwnerPartyRoleId", 12345L)));
        Number result = RelationalDataLoader.extractEntDataOwnerPartyRoleId(doc);
        assertThat(result).isNotNull();
        assertThat(result.longValue()).isEqualTo(12345L);
    }

    @Test
    void shouldReturnNullWhenNoEntitlements() {
        Document doc = new Document("_id", "test");
        Number result = RelationalDataLoader.extractEntDataOwnerPartyRoleId(doc);
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnNullWhenNoPxClient() {
        Document doc = new Document()
                .append("entitlements", new Document()
                        .append("advisoryContext", List.of("CTX1")));
        Number result = RelationalDataLoader.extractEntDataOwnerPartyRoleId(doc);
        assertThat(result).isNull();
    }

    @Test
    void shouldThrowForUnknownCollectionType() {
        RelationalDataLoader loader = new RelationalDataLoader();
        assertThatThrownBy(() ->
                loader.loadToRelational(null, "unknown", List.of(), 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown collection type");
    }

    @Test
    void shouldCreateInstance() {
        RelationalDataLoader loader = new RelationalDataLoader();
        assertThat(loader).isNotNull();
    }
}
