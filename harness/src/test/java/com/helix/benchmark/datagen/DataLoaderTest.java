package com.helix.benchmark.datagen;

import com.helix.benchmark.config.DatabaseTarget;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DataLoaderTest {

    @Test
    void shouldPartitionIntoBatches() {
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < 2500; i++) {
            docs.add(new Document("_id", String.valueOf(i)));
        }
        List<List<Document>> batches = DataLoader.partition(docs, 1000);
        assertThat(batches).hasSize(3);
        assertThat(batches.get(0)).hasSize(1000);
        assertThat(batches.get(1)).hasSize(1000);
        assertThat(batches.get(2)).hasSize(500);
    }

    @Test
    void shouldPartitionSingleBatch() {
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            docs.add(new Document("_id", String.valueOf(i)));
        }
        List<List<Document>> batches = DataLoader.partition(docs, 1000);
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0)).hasSize(500);
    }

    @Test
    void shouldHandleEmptyList() {
        List<List<Document>> batches = DataLoader.partition(List.of(), 1000);
        assertThat(batches).isEmpty();
    }

    @Test
    void shouldDetermineBatchSizeByTarget() {
        assertThat(DataLoader.batchSizeFor(DatabaseTarget.MONGO_NATIVE, 1000, 500))
                .isEqualTo(1000);
        assertThat(DataLoader.batchSizeFor(DatabaseTarget.ORACLE_JDBC, 1000, 500))
                .isEqualTo(500);
        assertThat(DataLoader.batchSizeFor(DatabaseTarget.ORACLE_MONGO_API, 1000, 500))
                .isEqualTo(1000);
    }
}
