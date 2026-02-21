package com.helix.benchmark.schema;

import com.helix.benchmark.config.SchemaModel;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaManagerTest {

    @Test
    void mongoSchemaManagerShouldReturnEmbeddedCollectionNames() {
        MongoSchemaManager mgr = new MongoSchemaManager();
        List<String> names = mgr.getCollectionNames(SchemaModel.EMBEDDED);
        assertThat(names).containsExactlyInAnyOrder(
                "account", "advisor", "bookRoleGroup", "bookRoleInvestor");
    }

    @Test
    void mongoSchemaManagerShouldReturnNormalizedCollectionName() {
        MongoSchemaManager mgr = new MongoSchemaManager();
        List<String> names = mgr.getCollectionNames(SchemaModel.NORMALIZED);
        assertThat(names).containsExactly("helix");
    }

    @Test
    void mongoSchemaManagerShouldReturnEmbeddedIndexCommands() {
        MongoSchemaManager mgr = new MongoSchemaManager();
        var indexes = mgr.getIndexDefinitions(SchemaModel.EMBEDDED);
        assertThat(indexes).isNotEmpty();
        // Q1-Q4 use bookRoleInvestor
        assertThat(indexes).anyMatch(idx ->
                idx.collection().equals("bookRoleInvestor") &&
                idx.keys().containsKey("investorType"));
    }

    @Test
    void mongoSchemaManagerShouldReturnNormalizedIndexCommands() {
        MongoSchemaManager mgr = new MongoSchemaManager();
        var indexes = mgr.getIndexDefinitions(SchemaModel.NORMALIZED);
        assertThat(indexes).isNotEmpty();
        // All normalized indexes target 'helix' collection
        assertThat(indexes).allMatch(idx -> idx.collection().equals("helix"));
        // Should have type discriminator indexes
        assertThat(indexes).anyMatch(idx -> idx.keys().containsKey("type"));
    }

    @Test
    void oracleSchemaManagerShouldReturnEmbeddedTableNames() {
        OracleSchemaManager mgr = new OracleSchemaManager();
        List<String> names = mgr.getTableNames(SchemaModel.EMBEDDED);
        assertThat(names).containsExactlyInAnyOrder(
                "account", "advisor", "book_role_group", "book_role_investor");
    }

    @Test
    void oracleSchemaManagerShouldReturnNormalizedTableName() {
        OracleSchemaManager mgr = new OracleSchemaManager();
        List<String> names = mgr.getTableNames(SchemaModel.NORMALIZED);
        assertThat(names).containsExactly("helix");
    }

    @Test
    void oracleSchemaManagerShouldReturnCreateTableStatements() {
        OracleSchemaManager mgr = new OracleSchemaManager();
        List<String> ddl = mgr.getCreateTableStatements(SchemaModel.EMBEDDED);
        assertThat(ddl).hasSize(4);
        assertThat(ddl).allMatch(s -> s.contains("JSON COLLECTION TABLE"));
    }

    @Test
    void oracleSchemaManagerShouldReturnIndexStatements() {
        OracleSchemaManager mgr = new OracleSchemaManager();
        List<String> indexes = mgr.getIndexStatements(SchemaModel.EMBEDDED);
        assertThat(indexes).isNotEmpty();
        // Should have multivalue indexes for arrays
        assertThat(indexes).anyMatch(s -> s.contains("MULTIVALUE INDEX"));
    }

    @Test
    void oracleSchemaManagerShouldReturnNormalizedCreateTable() {
        OracleSchemaManager mgr = new OracleSchemaManager();
        List<String> ddl = mgr.getCreateTableStatements(SchemaModel.NORMALIZED);
        assertThat(ddl).hasSize(1);
        assertThat(ddl.get(0)).contains("helix");
    }

    @Test
    void oracleSchemaManagerShouldReturnNormalizedIndexStatements() {
        OracleSchemaManager mgr = new OracleSchemaManager();
        List<String> indexes = mgr.getIndexStatements(SchemaModel.NORMALIZED);
        assertThat(indexes).isNotEmpty();
        // Should have type discriminator index
        assertThat(indexes).anyMatch(s -> s.contains("type"));
    }
}
