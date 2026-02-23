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
    void oracleSchemaManagerShouldReturnEmbeddedTableNames() {
        OracleSchemaManager mgr = new OracleSchemaManager();
        List<String> names = mgr.getTableNames(SchemaModel.EMBEDDED);
        assertThat(names).containsExactlyInAnyOrder(
                "jdbc_account", "jdbc_advisor", "jdbc_book_role_group", "jdbc_book_role_investor");
    }

    @Test
    void oracleJdbcTableNamesShouldNotCollideWithMongoApiCollectionNames() {
        OracleSchemaManager oracleMgr = new OracleSchemaManager();
        MongoSchemaManager mongoMgr = new MongoSchemaManager();
        List<String> jdbcNames = oracleMgr.getTableNames(SchemaModel.EMBEDDED);
        List<String> mongoNames = mongoMgr.getCollectionNames(SchemaModel.EMBEDDED);
        for (String jdbcName : jdbcNames) {
            assertThat(mongoNames)
                    .as("JDBC table '%s' should not collide with MongoDB API collection names", jdbcName)
                    .doesNotContain(jdbcName);
        }
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
    void oracleEmbeddedIndexesShouldReferenceJdbcPrefixedTables() {
        OracleSchemaManager mgr = new OracleSchemaManager();
        List<String> indexes = mgr.getIndexStatements(SchemaModel.EMBEDDED);
        // No index should reference unprefixed table names
        for (String idx : indexes) {
            assertThat(idx).doesNotContainPattern("\\bON account\\b");
            assertThat(idx).doesNotContainPattern("\\bON advisor\\b");
            assertThat(idx).doesNotContainPattern("\\bON book_role_group\\b");
            assertThat(idx).doesNotContainPattern("\\bON book_role_investor\\b");
        }
    }

    // --- OracleRelationalSchemaManager tests ---

    @Test
    void relationalSchemaManagerShouldReturn30Tables() {
        OracleRelationalSchemaManager mgr = new OracleRelationalSchemaManager();
        List<String> names = mgr.getTableNames();
        assertThat(names).hasSize(30);
    }

    @Test
    void relationalSchemaManagerShouldHave4ParentTables() {
        OracleRelationalSchemaManager mgr = new OracleRelationalSchemaManager();
        List<String> names = mgr.getTableNames();
        assertThat(names).contains(
                "rel_book_role_investor", "rel_book_role_group",
                "rel_account", "rel_advisor");
    }

    @Test
    void relationalSchemaManagerShouldHave26ChildTables() {
        OracleRelationalSchemaManager mgr = new OracleRelationalSchemaManager();
        List<String> names = mgr.getTableNames();
        assertThat(names).contains(
                "rel_bri_advisors", "rel_bri_advisory_ctx",
                "rel_bri_adv_book_roles", "rel_bri_investor_hierarchy",
                "rel_bri_persona_nm", "rel_bri_synonyms",
                "rel_brg_advisory_ctx", "rel_brg_persona_nm", "rel_brg_party_role_ids",
                "rel_brg_advisors", "rel_brg_adv_book_roles", "rel_brg_adv_investors",
                "rel_brg_hierarchy",
                "rel_acct_party_role_ids", "rel_acct_holdings",
                "rel_acct_advisors", "rel_acct_adv_book_roles",
                "rel_acct_rep_codes", "rel_acct_hierarchy",
                "rel_acct_ent_inv_entitlements", "rel_acct_advisory_ctx",
                "rel_adv_hierarchy", "rel_adv_party_role_ids",
                "rel_adv_holdings", "rel_adv_rep_codes", "rel_adv_advisory_ctx");
    }

    @Test
    void relationalDropOrderShouldListChildrenBeforeParents() {
        OracleRelationalSchemaManager mgr = new OracleRelationalSchemaManager();
        List<String> dropOrder = mgr.getDropOrder();
        // Children should come before their parents
        assertThat(dropOrder.indexOf("rel_bri_advisors"))
                .isLessThan(dropOrder.indexOf("rel_book_role_investor"));
        assertThat(dropOrder.indexOf("rel_brg_advisory_ctx"))
                .isLessThan(dropOrder.indexOf("rel_book_role_group"));
        assertThat(dropOrder.indexOf("rel_acct_holdings"))
                .isLessThan(dropOrder.indexOf("rel_account"));
        assertThat(dropOrder.indexOf("rel_adv_hierarchy"))
                .isLessThan(dropOrder.indexOf("rel_advisor"));
    }

    @Test
    void relationalCreateTableStatementsShouldHave30Statements() {
        OracleRelationalSchemaManager mgr = new OracleRelationalSchemaManager();
        List<String> ddl = mgr.getCreateTableStatements();
        assertThat(ddl).hasSize(30);
    }

    @Test
    void relationalCreateTableStatementsShouldNotContainJsonCollectionTable() {
        OracleRelationalSchemaManager mgr = new OracleRelationalSchemaManager();
        List<String> ddl = mgr.getCreateTableStatements();
        for (String stmt : ddl) {
            assertThat(stmt).doesNotContainIgnoringCase("JSON COLLECTION TABLE");
        }
    }

    @Test
    void relationalCreateTableStatementsShouldContainForeignKeys() {
        OracleRelationalSchemaManager mgr = new OracleRelationalSchemaManager();
        List<String> ddl = mgr.getCreateTableStatements();
        long fkCount = ddl.stream().filter(s -> s.contains("FOREIGN KEY")).count();
        assertThat(fkCount).isEqualTo(26); // 26 child tables with FKs
    }

    @Test
    void relationalIndexStatementsShouldNotBeEmpty() {
        OracleRelationalSchemaManager mgr = new OracleRelationalSchemaManager();
        List<String> indexes = mgr.getIndexStatements();
        assertThat(indexes).isNotEmpty();
        assertThat(indexes.size()).isGreaterThanOrEqualTo(30);
    }

    @Test
    void relationalTableNamesShouldNotCollideWithJdbcTables() {
        OracleRelationalSchemaManager relMgr = new OracleRelationalSchemaManager();
        OracleSchemaManager jdbcMgr = new OracleSchemaManager();
        List<String> relNames = relMgr.getTableNames();
        List<String> jdbcNames = jdbcMgr.getTableNames(SchemaModel.EMBEDDED);
        for (String relName : relNames) {
            assertThat(jdbcNames)
                    .as("Relational table '%s' should not collide with JDBC table names", relName)
                    .doesNotContain(relName);
        }
    }
}
