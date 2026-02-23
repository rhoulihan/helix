package com.helix.benchmark.schema;

import com.helix.benchmark.config.SchemaModel;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OracleRelationalSchemaManagerTest {

    private final OracleRelationalSchemaManager mgr = new OracleRelationalSchemaManager();

    @Test
    void shouldReturn30TableNames() {
        assertThat(mgr.getTableNames()).hasSize(30);
    }

    @Test
    void shouldReturn30CreateTableStatements() {
        assertThat(mgr.getCreateTableStatements()).hasSize(30);
    }

    @Test
    void createTableStatementsShouldUseRelPrefix() {
        for (String ddl : mgr.getCreateTableStatements()) {
            assertThat(ddl).containsIgnoringCase("rel_");
        }
    }

    @Test
    void createTableStatementsShouldContainCreateTable() {
        for (String ddl : mgr.getCreateTableStatements()) {
            assertThat(ddl).containsIgnoringCase("CREATE TABLE");
        }
    }

    @Test
    void parentTablesShouldHavePrimaryKeys() {
        List<String> ddl = mgr.getCreateTableStatements();
        // First 4 are parents
        for (int i = 0; i < 4; i++) {
            assertThat(ddl.get(i))
                    .as("Parent table DDL at index %d should have PRIMARY KEY", i)
                    .containsIgnoringCase("PRIMARY KEY");
        }
    }

    @Test
    void childTablesShouldHaveForeignKeys() {
        List<String> ddl = mgr.getCreateTableStatements();
        // Last 26 are children (indices 4-29)
        for (int i = 4; i < ddl.size(); i++) {
            assertThat(ddl.get(i))
                    .as("Child table DDL at index %d should have FOREIGN KEY", i)
                    .containsIgnoringCase("FOREIGN KEY");
        }
    }

    @Test
    void dropOrderShouldHave30Tables() {
        assertThat(mgr.getDropOrder()).hasSize(30);
    }

    @Test
    void dropOrderShouldStartWithChildTables() {
        List<String> dropOrder = mgr.getDropOrder();
        // First 26 should be child tables
        for (int i = 0; i < 26; i++) {
            String table = dropOrder.get(i);
            assertThat(table)
                    .as("Drop order index %d should be a child table", i)
                    .satisfiesAnyOf(
                            t -> assertThat(t).contains("rel_bri_"),
                            t -> assertThat(t).contains("rel_brg_"),
                            t -> assertThat(t).contains("rel_acct_"),
                            t -> assertThat(t).contains("rel_adv_")
                    );
        }
    }

    @Test
    void indexStatementsShouldHaveAtLeast30Indexes() {
        assertThat(mgr.getIndexStatements().size()).isGreaterThanOrEqualTo(30);
    }

    @Test
    void indexStatementsShouldReferenceRelPrefixedTables() {
        for (String idx : mgr.getIndexStatements()) {
            assertThat(idx).contains("rel_");
            assertThat(idx).doesNotContain("jdbc_");
        }
    }

    @Test
    void indexStatementsShouldNotContainMultivalue() {
        for (String idx : mgr.getIndexStatements()) {
            assertThat(idx).doesNotContainIgnoringCase("MULTIVALUE");
        }
    }

    @Test
    void getCollectionNamesShouldDelegateToGetTableNames() {
        assertThat(mgr.getCollectionNames(SchemaModel.EMBEDDED))
                .isEqualTo(mgr.getTableNames());
    }
}
