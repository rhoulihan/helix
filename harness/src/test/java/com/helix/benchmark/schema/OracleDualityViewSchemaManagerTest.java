package com.helix.benchmark.schema;

import com.helix.benchmark.config.SchemaModel;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OracleDualityViewSchemaManagerTest {

    private final OracleDualityViewSchemaManager mgr = new OracleDualityViewSchemaManager();

    @Test
    void shouldReturn4ViewNames() {
        assertThat(mgr.getViewNames()).hasSize(4);
    }

    @Test
    void viewNamesShouldUseDvPrefix() {
        for (String name : mgr.getViewNames()) {
            assertThat(name).startsWith("dv_");
        }
    }

    @Test
    void shouldReturn4CreateViewStatements() {
        assertThat(mgr.getCreateViewStatements()).hasSize(4);
    }

    @Test
    void createViewStatementsShouldContainDualityView() {
        for (String ddl : mgr.getCreateViewStatements()) {
            assertThat(ddl).containsIgnoringCase("JSON RELATIONAL DUALITY VIEW");
        }
    }

    @Test
    void createViewStatementsShouldReferenceRelationalTables() {
        for (String ddl : mgr.getCreateViewStatements()) {
            assertThat(ddl).contains("rel_");
        }
    }

    @Test
    void createViewStatementsShouldUseDvPrefix() {
        for (String ddl : mgr.getCreateViewStatements()) {
            assertThat(ddl).contains("dv_");
        }
    }

    @Test
    void dropOrderShouldContainAllViews() {
        assertThat(mgr.getDropOrder()).hasSize(4);
        assertThat(mgr.getDropOrder()).containsExactlyElementsOf(mgr.getViewNames());
    }

    @Test
    void bookRoleInvestorViewShouldIncludeAllChildTables() {
        String ddl = mgr.getCreateViewStatements().get(0);
        assertThat(ddl).contains("dv_book_role_investor");
        assertThat(ddl).contains("rel_book_role_investor");
        assertThat(ddl).contains("rel_bri_advisors");
        assertThat(ddl).contains("rel_bri_advisory_ctx");
        assertThat(ddl).contains("rel_bri_adv_book_roles");
        assertThat(ddl).contains("rel_bri_investor_hierarchy");
        assertThat(ddl).contains("rel_bri_persona_nm");
        assertThat(ddl).contains("rel_bri_synonyms");
        assertThat(ddl).contains("advisorId");
        assertThat(ddl).contains("viewableMarketValue");
    }

    @Test
    void bookRoleGroupViewShouldIncludeAllChildTables() {
        String ddl = mgr.getCreateViewStatements().get(1);
        assertThat(ddl).contains("dv_book_role_group");
        assertThat(ddl).contains("rel_brg_advisory_ctx");
        assertThat(ddl).contains("rel_brg_persona_nm");
        assertThat(ddl).contains("rel_brg_party_role_ids");
        assertThat(ddl).contains("rel_brg_advisors");
        assertThat(ddl).contains("rel_brg_adv_book_roles");
        assertThat(ddl).contains("rel_brg_adv_investors");
        assertThat(ddl).contains("rel_brg_hierarchy");
    }

    @Test
    void accountViewShouldIncludeAllChildTables() {
        String ddl = mgr.getCreateViewStatements().get(2);
        assertThat(ddl).contains("dv_account");
        assertThat(ddl).contains("rel_acct_party_role_ids");
        assertThat(ddl).contains("rel_acct_holdings");
        assertThat(ddl).contains("rel_acct_advisors");
        assertThat(ddl).contains("rel_acct_adv_book_roles");
        assertThat(ddl).contains("rel_acct_rep_codes");
        assertThat(ddl).contains("rel_acct_hierarchy");
        assertThat(ddl).contains("rel_acct_ent_inv_entitlements");
        assertThat(ddl).contains("rel_acct_advisory_ctx");
        assertThat(ddl).contains("fundTicker");
    }

    @Test
    void advisorViewShouldIncludeAllChildTables() {
        String ddl = mgr.getCreateViewStatements().get(3);
        assertThat(ddl).contains("dv_advisor");
        assertThat(ddl).contains("rel_adv_hierarchy");
        assertThat(ddl).contains("rel_adv_party_role_ids");
        assertThat(ddl).contains("rel_adv_holdings");
        assertThat(ddl).contains("rel_adv_rep_codes");
        assertThat(ddl).contains("rel_adv_advisory_ctx");
        assertThat(ddl).contains("partyNodePathValue");
        assertThat(ddl).contains("partyNodePathNm");
    }

    @Test
    void getCollectionNamesShouldDelegateToGetViewNames() {
        assertThat(mgr.getCollectionNames(SchemaModel.EMBEDDED))
                .isEqualTo(mgr.getViewNames());
    }
}
