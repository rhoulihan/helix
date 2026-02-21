package com.helix.benchmark.datagen;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TestDataGeneratorTest {

    private ReferenceRegistry registry;
    private TestDataGenerator generator;

    @BeforeEach
    void setUp() {
        registry = new ReferenceRegistry(50, 100, 5);
        generator = new TestDataGenerator(registry);
    }

    // --- Advisor generation ---

    @Test
    void shouldGenerateAdvisorDocuments() {
        List<Document> advisors = generator.generateAdvisors(10);
        assertThat(advisors).hasSize(10);
    }

    @Test
    void shouldPopulateAdvisorRequiredFields() {
        List<Document> advisors = generator.generateAdvisors(1);
        Document doc = advisors.get(0);
        assertThat(doc.getString("_id")).isNotBlank();
        assertThat(doc.getString("advisorName")).isNotBlank();
        assertThat(doc.get("finInstId")).isNotNull();
        assertThat(doc.get("entitlements")).isNotNull();
        assertThat(doc.get("advisorHierarchy")).isNotNull();
        assertThat(doc.get("repCodes")).isNotNull();
        assertThat(doc.get("holdings")).isNotNull();
        assertThat(doc.getDouble("accountViewableMarketValue")).isNotNull();
    }

    @Test
    void shouldRegisterAdvisorIdsInRegistry() {
        generator.generateAdvisors(5);
        assertThat(registry.getAdvisorIds()).hasSize(5);
    }

    @Test
    void advisorEntitlementsShouldContainRequiredFields() {
        List<Document> advisors = generator.generateAdvisors(1);
        Document entitlements = advisors.get(0).get("entitlements", Document.class);
        assertThat(entitlements.get("pxPartyRoleIdList")).isNotNull();
        assertThat(entitlements.get("advisoryContext")).isNotNull();
        assertThat(entitlements.get("pxClient")).isNotNull();
    }

    // --- BookRoleInvestor generation ---

    @Test
    void shouldGenerateBookRoleInvestorDocuments() {
        generator.generateAdvisors(10);
        List<Document> investors = generator.generateBookRoleInvestors(20);
        assertThat(investors).hasSize(20);
    }

    @Test
    void bookRoleInvestorShouldHaveRequiredFields() {
        generator.generateAdvisors(5);
        List<Document> investors = generator.generateBookRoleInvestors(1);
        Document doc = investors.get(0);
        assertThat(doc.getString("_id")).isNotBlank();
        assertThat(doc.getString("investorType")).isIn("Client", "Prospect");
        assertThat(doc.getString("entity")).isEqualTo("Client");
        assertThat(doc.getString("viewableSource")).isIn("Y", "N");
        assertThat(doc.getString("investorFullName")).isNotBlank();
        assertThat(doc.get("advisors")).isInstanceOf(List.class);
        assertThat(doc.get("entitlements")).isNotNull();
    }

    @Test
    void bookRoleInvestorShouldHaveEmbeddedAdvisors() {
        generator.generateAdvisors(5);
        List<Document> investors = generator.generateBookRoleInvestors(1);
        @SuppressWarnings("unchecked")
        List<Document> advisors = (List<Document>) investors.get(0).get("advisors");
        assertThat(advisors).isNotEmpty();
        assertThat(advisors).allMatch(a -> a.containsKey("advisorId"));
        assertThat(advisors).allMatch(a -> a.containsKey("advisorName"));
        assertThat(advisors).allMatch(a -> a.containsKey("noOfViewableAccts"));
        assertThat(advisors).allMatch(a -> a.containsKey("viewableMarketValue"));
        assertThat(advisors).allMatch(a -> a.containsKey("status"));
    }

    @Test
    void shouldRegisterInvestorIdsInRegistry() {
        generator.generateAdvisors(5);
        generator.generateBookRoleInvestors(10);
        assertThat(registry.getInvestorIds()).hasSize(10);
    }

    // --- BookRoleGroup generation ---

    @Test
    void shouldGenerateBookRoleGroupDocuments() {
        generator.generateAdvisors(5);
        generator.generateBookRoleInvestors(10);
        List<Document> groups = generator.generateBookRoleGroups(5);
        assertThat(groups).hasSize(5);
    }

    @Test
    void bookRoleGroupShouldHaveRequiredFields() {
        generator.generateAdvisors(5);
        generator.generateBookRoleInvestors(10);
        List<Document> groups = generator.generateBookRoleGroups(1);
        Document doc = groups.get(0);
        assertThat(doc.getString("_id")).isNotBlank();
        assertThat(doc.getString("entity")).isEqualTo("Group");
        assertThat(doc.get("personaNm")).isInstanceOf(List.class);
        assertThat(doc.get("advisors")).isInstanceOf(List.class);
        assertThat(doc.get("entitlements")).isNotNull();
        assertThat(doc.get("totalViewableAccountsMarketValue")).isNotNull();
    }

    @Test
    void bookRoleGroupAdvisorsShouldHaveInvestors() {
        generator.generateAdvisors(5);
        generator.generateBookRoleInvestors(10);
        List<Document> groups = generator.generateBookRoleGroups(1);
        @SuppressWarnings("unchecked")
        List<Document> advisors = (List<Document>) groups.get(0).get("advisors");
        assertThat(advisors).isNotEmpty();
        assertThat(advisors).allMatch(a -> a.containsKey("investors"));
    }

    // --- Account generation ---

    @Test
    void shouldGenerateAccountDocuments() {
        generator.generateAdvisors(5);
        generator.generateBookRoleInvestors(10);
        List<Document> accounts = generator.generateAccounts(10);
        assertThat(accounts).hasSize(10);
    }

    @Test
    void accountShouldHaveRequiredFields() {
        generator.generateAdvisors(5);
        generator.generateBookRoleInvestors(10);
        List<Document> accounts = generator.generateAccounts(1);
        Document doc = accounts.get(0);
        assertThat(doc.getString("_id")).isNotBlank();
        assertThat(doc.getString("accountid")).isNotBlank();
        assertThat(doc.getString("viewableSource")).isIn("Y", "N");
        assertThat(doc.get("advisors")).isInstanceOf(List.class);
        assertThat(doc.get("holdings")).isInstanceOf(List.class);
        assertThat(doc.get("entitlements")).isNotNull();
        assertThat(doc.get("advisorHierarchy")).isNotNull();
    }

    // --- Normalization transform ---

    @Test
    void shouldTransformToNormalizedModel() {
        generator.generateAdvisors(3);
        generator.generateBookRoleInvestors(5);
        generator.generateBookRoleGroups(2);
        List<Document> accounts = generator.generateAccounts(4);

        Map<String, List<Document>> embedded = Map.of(
                "advisor", generator.generateAdvisors(3),
                "bookRoleInvestor", generator.generateBookRoleInvestors(5),
                "bookRoleGroup", generator.generateBookRoleGroups(2),
                "account", generator.generateAccounts(4)
        );

        List<Document> normalized = TestDataGenerator.transformToNormalized(embedded);
        assertThat(normalized).hasSize(3 + 5 + 2 + 4);
    }

    @Test
    void normalizedDocsShouldHaveTypeField() {
        generator.generateAdvisors(2);
        generator.generateBookRoleInvestors(2);
        Map<String, List<Document>> embedded = Map.of(
                "advisor", generator.generateAdvisors(2),
                "bookRoleInvestor", generator.generateBookRoleInvestors(2),
                "bookRoleGroup", List.of(),
                "account", List.of()
        );

        List<Document> normalized = TestDataGenerator.transformToNormalized(embedded);
        assertThat(normalized).allMatch(d -> d.containsKey("type"));
    }

    @Test
    void normalizedAdvisorsShouldHaveAdvisorIdField() {
        generator.generateAdvisors(2);
        Map<String, List<Document>> embedded = Map.of(
                "advisor", generator.generateAdvisors(2),
                "bookRoleInvestor", List.of(),
                "bookRoleGroup", List.of(),
                "account", List.of()
        );

        List<Document> normalized = TestDataGenerator.transformToNormalized(embedded);
        List<Document> advisors = normalized.stream()
                .filter(d -> "Advisor".equals(d.getString("type")))
                .toList();
        assertThat(advisors).allMatch(d -> d.containsKey("advisorId"));
    }

    @Test
    void normalizedInvestorsShouldHaveAdvisorIdsInsteadOfAdvisors() {
        generator.generateAdvisors(3);
        Map<String, List<Document>> embedded = Map.of(
                "advisor", List.of(),
                "bookRoleInvestor", generator.generateBookRoleInvestors(3),
                "bookRoleGroup", List.of(),
                "account", List.of()
        );

        List<Document> normalized = TestDataGenerator.transformToNormalized(embedded);
        List<Document> investors = normalized.stream()
                .filter(d -> "BookRoleInvestor".equals(d.getString("type")))
                .toList();
        assertThat(investors).allMatch(d -> d.containsKey("advisorIds"));
        assertThat(investors).allMatch(d -> d.containsKey("advisorsMetadata"));
        assertThat(investors).allMatch(d -> !d.containsKey("advisors"));
    }

    @Test
    void normalizedGroupsShouldHaveInvestorIds() {
        generator.generateAdvisors(3);
        generator.generateBookRoleInvestors(5);
        Map<String, List<Document>> embedded = Map.of(
                "advisor", List.of(),
                "bookRoleInvestor", List.of(),
                "bookRoleGroup", generator.generateBookRoleGroups(2),
                "account", List.of()
        );

        List<Document> normalized = TestDataGenerator.transformToNormalized(embedded);
        List<Document> groups = normalized.stream()
                .filter(d -> "BookRoleGroup".equals(d.getString("type")))
                .toList();
        assertThat(groups).allMatch(d -> d.containsKey("investorIds"));
    }
}
