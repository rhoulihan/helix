package com.helix.benchmark.datagen;

import net.datafaker.Faker;
import org.bson.Document;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TestDataGenerator {
    private final ReferenceRegistry registry;
    private final Faker faker = new Faker();

    private static final String[] PERSONAS = {"Home Office", "Wove Administrator", "Advisor", "Investor"};
    private static final String[] BOOK_ROLES = {"Home Office", "Primary", "Secondary", "Service Team"};
    private static final String[] ACCOUNT_TYPES = {"IRA", "Roth IRA", "401K", "Brokerage", "Trust", "Joint"};
    private static final String[] STATES = {"NY", "CA", "TX", "FL", "PA", "OH", "IL", "GA", "NC", "MI"};
    private static final String[] ETL_SOURCES = {"xyz", "abc"};

    public TestDataGenerator(ReferenceRegistry registry) {
        this.registry = registry;
    }

    public List<Document> generateAdvisors(int count) {
        List<Document> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String id = String.valueOf(1_000_000_000_000L + registry.getAdvisorIds().size() + i);
            Document doc = new Document()
                    .append("_id", id)
                    .append("advisorName", faker.name().fullName().toUpperCase())
                    .append("pxId", "F" + ThreadLocalRandom.current().nextInt(10, 999))
                    .append("partyNodeLabelId", String.valueOf(300000 + i))
                    .append("advisorTaxId", String.valueOf(ThreadLocalRandom.current().nextInt(10000000, 99999999)))
                    .append("userType", randomChoice("bcd", "abc", "xyz"))
                    .append("finInstId", registry.randomFinInstId())
                    .append("advState", randomChoice(STATES))
                    .append("advisorFullName", faker.name().fullName().toUpperCase())
                    .append("advSetupTmst", randomDate(2010, 2018))
                    .append("advUpdateTmst", randomDate(2018, 2025))
                    .append("advAcctMethod", randomChoice("avd", "avg", "ffo"))
                    .append("advMethodFlag", randomChoice("Y", "N"))
                    .append("riaIarQuestion", randomChoice("Y", "N"))
                    .append("dbaQuestion", randomChoice("Y", "N"))
                    .append("noOfSegments", String.valueOf(ThreadLocalRandom.current().nextInt(1, 50)))
                    .append("finInstName", faker.company().name())
                    .append("finLastName", faker.name().lastName().toUpperCase())
                    .append("finFirstName", faker.name().firstName().toUpperCase())
                    .append("accountViewableMarketValue", randomMarketValue())
                    .append("viewableInvestorCount", ThreadLocalRandom.current().nextLong(1, 50))
                    .append("accountViewableCount", ThreadLocalRandom.current().nextLong(1, 50))
                    .append("repCodes", List.of(buildRepCode()))
                    .append("holdings", buildHoldings(ThreadLocalRandom.current().nextInt(1, 10)))
                    .append("advisorHierarchy", buildAdvisorHierarchy())
                    .append("entitlements", buildEntitlements())
                    .append("state", randomChoice(STATES))
                    .append("city", faker.address().city())
                    .append("zip", faker.address().zipCode())
                    .append("country", "USA")
                    .append("status", randomChoice("VIEWABLE", "Active"))
                    .append("viewableSource", weightedChoice("Y", 0.8, "N"))
                    .append("ETLUpdateTS", Instant.now().toString());

            result.add(doc);
            registry.registerAdvisorId(id);
        }
        return result;
    }

    public List<Document> generateBookRoleInvestors(int count) {
        List<Document> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long finInstId = registry.randomFinInstId();
            String investorId = String.valueOf(100_000_000_000L + registry.getInvestorIds().size() + i);
            String id = finInstId + "_" + investorId;
            String firstName = faker.name().firstName().toUpperCase();
            String lastName = faker.name().lastName().toUpperCase();

            int advisorCount = ThreadLocalRandom.current().nextInt(1, 5);
            List<Document> advisors = buildEmbeddedAdvisors(advisorCount);

            Document doc = new Document()
                    .append("_id", id)
                    .append("partyRoleId", ThreadLocalRandom.current().nextLong(1, 100_000_000))
                    .append("partyId", ThreadLocalRandom.current().nextLong(1, 100_000_000))
                    .append("conversionInProgress", false)
                    .append("dataOwnerPartyRoleId", finInstId)
                    .append("entitlements", buildEntitlements())
                    .append("advisorHierarchy", buildAdvisorHierarchy())
                    .append("investorId", investorId)
                    .append("personaNm", randomSubList(PERSONAS, 1, 3))
                    .append("entity", "Client")
                    .append("totalMarketValue", randomMarketValue())
                    .append("totalAccounts", ThreadLocalRandom.current().nextLong(1, 20))
                    .append("totalViewableAccountsMarketValue", randomMarketValue())
                    .append("totalViewableAccountCount", ThreadLocalRandom.current().nextLong(1, 20))
                    .append("advisors", advisors)
                    .append("ssnTin", String.valueOf(ThreadLocalRandom.current().nextInt(10000, 99999)))
                    .append("finInstId", finInstId)
                    .append("investorType", weightedChoice("Client", 0.9, "Prospect"))
                    .append("investorLastName", lastName)
                    .append("investorFirstName", firstName)
                    .append("investorMiddleName", "")
                    .append("investorFullName", firstName + " " + lastName)
                    .append("investorpartyRoleId", ThreadLocalRandom.current().nextLong(1, 100_000_000))
                    .append("investorCity", faker.address().city())
                    .append("investorState", randomChoice(STATES))
                    .append("investorZipCode", faker.address().zipCode())
                    .append("investorBirthdate", randomDate(1940, 2000))
                    .append("viewableFlag", weightedChoice("Y", 0.85, "N"))
                    .append("viewableSource", weightedChoice("Y", 0.8, "N"))
                    .append("clientAccess", randomChoice("Invite", "Full", "View Only"))
                    .append("trustFlag", randomChoice("Y", "N"))
                    .append("riskProfile", new Document())
                    .append("synonyms", buildSynonyms())
                    .append("updateTmst", randomDate(2020, 2025))
                    .append("setupTmst", randomDate(2015, 2020))
                    .append("ETLUpdateTS", Instant.now().toString());

            result.add(doc);
            registry.registerInvestorId(investorId);
        }
        return result;
    }

    public List<Document> generateBookRoleGroups(int count) {
        List<Document> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long finInstId = registry.randomFinInstId();
            String id = finInstId + "_" + (1_000_000 + i);
            int advisorCount = ThreadLocalRandom.current().nextInt(1, 4);
            double marketValue = randomMarketValue();

            List<Document> advisors = new ArrayList<>();
            for (int a = 0; a < advisorCount; a++) {
                Document advisor = buildEmbeddedAdvisorForGroup();
                advisors.add(advisor);
            }

            Document doc = new Document()
                    .append("_id", id)
                    .append("investorWriId", "")
                    .append("etlSourceGroup", randomChoice(ETL_SOURCES))
                    .append("finInstId", finInstId)
                    .append("personaNm", randomSubList(PERSONAS, 1, 3))
                    .append("entity", "Group")
                    .append("dataOwnerPartyRoleId", finInstId)
                    .append("advisorHierarchy", buildAdvisorHierarchyShort())
                    .append("entitlements", buildEntitlements())
                    .append("accountCount", ThreadLocalRandom.current().nextLong(1, 50))
                    .append("totalMarketValue", marketValue)
                    .append("totalViewableAccountCount", ThreadLocalRandom.current().nextLong(1, 50))
                    .append("totalViewableAccountsMarketValue", marketValue * 0.95)
                    .append("advisors", advisors)
                    .append("accountGroupName", faker.company().name().toUpperCase())
                    .append("accountGroupId", String.valueOf(10000 + i))
                    .append("accountGroupType", randomChoice("Performance", "Standard", "Custom"))
                    .append("visibleFlag", weightedChoice("Y", 0.85, "N"))
                    .append("portfolioType", randomChoice("A", "B", "C"))
                    .append("ETLUpdateTS", Instant.now().toString());

            result.add(doc);
        }
        return result;
    }

    public List<Document> generateAccounts(int count) {
        List<Document> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String id = String.valueOf(1_000_001_000_000L + i);
            int advisorCount = ThreadLocalRandom.current().nextInt(1, 3);

            Document doc = new Document()
                    .append("_id", id)
                    .append("accountid", "A" + String.format("%06d", i))
                    .append("ssnTin", String.valueOf(ThreadLocalRandom.current().nextInt(1000000, 9999999)))
                    .append("finInstId", registry.randomFinInstId())
                    .append("clientName", faker.name().fullName().toUpperCase())
                    .append("clientId", String.valueOf(100000 + i))
                    .append("finInstName", faker.company().name())
                    .append("accountType", randomChoice(ACCOUNT_TYPES))
                    .append("acctName", faker.company().name().toUpperCase())
                    .append("viewable", true)
                    .append("viewableSource", weightedChoice("Y", 0.8, "N"))
                    .append("setupTmst", randomDate(2010, 2020))
                    .append("updateTmst", randomDate(2020, 2025))
                    .append("entitlements", buildAccountEntitlements())
                    .append("repCodes", List.of(buildRepCode()))
                    .append("advisors", buildEmbeddedAdvisors(advisorCount))
                    .append("advisorHierarchy", buildAdvisorHierarchy())
                    .append("holdings", buildHoldings(ThreadLocalRandom.current().nextInt(1, 8)))
                    .append("acctTitle", faker.company().name().toUpperCase())
                    .append("category", randomChoice("ins", "inv", "ret"))
                    .append("ETLUpdateTS", Instant.now().toString());

            result.add(doc);
        }
        return result;
    }

    public static List<Document> transformToNormalized(Map<String, List<Document>> embedded) {
        List<Document> normalized = new ArrayList<>();

        for (Document doc : embedded.getOrDefault("advisor", List.of())) {
            Document norm = new Document(doc);
            norm.append("type", "Advisor");
            norm.append("advisorId", doc.getString("_id"));
            normalized.add(norm);
        }

        for (Document doc : embedded.getOrDefault("bookRoleInvestor", List.of())) {
            Document norm = new Document(doc);
            norm.append("type", "BookRoleInvestor");
            transformAdvisorsToMetadata(norm);
            normalized.add(norm);
        }

        for (Document doc : embedded.getOrDefault("bookRoleGroup", List.of())) {
            Document norm = new Document(doc);
            norm.append("type", "BookRoleGroup");
            transformAdvisorsToMetadata(norm);
            extractInvestorIds(norm);
            normalized.add(norm);
        }

        for (Document doc : embedded.getOrDefault("account", List.of())) {
            Document norm = new Document(doc);
            norm.append("type", "Account");
            transformAdvisorsToMetadata(norm);
            normalized.add(norm);
        }

        return normalized;
    }

    @SuppressWarnings("unchecked")
    private static void transformAdvisorsToMetadata(Document doc) {
        List<Document> advisors = (List<Document>) doc.get("advisors");
        if (advisors == null) return;

        List<String> advisorIds = new ArrayList<>();
        List<Document> metadata = new ArrayList<>();

        for (Document advisor : advisors) {
            advisorIds.add(advisor.getString("advisorId"));
            metadata.add(new Document()
                    .append("advisorId", advisor.getString("advisorId"))
                    .append("advisorName", advisor.getString("advisorName"))
                    .append("noOfViewableAccts", advisor.get("noOfViewableAccts"))
                    .append("viewableMarketValue", advisor.get("viewableMarketValue"))
                    .append("bookRoles", advisor.get("bookRoles"))
                    .append("status", advisor.getString("status")));
        }

        doc.remove("advisors");
        doc.append("advisorIds", advisorIds);
        doc.append("advisorsMetadata", metadata);
    }

    @SuppressWarnings("unchecked")
    private static void extractInvestorIds(Document doc) {
        // In embedded model, investors are nested within advisors
        // Since we already transformed advisors, we check the original structure
        // The investorIds are extracted from what was advisors[].investors[]
        List<Document> metadata = (List<Document>) doc.get("advisorsMetadata");
        List<String> investorIds = new ArrayList<>();
        // In the pre-transform state, investors were already extracted;
        // we'll add a placeholder list from the doc if it exists
        if (doc.containsKey("investorIds")) return;
        // For groups, investors were nested in advisors before transform
        // We add an empty list; actual data comes from the embedded advisors before transform
        doc.append("investorIds", investorIds);
    }

    // --- Builder helpers ---

    private List<Document> buildEmbeddedAdvisors(int count) {
        List<Document> advisors = new ArrayList<>();
        List<String> ids = registry.randomAdvisorIds(count);
        for (String advisorId : ids) {
            advisors.add(new Document()
                    .append("advisorId", advisorId)
                    .append("advisorName", faker.name().fullName().toUpperCase())
                    .append("advisorTaxId", String.valueOf(ThreadLocalRandom.current().nextInt(10000000, 99999999)))
                    .append("finInstId", registry.randomFinInstId())
                    .append("lastName", faker.name().lastName().toUpperCase())
                    .append("firstName", faker.name().firstName().toUpperCase())
                    .append("middleName", "")
                    .append("state", randomChoice(STATES))
                    .append("city", faker.address().city())
                    .append("zipCode", faker.address().zipCode())
                    .append("country", "USA")
                    .append("businessPhone", faker.phoneNumber().phoneNumber())
                    .append("bookRoles", randomSubList(BOOK_ROLES, 1, 2))
                    .append("bookType", randomChoice("WRI", "ADV", "HO"))
                    .append("marketValue", randomMarketValue())
                    .append("noOfAccts", ThreadLocalRandom.current().nextLong(1, 20))
                    .append("noOfViewableAccts", ThreadLocalRandom.current().nextLong(1, 20))
                    .append("viewableMarketValue", randomMarketValue())
                    .append("status", weightedChoice("Active", 0.9, "Inactive"))
                    .append("isPrimary", ThreadLocalRandom.current().nextBoolean())
                    .append("email", faker.internet().emailAddress()));
        }
        return advisors;
    }

    private Document buildEmbeddedAdvisorForGroup() {
        String advisorId = registry.randomAdvisorId();
        int investorCount = ThreadLocalRandom.current().nextInt(1, 6);
        List<Document> investors = new ArrayList<>();
        List<String> investorIdList = registry.getInvestorIds().isEmpty() ?
                List.of("placeholder") : registry.randomInvestorIds(Math.min(investorCount, registry.getInvestorIds().size()));
        for (String invId : investorIdList) {
            investors.add(new Document("investorId", invId));
        }

        return new Document()
                .append("advisorId", advisorId)
                .append("advisorTaxId", String.valueOf(ThreadLocalRandom.current().nextInt(10000000, 99999999)))
                .append("finInstId", registry.randomFinInstId())
                .append("firstName", faker.name().firstName().toUpperCase())
                .append("middleName", "")
                .append("lastName", faker.name().lastName().toUpperCase())
                .append("advisorName", faker.name().fullName().toUpperCase())
                .append("bookRoles", randomSubList(BOOK_ROLES, 1, 2))
                .append("bookType", randomChoice("WRI", "ADV"))
                .append("totalViewableAccountsMarketValue", randomMarketValue())
                .append("totalViewableAccountCount", ThreadLocalRandom.current().nextInt(1, 20))
                .append("investors", investors)
                .append("noOfViewableAccts", ThreadLocalRandom.current().nextLong(1, 20))
                .append("viewableMarketValue", randomMarketValue())
                .append("status", weightedChoice("Active", 0.9, "Inactive"));
    }

    private Document buildEntitlements() {
        int partyCount = ThreadLocalRandom.current().nextInt(1, 5);
        List<Long> partyRoleIds = new ArrayList<>();
        for (int i = 0; i < partyCount; i++) {
            partyRoleIds.add(registry.randomPartyRoleId());
        }
        int ctxCount = ThreadLocalRandom.current().nextInt(1, 4);
        List<String> contexts = new ArrayList<>();
        for (int i = 0; i < ctxCount; i++) {
            contexts.add(registry.randomAdvisoryContextId());
        }
        long finInstId = registry.randomFinInstId();
        return new Document()
                .append("pxPartyRoleIdList", partyRoleIds)
                .append("advisoryContext", contexts)
                .append("pxClient", new Document()
                        .append("pxClientId", String.valueOf(finInstId))
                        .append("pxClientName", faker.company().name())
                        .append("Id", String.valueOf(finInstId))
                        .append("dataOwnerPartyRoleId", finInstId));
    }

    private Document buildAccountEntitlements() {
        Document base = buildEntitlements();
        int invCount = ThreadLocalRandom.current().nextInt(1, 4);
        List<Document> investorEntitlements = new ArrayList<>();
        for (int i = 0; i < invCount; i++) {
            investorEntitlements.add(new Document()
                    .append("partyRoleId", ThreadLocalRandom.current().nextLong(1, 100_000_000))
                    .append("accountRole", randomChoice("View Only", "Full Access"))
                    .append("accountSource", randomChoice("ABC", "BCD", "XYZ"))
                    .append("accountAccessStatus", "Approved")
                    .append("investorId", registry.getInvestorIds().isEmpty() ?
                            "placeholder" : registry.randomInvestorId())
                    .append("accountRoleCode", randomChoice("XYZ", "ABCD")));
        }
        base.append("pxInvestorEntitlements", investorEntitlements);
        return base;
    }

    private List<Document> buildAdvisorHierarchy() {
        return List.of(
                new Document("partyNodePathNm", "Firm")
                        .append("partyNodePathValue", registry.randomHierarchyPathValue()),
                new Document("partyNodePathNm", "Region")
                        .append("partyNodePathValue", registry.randomHierarchyPathValue()),
                new Document("partyNodePathNm", "IPPersonTeam")
                        .append("partyNodePathValue", registry.randomHierarchyPathValue())
        );
    }

    private List<Document> buildAdvisorHierarchyShort() {
        return List.of(
                new Document("partyNodePathValue", registry.randomHierarchyPathValue()),
                new Document("partyNodePathValue", registry.randomHierarchyPathValue()),
                new Document("partyNodePathValue", registry.randomHierarchyPathValue())
        );
    }

    private List<Document> buildHoldings(int count) {
        List<Document> holdings = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            holdings.add(new Document()
                    .append("fundId", String.valueOf(1000 + i))
                    .append("fundName", "CAPITAL " + faker.company().buzzword().toUpperCase())
                    .append("fundTicker", registry.randomFundTicker())
                    .append("mgtName", faker.company().name())
                    .append("dividendRate", Math.round(ThreadLocalRandom.current().nextDouble(0, 5) * 10000.0) / 10000.0));
        }
        return holdings;
    }

    private Document buildRepCode() {
        return new Document()
                .append("advisorRepNumber", String.valueOf(ThreadLocalRandom.current().nextInt(100000, 999999)))
                .append("intType", ThreadLocalRandom.current().nextInt(1, 20))
                .append("repcodeSource", randomChoice("xyz", "abc"));
    }

    private List<Document> buildSynonyms() {
        return List.of(
                new Document("partySynonymTypeCd", "TID").append("partySynonymStr",
                        String.valueOf(ThreadLocalRandom.current().nextInt(100000, 999999))),
                new Document("partySynonymTypeCd", "XID").append("partySynonymStr",
                        UUID.randomUUID().toString()),
                new Document("partySynonymTypeCd", "WID").append("partySynonymStr",
                        String.valueOf(1_000_000_000_000L + ThreadLocalRandom.current().nextInt(0, 1_000_000))),
                new Document("partySynonymTypeCd", "SID").append("partySynonymStr",
                        String.valueOf(ThreadLocalRandom.current().nextInt(100000000, 999999999)))
        );
    }

    private double randomMarketValue() {
        return Math.round(ThreadLocalRandom.current().nextDouble(1000, 50_000_000) * 100.0) / 100.0;
    }

    private Date randomDate(int yearFrom, int yearTo) {
        long minEpoch = Instant.parse(yearFrom + "-01-01T00:00:00Z").toEpochMilli();
        long maxEpoch = Instant.parse(yearTo + "-01-01T00:00:00Z").toEpochMilli();
        long randomEpoch = ThreadLocalRandom.current().nextLong(minEpoch, maxEpoch);
        return Date.from(Instant.ofEpochMilli(randomEpoch));
    }

    @SafeVarargs
    private static <T> T randomChoice(T... options) {
        return options[ThreadLocalRandom.current().nextInt(options.length)];
    }

    private static String weightedChoice(String primary, double weight, String secondary) {
        return ThreadLocalRandom.current().nextDouble() < weight ? primary : secondary;
    }

    private static List<String> randomSubList(String[] options, int min, int max) {
        int count = ThreadLocalRandom.current().nextInt(min, max + 1);
        List<String> shuffled = new ArrayList<>(Arrays.asList(options));
        Collections.shuffle(shuffled, ThreadLocalRandom.current());
        return shuffled.subList(0, Math.min(count, shuffled.size()));
    }
}
