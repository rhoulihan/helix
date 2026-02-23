package com.helix.benchmark.datagen;

import org.bson.Document;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TestDataGenerator {
    private final ReferenceRegistry registry;

    private static final String[] PERSONAS = {"Home Office", "Wove Administrator", "Advisor", "Investor"};
    private static final String[] BOOK_ROLES = {"Home Office", "Primary", "Secondary", "Service Team"};
    private static final String[] ACCOUNT_TYPES = {"IRA", "Roth IRA", "401K", "Brokerage", "Trust", "Joint"};
    private static final String[] STATES = {"NY", "CA", "TX", "FL", "PA", "OH", "IL", "GA", "NC", "MI"};
    private static final String[] ETL_SOURCES = {"xyz", "abc"};

    // Pre-generated pools for fast random data (avoids Faker overhead)
    private static final String[] FIRST_NAMES = {
            "JAMES", "MARY", "JOHN", "PATRICIA", "ROBERT", "JENNIFER", "MICHAEL", "LINDA",
            "WILLIAM", "ELIZABETH", "DAVID", "BARBARA", "RICHARD", "SUSAN", "JOSEPH", "JESSICA",
            "THOMAS", "SARAH", "CHARLES", "KAREN", "CHRISTOPHER", "LISA", "DANIEL", "NANCY",
            "MATTHEW", "BETTY", "ANTHONY", "MARGARET", "MARK", "SANDRA", "DONALD", "ASHLEY",
            "STEVEN", "KIMBERLY", "PAUL", "EMILY", "ANDREW", "DONNA", "JOSHUA", "MICHELLE",
            "KEVIN", "DOROTHY", "BRIAN", "CAROL", "GEORGE", "AMANDA", "TIMOTHY", "MELISSA",
            "RONALD", "DEBORAH", "EDWARD", "STEPHANIE", "JASON", "REBECCA", "JEFFREY", "SHARON",
            "RYAN", "LAURA", "JACOB", "CYNTHIA", "GARY", "KATHLEEN", "NICHOLAS", "AMY"
    };
    private static final String[] LAST_NAMES = {
            "SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER", "DAVIS",
            "RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "GONZALEZ", "WILSON", "ANDERSON",
            "THOMAS", "TAYLOR", "MOORE", "JACKSON", "MARTIN", "LEE", "PEREZ", "THOMPSON",
            "WHITE", "HARRIS", "SANCHEZ", "CLARK", "RAMIREZ", "LEWIS", "ROBINSON", "WALKER",
            "YOUNG", "ALLEN", "KING", "WRIGHT", "SCOTT", "TORRES", "NGUYEN", "HILL", "FLORES",
            "GREEN", "ADAMS", "NELSON", "BAKER", "HALL", "RIVERA", "CAMPBELL", "MITCHELL"
    };
    private static final String[] CITIES = {
            "NEW YORK", "LOS ANGELES", "CHICAGO", "HOUSTON", "PHOENIX", "PHILADELPHIA",
            "SAN ANTONIO", "SAN DIEGO", "DALLAS", "SAN JOSE", "AUSTIN", "JACKSONVILLE",
            "FORT WORTH", "COLUMBUS", "CHARLOTTE", "INDIANAPOLIS", "SAN FRANCISCO", "SEATTLE",
            "DENVER", "NASHVILLE", "OKLAHOMA CITY", "PORTLAND", "LAS VEGAS", "MEMPHIS",
            "LOUISVILLE", "BALTIMORE", "MILWAUKEE", "ALBUQUERQUE", "TUCSON", "FRESNO"
    };
    private static final String[] COMPANY_NAMES = {
            "GLOBAL ADVISORS LLC", "CAPITAL MANAGEMENT GROUP", "WEALTH SOLUTIONS INC",
            "FINANCIAL PARTNERS CORP", "PREMIER INVESTMENTS", "STRATEGIC CAPITAL MGMT",
            "HARBOR FINANCIAL GROUP", "SUMMIT WEALTH ADVISORS", "PINNACLE ASSET MGMT",
            "ALLIANCE FINANCIAL SERVICES", "HERITAGE CAPITAL GROUP", "FRONTIER INVESTMENTS",
            "PACIFIC WEALTH MANAGEMENT", "ATLANTIC ADVISORY GROUP", "GOLDEN STATE ADVISORS",
            "EMPIRE FINANCIAL CORP", "CONTINENTAL WEALTH GROUP", "NATIONAL CAPITAL MGMT"
    };
    private static final String[] BUZZWORDS = {
            "GROWTH", "VALUE", "EQUITY", "INCOME", "BALANCED", "AGGRESSIVE", "CONSERVATIVE",
            "DIVERSIFIED", "INTERNATIONAL", "DOMESTIC", "SMALL CAP", "MID CAP", "LARGE CAP",
            "EMERGING", "SUSTAINABLE", "TECHNOLOGY", "HEALTHCARE", "ENERGY", "REAL ESTATE"
    };

    public TestDataGenerator(ReferenceRegistry registry) {
        this.registry = registry;
    }

    public List<Document> generateAdvisors(int count) {
        return generateAdvisors(0, count);
    }

    public List<Document> generateAdvisors(int startIndex, int count) {
        List<Document> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String id = String.valueOf(1_000_000_000_000L + startIndex + i);
            String firstName = randomChoice(FIRST_NAMES);
            String lastName = randomChoice(LAST_NAMES);
            Document doc = new Document()
                    .append("_id", id)
                    .append("advisorName", firstName + " " + lastName)
                    .append("pxId", "F" + ThreadLocalRandom.current().nextInt(10, 999))
                    .append("partyNodeLabelId", String.valueOf(300000 + startIndex + i))
                    .append("advisorTaxId", String.valueOf(ThreadLocalRandom.current().nextInt(10000000, 99999999)))
                    .append("userType", randomChoice("bcd", "abc", "xyz"))
                    .append("finInstId", registry.randomFinInstId())
                    .append("advState", randomChoice(STATES))
                    .append("advisorFullName", firstName + " " + lastName)
                    .append("advSetupTmst", randomDate(2010, 2018))
                    .append("advUpdateTmst", randomDate(2018, 2025))
                    .append("advAcctMethod", randomChoice("avd", "avg", "ffo"))
                    .append("advMethodFlag", randomChoice("Y", "N"))
                    .append("riaIarQuestion", randomChoice("Y", "N"))
                    .append("dbaQuestion", randomChoice("Y", "N"))
                    .append("noOfSegments", String.valueOf(ThreadLocalRandom.current().nextInt(1, 50)))
                    .append("finInstName", randomChoice(COMPANY_NAMES))
                    .append("finLastName", randomChoice(LAST_NAMES))
                    .append("finFirstName", randomChoice(FIRST_NAMES))
                    .append("accountViewableMarketValue", randomMarketValue())
                    .append("viewableInvestorCount", ThreadLocalRandom.current().nextLong(1, 50))
                    .append("accountViewableCount", ThreadLocalRandom.current().nextLong(1, 50))
                    .append("repCodes", List.of(buildRepCode()))
                    .append("holdings", buildHoldings(ThreadLocalRandom.current().nextInt(1, 10)))
                    .append("advisorHierarchy", buildAdvisorHierarchy())
                    .append("entitlements", buildEntitlements())
                    .append("state", randomChoice(STATES))
                    .append("city", randomChoice(CITIES))
                    .append("zip", randomZip())
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
        return generateBookRoleInvestors(0, count);
    }

    public List<Document> generateBookRoleInvestors(int startIndex, int count) {
        List<Document> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long finInstId = registry.randomFinInstId();
            String investorId = String.valueOf(100_000_000_000L + startIndex + i);
            String id = finInstId + "_" + investorId;
            String firstName = randomChoice(FIRST_NAMES);
            String lastName = randomChoice(LAST_NAMES);

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
                    .append("investorCity", randomChoice(CITIES))
                    .append("investorState", randomChoice(STATES))
                    .append("investorZipCode", randomZip())
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
        return generateBookRoleGroups(0, count);
    }

    public List<Document> generateBookRoleGroups(int startIndex, int count) {
        List<Document> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long finInstId = registry.randomFinInstId();
            String id = finInstId + "_" + (1_000_000 + startIndex + i);
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
                    .append("accountGroupName", randomChoice(COMPANY_NAMES) + " GROUP")
                    .append("accountGroupId", String.valueOf(10000 + startIndex + i))
                    .append("accountGroupType", randomChoice("Performance", "Standard", "Custom"))
                    .append("visibleFlag", weightedChoice("Y", 0.85, "N"))
                    .append("portfolioType", randomChoice("A", "B", "C"))
                    .append("ETLUpdateTS", Instant.now().toString());

            result.add(doc);
        }
        return result;
    }

    public List<Document> generateAccounts(int count) {
        return generateAccounts(0, count);
    }

    public List<Document> generateAccounts(int startIndex, int count) {
        List<Document> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String id = String.valueOf(1_000_001_000_000L + startIndex + i);
            int advisorCount = ThreadLocalRandom.current().nextInt(1, 3);

            Document doc = new Document()
                    .append("_id", id)
                    .append("accountid", "A" + String.format("%06d", startIndex + i))
                    .append("ssnTin", String.valueOf(ThreadLocalRandom.current().nextInt(1000000, 9999999)))
                    .append("finInstId", registry.randomFinInstId())
                    .append("clientName", randomChoice(FIRST_NAMES) + " " + randomChoice(LAST_NAMES))
                    .append("clientId", String.valueOf(100000 + startIndex + i))
                    .append("finInstName", randomChoice(COMPANY_NAMES))
                    .append("accountType", randomChoice(ACCOUNT_TYPES))
                    .append("acctName", randomChoice(COMPANY_NAMES))
                    .append("viewable", true)
                    .append("viewableSource", weightedChoice("Y", 0.8, "N"))
                    .append("setupTmst", randomDate(2010, 2020))
                    .append("updateTmst", randomDate(2020, 2025))
                    .append("entitlements", buildAccountEntitlements())
                    .append("repCodes", List.of(buildRepCode()))
                    .append("advisors", buildEmbeddedAdvisors(advisorCount))
                    .append("advisorHierarchy", buildAdvisorHierarchy())
                    .append("holdings", buildHoldings(ThreadLocalRandom.current().nextInt(1, 8)))
                    .append("acctTitle", randomChoice(COMPANY_NAMES))
                    .append("category", randomChoice("ins", "inv", "ret"))
                    .append("ETLUpdateTS", Instant.now().toString());

            result.add(doc);
        }
        return result;
    }

    // --- Builder helpers ---

    private List<Document> buildEmbeddedAdvisors(int count) {
        List<Document> advisors = new ArrayList<>();
        List<String> ids = registry.randomAdvisorIds(count);
        for (String advisorId : ids) {
            String firstName = randomChoice(FIRST_NAMES);
            String lastName = randomChoice(LAST_NAMES);
            advisors.add(new Document()
                    .append("advisorId", advisorId)
                    .append("advisorName", firstName + " " + lastName)
                    .append("advisorTaxId", String.valueOf(ThreadLocalRandom.current().nextInt(10000000, 99999999)))
                    .append("finInstId", registry.randomFinInstId())
                    .append("lastName", lastName)
                    .append("firstName", firstName)
                    .append("middleName", "")
                    .append("state", randomChoice(STATES))
                    .append("city", randomChoice(CITIES))
                    .append("zipCode", randomZip())
                    .append("country", "USA")
                    .append("businessPhone", randomPhone())
                    .append("bookRoles", randomSubList(BOOK_ROLES, 1, 2))
                    .append("bookType", randomChoice("WRI", "ADV", "HO"))
                    .append("marketValue", randomMarketValue())
                    .append("noOfAccts", ThreadLocalRandom.current().nextLong(1, 20))
                    .append("noOfViewableAccts", ThreadLocalRandom.current().nextLong(1, 20))
                    .append("viewableMarketValue", randomMarketValue())
                    .append("status", weightedChoice("Active", 0.9, "Inactive"))
                    .append("isPrimary", ThreadLocalRandom.current().nextBoolean())
                    .append("email", randomEmail(firstName, lastName)));
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

        String firstName = randomChoice(FIRST_NAMES);
        String lastName = randomChoice(LAST_NAMES);
        return new Document()
                .append("advisorId", advisorId)
                .append("advisorTaxId", String.valueOf(ThreadLocalRandom.current().nextInt(10000000, 99999999)))
                .append("finInstId", registry.randomFinInstId())
                .append("firstName", firstName)
                .append("middleName", "")
                .append("lastName", lastName)
                .append("advisorName", firstName + " " + lastName)
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
                        .append("pxClientName", randomChoice(COMPANY_NAMES))
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
                    .append("fundName", "CAPITAL " + randomChoice(BUZZWORDS))
                    .append("fundTicker", registry.randomFundTicker())
                    .append("mgtName", randomChoice(COMPANY_NAMES))
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

    private static double randomMarketValue() {
        return Math.round(ThreadLocalRandom.current().nextDouble(1000, 50_000_000) * 100.0) / 100.0;
    }

    private static Date randomDate(int yearFrom, int yearTo) {
        long minEpoch = Instant.parse(yearFrom + "-01-01T00:00:00Z").toEpochMilli();
        long maxEpoch = Instant.parse(yearTo + "-01-01T00:00:00Z").toEpochMilli();
        long randomEpoch = ThreadLocalRandom.current().nextLong(minEpoch, maxEpoch);
        return Date.from(Instant.ofEpochMilli(randomEpoch));
    }

    private static String randomZip() {
        return String.format("%05d", ThreadLocalRandom.current().nextInt(10000, 99999));
    }

    private static String randomPhone() {
        return String.format("(%03d) %03d-%04d",
                ThreadLocalRandom.current().nextInt(200, 999),
                ThreadLocalRandom.current().nextInt(200, 999),
                ThreadLocalRandom.current().nextInt(1000, 9999));
    }

    private static String randomEmail(String firstName, String lastName) {
        return firstName.toLowerCase() + "." + lastName.toLowerCase() + "@example.com";
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
