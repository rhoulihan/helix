# Helix Database Benchmark Harness

A Java benchmark harness that evaluates 9 query patterns across 4 database targets, generates ~4.5 GB of synthetic financial services test data, validates result consistency across all targets, and produces a self-contained HTML report comparing latency and throughput.

---

## Table of Contents

- [Test Matrix](#test-matrix)
- [Architecture Overview](#architecture-overview)
- [Schema: Embedded Document Model (4 Collections)](#schema-embedded-document-model-4-collections)
  - [BookRoleInvestor Collection](#bookroleinvestor-collection)
  - [BookRoleGroup Collection](#bookrolegroup-collection)
  - [Account Collection](#account-collection)
  - [Advisor Collection](#advisor-collection)
- [Oracle Relational Schema](#oracle-relational-schema)
- [Index Definitions](#index-definitions)
  - [MongoDB Indexes](#mongodb-indexes)
  - [Oracle JDBC Indexes (JSON Collections)](#oracle-jdbc-indexes-json-collections)
  - [Oracle Relational Indexes](#oracle-relational-indexes)
- [Access Patterns (9 Queries)](#access-patterns-9-queries)
  - [Q1: Investor List by Advisor](#q1-investor-list-by-advisor)
  - [Q2: Investor Search by Name (Regex)](#q2-investor-search-by-name-regex)
  - [Q3: Investor List by Entitlements + Advisor](#q3-investor-list-by-entitlements--advisor)
  - [Q4: Investor List with Market Value Range](#q4-investor-list-with-market-value-range)
  - [Q5: Groups by Entitlements, Persona, Market Value](#q5-groups-by-entitlements-persona-market-value)
  - [Q6: Groups by Entitlements + User ID](#q6-groups-by-entitlements--user-id)
  - [Q7: Accounts by Fund Ticker](#q7-accounts-by-fund-ticker)
  - [Q8: Advisors by Hierarchy Path](#q8-advisors-by-hierarchy-path)
  - [Q9: Advisors by Market Value Range](#q9-advisors-by-market-value-range)
- [Benchmark Workload](#benchmark-workload)
  - [Data Generation](#data-generation)
  - [Execution Protocol](#execution-protocol)
  - [Result Validation](#result-validation)
  - [Metrics Collected](#metrics-collected)
  - [HTML Report](#html-report)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Infrastructure Setup](#infrastructure-setup)
  - [Build and Run Tests](#build-and-run-tests)
  - [Running the Benchmark](#running-the-benchmark)
  - [Smoke Test (Quick Validation)](#smoke-test-quick-validation)
  - [Configuration Reference](#configuration-reference)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)

---

## Test Matrix

The harness benchmarks every query across 4 active database targets using the embedded document model:

| # | Target | Description | Storage Model | Driver |
|---|--------|-------------|---------------|--------|
| 1 | **MongoDB Native** | MongoDB 8.2 | 4 JSON document collections | mongodb-driver-sync 5.3 |
| 2 | **Oracle JDBC** | Oracle 23ai JSON collections via SODA | 4 JSON collection tables (`jdbc_*`) | ojdbc17 23.7 + SQL/JSON |
| 3 | **Oracle MongoDB API** | Oracle 23ai via MongoDB wire protocol | 4 JSON collections via ORDS | mongodb-driver-sync 5.3 via ORDS |
| 4 | **Oracle Relational** | Oracle 23ai normalized relational tables | 4 parent + 23 child tables (`rel_*`) | ojdbc17 23.7 + standard SQL |

Two additional targets are defined but not active by default:

| # | Target | Description |
|---|--------|-------------|
| 5 | Oracle Duality View | JSON Relational Duality Views via JDBC |
| 6 | Oracle MongoDB API (DV) | Duality Views via MongoDB wire protocol |

To enable/disable targets, edit `activeTargets` in `benchmark-config.yaml`.

---

## Architecture Overview

The benchmark orchestrator (`HelixBenchmarkMain`) executes in four steps:

```
Step 1: Data Generation & Loading
    Generate synthetic documents with Datafaker → bulk-insert to all targets
    (MongoDB, Oracle JSON collections, Oracle relational tables)

Step 2: Index Creation
    Create target-specific indexes for optimal query performance

Step 2.5: Result Validation
    Execute each query on all targets, compare results row-by-row
    → All 9 queries must return identical results across all targets

Step 3: Benchmark Execution
    Warm-up phase (50 iterations) → Measurement phase (200 iterations)
    → Compute p50/p95/p99 latency and throughput

Step 4: Report Generation
    → benchmark-report.html with charts and comparison tables
```

Each query has a dedicated executor per target:
- **MongoQueryExecutor** — builds MongoDB aggregation pipelines and find filters; used by both MongoDB Native and Oracle MongoDB API targets
- **OracleJdbcQueryExecutor** — builds SQL/JSON queries against `jdbc_*` JSON collection tables
- **OracleRelationalQueryExecutor** — builds standard SQL queries against `rel_*` normalized tables
- **OracleDualityViewQueryExecutor** — builds SQL/JSON queries against `dv_*` duality views

---

## Schema: Embedded Document Model (4 Collections)

All 4 targets store the same logical documents. MongoDB and Oracle JDBC store them as-is in JSON collections. Oracle Relational decomposes them into normalized tables (see [Oracle Relational Schema](#oracle-relational-schema)).

### BookRoleInvestor Collection

The most voluminous collection. Represents an investor/client record with embedded advisor details and per-advisor metrics.

```
{
  _id:                              String   // "finInstId_investorId"
  partyRoleId:                      Long
  partyId:                          Long
  conversionInProgress:             Boolean
  dataOwnerPartyRoleId:             Long
  entitlements: {
    advisoryContext:                String[]  // Book IDs
    pxClients: {
      pxClientNm:                   String
      dataOwnerPartyRoleId:         Long
    }
    pxPartyRoleIdList:             Long[]    // Authorized user IDs
  }
  advisorHierarchy: [{
    partyNodePathNm:                String   // "Region" | "Firm" | "IPPersonTeam"
    partyNodePathValue:             String
  }]
  investorId:                       String
  personaNm:                        String[]
  entity:                           String   // "Client"
  totalMarketValue:                 Double
  totalAccounts:                    Long
  totalViewableAccountsMarketValue: Double
  totalViewableAccountCount:        Long
  advisors: [{                               // EMBEDDED advisor details
    advisorId:                      String   // FK to Advisor._id
    advisorTaxId:                   String
    advisorName:                    String
    finInstId:                      Long
    lastName, firstName, middleName: String
    state, city, zipCode, country:  String
    businessPhone, email:           String
    bookRoles:                      String[] // ["Home Office", "Primary", ...]
    bookType:                       String   // "WRI" | "ADV" | "HO"
    marketValue:                    Double
    noOfAccts:                      Long
    noOfViewableAccts:              Long     // KEY FIELD — used in Q1-Q4
    viewableMarketValue:            Double   // KEY FIELD — sort/filter target
    isPrimary:                      Boolean
    status:                         String   // "Active" | "Inactive"
  }]
  ssnTin:                           String
  finInstId:                        Long
  investorType:                     String   // "Client" | "Prospect"
  investorFullName:                 String   // Used in Q2 regex search
  investorLastName, investorFirstName, investorMiddleName: String
  investorCity, investorState, investorZipCode: String
  investorBirthdate:                Date
  viewableFlag:                     String   // "Y" | "N"
  viewableSource:                   String   // "Y" | "N"
  clientAccess:                     String   // "Invite" | "Full" | "View Only"
  trustFlag:                        String
  synonyms: [{
    partySynonymTypeCd:             String   // "TID" | "XID" | "WID" | "SID"
    partySynonymStr:                String
  }]
  ETLUpdateTS:                      String
}
```

### BookRoleGroup Collection

Represents a group-level book of business (household, performance group). Contains embedded advisors, each with a nested investors array.

```
{
  _id:                              String   // "finInstId_groupId"
  investorWriId:                    String
  etlSourceGroup:                   String
  finInstId:                        Long
  personaNm:                        String[]
  entity:                           String   // "Group"
  dataOwnerPartyRoleId:             Long
  advisorHierarchy: [{ partyNodePathValue: String }]
  entitlements: {
    advisoryContext:                String[]
    pxClient: { pxClientNm: String, dataOwnerPartyRoleId: Long }
    pxPartyRoleIdList:             Long[]
  }
  accountCount:                     Long
  totalMarketValue:                 Double
  totalViewableAccountCount:        Long
  totalViewableAccountsMarketValue: Double
  advisors: [{
    advisorId:                      String
    advisorName:                    String
    bookRoles:                      String[]
    bookType:                       String
    noOfViewableAccts:              Long
    viewableMarketValue:            Double
    investors: [{ investorId: String }]
    status:                         String
  }]
  accountGroupName:                 String
  accountGroupId:                   String
  accountGroupType:                 String
  visibleFlag:                      String
  portfolioType:                    String
  ETLUpdateTS:                      String
}
```

### Account Collection

Represents a financial account with embedded advisors and portfolio holdings.

```
{
  _id:                  String       // Unique account ID
  accountid:            String
  ssnTin:               String
  finInstId:            Long
  clientName:           String
  accountType:          String       // IRA | Roth IRA | 401K | Brokerage | Trust | Joint
  viewableSource:       String       // "Y" | "N"
  entitlements: {
    pxPartyRoleIdList:  Long[]
    advisoryContext:    String[]
    pxClient:           { pxClientId, pxClientName, Id, dataOwnerPartyRoleId }
    pxInvestorEntitlements: [{ partyRoleId, accountRole, accountSource, ... }]
  }
  repCodes: [{ advisorRepNumber, intType, repcodeSource }]
  advisors: [{ advisorId, advisorName, state, city, status, ... }]
  advisorHierarchy: [{ partyNodepathNm, partyNodePathValue }]
  holdings: [{
    fundId:             String
    fundName:           String
    fundTicker:         String       // e.g. "VTI", "SPY"
    mgtName:            String
    dividendRate:       Double
  }]
  category:             String       // "ins" | "inv" | "ret"
  ETLUpdateTS:          String
}
```

### Advisor Collection

Represents a financial advisor / book of business.

```
{
  _id:                      String   // Unique advisor ID
  advisorName:              String
  pxId:                     String
  advisorTaxId:             String
  userType:                 String
  finInstId:                Long
  advisorFullName:          String
  accountViewableMarketValue: Double // Total viewable market value
  viewableInvestorCount:    Long
  accountViewableCount:     Long
  repCodes: [{ advisorRepNumber, intType, repcodeSource }]
  holdings: [{ fundId, fundName, fundTicker, mgtName, dividendRate }]
  advisorHierarchy: [{ partyNodePathNm, partyNodePathValue }]
  entitlements: {
    pxPartyRoleIdList:      Long[]
    advisoryContext:        String[]
    pxClient: { pxClientId, pxClientName, Id, dataOwnerPartyRoleId }
  }
  state, city, zip, country: String
  status:                   String
  viewableSource:           String
  ETLUpdateTS:              String
}
```

---

## Oracle Relational Schema

The Oracle Relational target decomposes each document type into a parent table plus child tables for embedded arrays. Foreign keys enforce referential integrity.

**4 Parent Tables:**

| Table | Columns | Description |
|-------|---------|-------------|
| `rel_book_role_investor` | 35 | Investor scalar fields |
| `rel_book_role_group` | 18 | Group scalar fields |
| `rel_account` | 18 | Account scalar fields |
| `rel_advisor` | 28 | Advisor scalar fields |

**23 Child Tables:**

| Parent | Child Tables |
|--------|-------------|
| BookRoleInvestor (6) | `rel_bri_advisors`, `rel_bri_advisory_ctx`, `rel_bri_adv_book_roles`, `rel_bri_investor_hierarchy`, `rel_bri_persona_nm`, `rel_bri_synonyms` |
| BookRoleGroup (7) | `rel_brg_advisory_ctx`, `rel_brg_persona_nm`, `rel_brg_party_role_ids`, `rel_brg_advisors`, `rel_brg_adv_book_roles`, `rel_brg_adv_investors`, `rel_brg_hierarchy` |
| Account (8) | `rel_acct_party_role_ids`, `rel_acct_holdings`, `rel_acct_advisors`, `rel_acct_adv_book_roles`, `rel_acct_rep_codes`, `rel_acct_hierarchy`, `rel_acct_ent_inv_entitlements`, `rel_acct_advisory_ctx` |
| Advisor (5) | `rel_adv_hierarchy`, `rel_adv_party_role_ids`, `rel_adv_holdings`, `rel_adv_rep_codes`, `rel_adv_advisory_ctx` |

All child tables have foreign keys to their parent and are dropped in reverse dependency order during schema reset.

---

## Index Definitions

### MongoDB Indexes

Applied to both MongoDB Native and Oracle MongoDB API targets via the MongoDB wire protocol.

**bookRoleInvestor** (Q1-Q4):

| Index Keys | Purpose |
|------------|---------|
| `{investorType: 1, viewableSource: 1}` | Initial filter for Client + viewable |
| `{advisors.advisorId: 1, advisors.noOfViewableAccts: 1}` | Compound advisor array index (primary for Q1-Q4) |
| `{entitlements.advisoryContext: 1}` | Q3 entitlements filter |
| `{entitlements.pxClient.dataOwnerPartyRoleId: 1}` | Q3 data owner filter |
| `{partyRoleId: 1}` | Q2 party role exact match |
| `{entitlements.pxPartyRoleIdList: 1}` | User ID filter |

**bookRoleGroup** (Q5-Q6):

| Index Keys | Purpose |
|------------|---------|
| `{entitlements.advisoryContext: 1}` | Book ID filter |
| `{dataOwnerPartyRoleId: 1, personaNm: 1}` | Q5 owner + persona compound |
| `{totalViewableAccountsMarketValue: 1}` | Market value range scans |
| `{entitlements.pxPartyRoleIdList: 1}` | Q6 user ID filter |

**account** (Q7):

| Index Keys | Purpose |
|------------|---------|
| `{entitlements.pxPartyRoleIdList: 1, viewableSource: 1}` | User + viewable compound |
| `{holdings.fundTicker: 1}` | Fund ticker lookup |

**advisor** (Q8-Q9):

| Index Keys | Purpose |
|------------|---------|
| `{entitlements.pxClient.dataOwnerPartyRoleId: 1}` | Q8 data owner filter |
| `{advisorHierarchy.partyNodePathValue: 1}` | Q8 hierarchy path |
| `{entitlements.pxPartyRoleIdList: 1}` | Q9 user ID filter |
| `{accountViewableMarketValue: 1}` | Q9 market value range |

### Oracle JDBC Indexes (JSON Collections)

Oracle uses functional indexes on `json_value()` extractions and `MULTIVALUE INDEX` for JSON array elements. Applied to `jdbc_*` tables.

**jdbc_book_role_investor:**

| Index | Type | Expression |
|-------|------|------------|
| `idx_bri_inv_type` | Functional compound | `json_value(data, '$.investorType'), json_value(data, '$.viewableSource')` |
| `idx_bri_adv_id` | Multivalue | `data.advisors[*].advisorId.string()` |
| `idx_bri_adv_ctx` | Multivalue | `data.entitlements.advisoryContext[*].string()` |
| `idx_bri_party` | Multivalue | `data.entitlements.pxPartyRoleIdList[*].number()` |
| `idx_bri_party_role` | Functional | `json_value(data, '$.partyRoleId' RETURNING NUMBER)` |

**jdbc_book_role_group:**

| Index | Type | Expression |
|-------|------|------------|
| `idx_brg_adv_ctx` | Multivalue | `data.entitlements.advisoryContext[*].string()` |
| `idx_brg_owner` | Functional | `json_value(data, '$.dataOwnerPartyRoleId' RETURNING NUMBER)` |
| `idx_brg_persona` | Multivalue | `data.personaNm[*].string()` |
| `idx_brg_mkt_val` | Functional | `json_value(data, '$.totalViewableAccountsMarketValue' RETURNING NUMBER)` |
| `idx_brg_party` | Multivalue | `data.entitlements.pxPartyRoleIdList[*].number()` |

**jdbc_account:**

| Index | Type | Expression |
|-------|------|------------|
| `idx_acct_party` | Multivalue | `data.entitlements.pxPartyRoleIdList[*].number()` |
| `idx_acct_viewable` | Functional | `json_value(data, '$.viewableSource')` |
| `idx_acct_ticker` | Multivalue | `data.holdings[*].fundTicker.string()` |

**jdbc_advisor:**

| Index | Type | Expression |
|-------|------|------------|
| `idx_adv_owner` | Functional | `json_value(data, '$.entitlements.pxClient.dataOwnerPartyRoleId' RETURNING NUMBER)` |
| `idx_adv_hier` | Multivalue | `data.advisorHierarchy[*].partyNodePathValue.string()` |
| `idx_adv_party` | Multivalue | `data.entitlements.pxPartyRoleIdList[*].number()` |
| `idx_adv_mkt_val` | Functional | `json_value(data, '$.accountViewableMarketValue' RETURNING NUMBER)` |

### Oracle Relational Indexes

31 indexes covering all query access patterns on `rel_*` tables, including composite indexes on frequently co-filtered columns and indexes on child table join columns.

---

## Access Patterns (9 Queries)

Queries Q1-Q4 are aggregation pipelines on BookRoleInvestor. Queries Q5-Q9 are find operations on the remaining collections. Each query is executed identically across all 4 targets, and results are validated to match.

### Q1: Investor List by Advisor

Returns the top 50 investors for a specific advisor, sorted by viewable market value descending. The primary client-list query.

| Parameter | Type | Source |
|-----------|------|--------|
| `advisorId` | String | Random from generated advisors |

**MongoDB pipeline:**
```javascript
[
  { $match: {
      advisors: { $elemMatch: { advisorId: "?", noOfViewableAccts: { $gte: 1 } } },
      investorType: "Client",
      viewableSource: "Y"
  }},
  { $addFields: {
      advisors: {
        $filter: {
          input: "$advisors", as: "a",
          cond: { $and: [
            { $eq: ["$$a.advisorId", "?"] },
            { $gte: ["$$a.noOfViewableAccts", 1] }
          ]}
        }
      }
  }},
  { $unwind: "$advisors" },
  { $match: {
      "advisors.advisorId": "?",
      "advisors.noOfViewableAccts": { $gte: 1 }
  }},
  { $project: {
      _id: 1, investorFullName: 1, investorType: 1,
      investorLastName: 1, investorFirstName: 1, investorMiddleName: 1,
      investorCity: 1, investorState: 1, investorZipCode: 1, investorCountry: 1,
      ssnTin: 1, partyRoleId: 1, partyId: 1, finInstId: 1, clientAccess: 1, ETLUpdateTS: 1,
      advisorId: "$advisors.advisorId",
      viewableMarketValue: "$advisors.viewableMarketValue",
      noOfViewableAccts: "$advisors.noOfViewableAccts"
  }},
  { $sort: { viewableMarketValue: -1 } },
  { $limit: 50 }
]
```

**Note:** On MongoDB Native, a `$setWindowFields` stage computing `totalCount` is inserted before `$sort`. Oracle MongoDB API does not support `$setWindowFields`.

**Oracle MongoDB API:** Same pipeline with `.hint({"advisors.advisorId": 1, "advisors.noOfViewableAccts": 1})` to force use of the compound advisor index (Oracle's optimizer would otherwise choose the less selective `investorType+viewableSource` index).

**Oracle JDBC (JSON collections):**
```sql
SELECT * FROM (
  SELECT json_value(b.data, '$._id') AS "_id",
         json_value(b.data, '$.partyRoleId' RETURNING NUMBER) AS "partyRoleId",
         json_value(b.data, '$.partyId' RETURNING NUMBER) AS "partyId",
         json_value(b.data, '$.ssnTin') AS "ssnTin",
         json_value(b.data, '$.finInstId' RETURNING NUMBER) AS "finInstId",
         json_value(b.data, '$.investorType') AS "investorType",
         json_value(b.data, '$.investorLastName') AS "investorLastName",
         json_value(b.data, '$.investorFirstName') AS "investorFirstName",
         json_value(b.data, '$.investorMiddleName') AS "investorMiddleName",
         json_value(b.data, '$.investorFullName') AS "investorFullName",
         json_value(b.data, '$.investorCity') AS "investorCity",
         json_value(b.data, '$.investorState') AS "investorState",
         json_value(b.data, '$.investorZipCode') AS "investorZipCode",
         json_value(b.data, '$.clientAccess') AS "clientAccess",
         json_value(b.data, '$.ETLUpdateTS') AS "ETLUpdateTS",
         jt.advisor_id AS "advisorId",
         jt.viewable_mv AS "viewableMarketValue",
         jt.viewable_accts AS "noOfViewableAccts",
         COUNT(*) OVER () AS "totalCount"
  FROM jdbc_book_role_investor b,
       JSON_TABLE(b.data, '$.advisors[*]' COLUMNS (
           advisor_id     VARCHAR2(30) PATH '$.advisorId',
           viewable_mv    NUMBER       PATH '$.viewableMarketValue',
           viewable_accts NUMBER       PATH '$.noOfViewableAccts'
       )) jt
  WHERE json_exists(b.data, '$.advisors[*]?(@.advisorId == $aid)' PASSING ? AS "aid")
    AND json_value(b.data, '$.investorType') = 'Client'
    AND json_value(b.data, '$.viewableSource') = 'Y'
    AND jt.advisor_id = ?
    AND jt.viewable_accts >= 1
  ORDER BY jt.viewable_mv DESC
  FETCH FIRST 50 ROWS ONLY
)
```

**Oracle Relational:**
```sql
SELECT * FROM (
  SELECT b.id AS "_id",
         b.party_role_id AS "partyRoleId",
         b.party_id AS "partyId",
         b.ssn_tin AS "ssnTin",
         b.fin_inst_id AS "finInstId",
         b.investor_type AS "investorType",
         b.investor_last_name AS "investorLastName",
         b.investor_first_name AS "investorFirstName",
         b.investor_middle_name AS "investorMiddleName",
         b.investor_full_name AS "investorFullName",
         b.investor_city AS "investorCity",
         b.investor_state AS "investorState",
         b.investor_zip_code AS "investorZipCode",
         b.client_access AS "clientAccess",
         b.etl_update_ts AS "ETLUpdateTS",
         a.advisor_id AS "advisorId",
         a.viewable_mv AS "viewableMarketValue",
         a.no_of_viewable_accts AS "noOfViewableAccts",
         COUNT(*) OVER () AS "totalCount"
  FROM rel_book_role_investor b
  JOIN rel_bri_advisors a ON a.investor_id = b.id
  WHERE a.advisor_id = ?
    AND b.investor_type = 'Client'
    AND b.viewable_source = 'Y'
    AND a.no_of_viewable_accts >= 1
  ORDER BY a.viewable_mv DESC
  FETCH FIRST 50 ROWS ONLY
)
```

---

### Q2: Investor Search by Name (Regex)

Same structure as Q1 but adds a case-insensitive regex search on investor name plus a party role filter.

| Parameter | Type | Source |
|-----------|------|--------|
| `advisorId` | String | Random advisor |
| `partyRoleId` | Long | Random party role ID |
| `searchTerm` | String | Random surname fragment |

**Additional filters over Q1:**
```
viewableFlag = "Y"
partyRoleId = ?
investorFullName $regex /<searchTerm>/i
```

**Oracle JDBC:** Uses `UPPER(json_value(b.data, '$.investorFullName')) LIKE UPPER('%term%')`.

---

### Q3: Investor List by Entitlements + Advisor

Extends Q1 with entitlements-based access control filters.

| Parameter | Type | Source |
|-----------|------|--------|
| `advisorId` | String | Random advisor |
| `advisoryContext` | String | Random book ID |
| `dataOwnerPartyRoleId` | Long | Random IBDID |

**Additional filters over Q1:**
```
entitlements.advisoryContext = ?
entitlements.pxClient.dataOwnerPartyRoleId = ?
```

**Oracle JDBC:** Uses `json_exists(b.data, '$.entitlements.advisoryContext[*]?(@ == $ctx)' PASSING ? AS "ctx")`.

---

### Q4: Investor List with Market Value Range

Extends Q1 with a post-unwind market value range filter on the advisor's viewable market value.

| Parameter | Type | Source |
|-----------|------|--------|
| `advisorId` | String | Random advisor |
| `minMarketValue` | Double | Random base value |
| `maxMarketValue` | Double | min + $10M range |

**Additional post-unwind filter:**
```
advisors.viewableMarketValue >= ? AND <= ?
```

---

### Q5: Groups by Entitlements, Persona, Market Value

A find query against BookRoleGroup. Filters by authorization context, data owner, persona, visibility, and market value range.

| Parameter | Type | Source |
|-----------|------|--------|
| `advisoryContext` | String | Random book ID |
| `dataOwnerPartyRoleId` | Long | Random IBDID |
| `personaNm` | String | Random persona name |
| `minMarketValue` / `maxMarketValue` | Double | Random range |

**MongoDB filter:**
```
entitlements.advisoryContext = ?
dataOwnerPartyRoleId = ?
personaNm = ?
visibleFlag != "N"
totalViewableAccountsMarketValue BETWEEN ? AND ?
```

---

### Q6: Groups by Entitlements + User ID

Similar to Q5 but filters by user party role ID instead of data owner + persona.

| Parameter | Type | Source |
|-----------|------|--------|
| `advisoryContext` | String | Random book ID |
| `pxPartyRoleId` | Long | Random user ID |
| `minMarketValue` / `maxMarketValue` | Double | Random range |

---

### Q7: Accounts by Fund Ticker

Finds accounts visible to a user that hold a specific fund.

| Parameter | Type | Source |
|-----------|------|--------|
| `pxPartyRoleId` | Long | Random user ID |
| `fundTicker` | String | Random ticker (VTI, SPY, QQQ, etc.) |

**MongoDB filter:**
```
entitlements.pxPartyRoleIdList = ?
viewableSource = "Y"
holdings.fundTicker $in [?]
```

---

### Q8: Advisors by Hierarchy Path

Finds advisors under a specific organizational hierarchy node for a given institution.

| Parameter | Type | Source |
|-----------|------|--------|
| `dataOwnerPartyRoleId` | Long | Random IBDID |
| `partyNodePathValue` | String | Random hierarchy value |

---

### Q9: Advisors by Market Value Range

Finds advisors visible to a user whose book falls within a market value range.

| Parameter | Type | Source |
|-----------|------|--------|
| `pxPartyRoleId` | Long | Random user ID |
| `minMarketValue` / `maxMarketValue` | Double | Random range |

---

## Benchmark Workload

### Data Generation

Test data is generated using [Datafaker](https://www.datafaker.net/) with referentially consistent IDs maintained by a `ReferenceRegistry`. Data is loaded into all targets simultaneously during generation.

**Volume targets (~4.5 GB total):**

| Collection | Document Count | Avg Doc Size | Subtotal |
|------------|---------------|-------------|----------|
| Advisor | 3,000 | ~4 KB | ~12 MB |
| Account | 300,000 | ~5 KB | ~1.5 GB |
| BookRoleGroup | 90,000 | ~4 KB | ~360 MB |
| BookRoleInvestor | 450,000 | ~6 KB | ~2.7 GB |
| **Total** | **843,000** | | **~4.5 GB** |

**Generation order** (preserves referential integrity):

1. **Advisors** (3,000) — IDs stored in registry
2. **BookRoleInvestors** (450,000) — Each gets 1-4 random advisors embedded; investor IDs stored
3. **BookRoleGroups** (90,000) — Each gets 1-3 advisors embedded, each with nested investor refs
4. **Accounts** (300,000) — Each gets 1-2 advisors embedded

Documents are generated and loaded in chunks of 50,000 to manage memory. The harness detects existing data and skips generation on repeat runs.

**Shared reference pools:**

| Pool | Size | Format |
|------|------|--------|
| Advisory Context IDs (book IDs) | 15,000 | `"1000000000000" + i` |
| Party Role IDs (user IDs) | 30,000 | `35000000L + i` |
| Financial Institution IDs (IBDIDs) | 150 | `100L + i` |
| Fund Tickers | 200 | 20 real ETFs (VTI, SPY, QQQ...) + 180 random |
| Hierarchy Path Values | 500 | Mixed: firm IDs, region strings, team IDs |

**Weighted distributions:**
- `investorType`: 90% Client, 10% Prospect
- `viewableSource`: 80% Y, 20% N
- `viewableFlag`: 85% Y, 15% N
- `status`: 90% Active, 10% Inactive

### Execution Protocol

For each of the 36 benchmark runs (9 queries x 4 targets):

```
1. Sample 100 unique parameter sets from actual MongoDB data
   (ensures non-empty results)

2. Warm-up phase: 50 iterations
   - Results discarded
   - Allows JIT compilation and cache warming

3. Measurement phase: 200 iterations
   - Each iteration timed at nanosecond precision (System.nanoTime)
   - Per-iteration: generate params → execute query → drain results

4. Compute statistics:
   - p50, p95, p99 latencies
   - Average latency and throughput
```

### Result Validation

Before benchmarking, a validation step runs each query on every target and compares results:

- **Q1-Q4 (aggregation):** Compares projected fields row-by-row (sorted by viewableMarketValue DESC) — investor IDs, advisor IDs, and market values must match
- **Q5-Q9 (find):** Compares returned document ID sets across all targets

Validation failures are logged as warnings. The benchmark proceeds regardless but mismatches indicate a query or schema discrepancy.

### Metrics Collected

| Metric | Unit | Description |
|--------|------|-------------|
| p50 | ms | Median latency — 50th percentile |
| p95 | ms | 95th percentile latency |
| p99 | ms | 99th percentile latency (tail) |
| Average | ms | Arithmetic mean latency |
| Throughput | ops/sec | Operations per second over measurement window |

### HTML Report

The harness generates a standalone HTML file (`benchmark-report.html`) with all data embedded.

**Report sections:**

1. **Header** — JVM version, OS, generation timestamp
2. **Summary Table** — All 9 queries x all targets, showing p50/p95/p99 and throughput (fastest highlighted in green)
3. **Latency Bar Charts** — Grouped bar charts per query showing p50/p95/p99
4. **Throughput Comparison** — Horizontal bar chart across all queries
5. **Performance Radar** — Normalized p50 performance profile per target

Query details (MongoDB explain plans, SQL statements, Oracle SQL IDs) are captured and embedded in the report for post-hoc analysis.

---

## Getting Started

### Prerequisites

- **Java 21+** (JDK, not just JRE)
- **Maven 3.9+**
- **Docker** and **Docker Compose** (for database containers)
- ~10 GB free disk space (for data generation + database storage)

### Infrastructure Setup

1. **Start the database containers:**

```bash
cd docker
docker compose up -d
```

This launches:
- **MongoDB 8.2** on port `27017` — 4 GB memory limit, 2 CPUs, WiredTiger cache 1.5 GB, replica set `rs0`
- **Oracle ADB-Free 23ai** on ports `1521` (JDBC/TLS), `8443` (ORDS REST), `27018` (MongoDB API)

2. **Wait for Oracle to become healthy** (~5 minutes on first start):

```bash
docker compose ps
```

Both services should show `healthy`. Oracle's health check polls ORDS at 30-second intervals with a 5-minute startup grace period.

3. **Verify connectivity:**

```bash
# MongoDB
mongosh mongodb://localhost:27017

# Oracle MongoDB API
mongosh "mongodb://ADMIN:Welcome_12345!@localhost:27018/admin?authMechanism=PLAIN&authSource=\$external&tls=true&tlsAllowInvalidCertificates=true&loadBalanced=true&retryWrites=false"
```

### Build and Run Tests

```bash
cd harness

# Run all unit tests (291 tests)
mvn clean test

# Build the fat JAR (skip tests for speed)
mvn clean package -DskipTests
```

All 291 unit tests run offline — no database connections required.

### Running the Benchmark

```bash
cd harness

# Full benchmark using default config (benchmark-config.yaml)
java -jar target/helix-benchmark-1.0.0-SNAPSHOT.jar

# Or with a custom config file
java -jar target/helix-benchmark-1.0.0-SNAPSHOT.jar path/to/config.yaml
```

**What happens on a full run:**

1. **Data loading** (~5-15 minutes on first run): generates 843K documents and loads them to all 4 targets. Skipped on subsequent runs if data is already present.
2. **Index creation** (~10 seconds): creates all indexes. Safe to re-run (duplicates are caught).
3. **Result validation** (~7 minutes): runs each query on every target, compares results.
4. **Benchmark** (~10 minutes): 50 warmup + 200 measured iterations per query per target.
5. **Report generation**: writes `benchmark-report.html` in the `harness/` directory.

**Output files:**
- `harness/benchmark-report.html` — Main benchmark report with charts
- Console output with per-query timing summaries

### Smoke Test (Quick Validation)

For fast iteration, use the smoke config which uses tiny data volumes (750 investors, 50 advisors) and minimal iterations:

```bash
java -jar target/helix-benchmark-1.0.0-SNAPSHOT.jar smoke-config.yaml
```

### Configuration Reference

Edit `benchmark-config.yaml` to tune the benchmark:

```yaml
benchmark:
  warmUpIterations: 50          # Discarded warm-up runs per query
  measurementIterations: 200    # Measured runs per query
  batchSize: 1000               # MongoDB bulk insert batch size
  jdbcBatchSize: 500            # Oracle JDBC batch size
  activeTargets:                # Which targets to benchmark
    - MONGO_NATIVE
    - ORACLE_JDBC
    - ORACLE_MONGO_API
    - ORACLE_RELATIONAL
    # - ORACLE_DUALITY_VIEW     # Uncomment to enable
    # - ORACLE_MONGO_API_DV     # Uncomment to enable

dataGeneration:
  advisorCount: 3000
  accountCount: 300000
  bookRoleGroupCount: 90000
  bookRoleInvestorCount: 450000
  advisoryContextPoolSize: 15000
  partyRoleIdPoolSize: 30000
  finInstIdPoolSize: 150
  targetSizeGb: 4.5

connections:
  mongoNative:
    uri: "mongodb://localhost:27017/?replicaSet=rs0&w=1&journal=true"
    database: "helix"
  oracleJdbc:
    url: "jdbc:oracle:thin:@(description=...)"
    username: "ADMIN"
    password: "Welcome_12345!"
    maxPoolSize: 10
  oracleMongoApi:
    uri: "mongodb://ADMIN:Welcome_12345!@localhost:27018/admin?authMechanism=PLAIN&..."
    database: "ADMIN"
```

**Key configuration notes:**
- The harness auto-detects existing data and skips generation on repeat runs
- Oracle connections use TLS — the harness auto-discovers the truststore from `docker/truststore.jks`
- If relational or duality view targets are active, the harness automatically creates and populates the relational tables
- Query parameters are sampled from actual MongoDB data to guarantee non-empty result sets

---

## Project Structure

```
helix/
├── README.md
├── docs/                                        # Reference schemas & query samples
│   ├── Account.json
│   ├── Advisor.json
│   ├── BookRoleGroup.json
│   ├── BookRoleInvestor.json
│   ├── SampleMongoQueries.txt
│   └── erd.html
├── docker/
│   ├── docker-compose.yml                       # MongoDB 8.2 + Oracle ADB-Free 23ai
│   ├── mongo-init/01-init.js                    # Database + collection init
│   ├── keystore.jks                             # TLS keystore
│   └── truststore.jks                           # TLS truststore
└── harness/                                     # Java 21 Maven project
    ├── pom.xml
    ├── benchmark-config.yaml                    # Full benchmark configuration
    ├── smoke-config.yaml                        # Quick-run smoke test config
    ├── benchmark-report.html                    # Generated report (git-ignored)
    └── src/
        ├── main/java/com/helix/benchmark/
        │   ├── HelixBenchmarkMain.java          # Entry point & orchestrator
        │   ├── config/
        │   │   ├── BenchmarkConfig.java         # YAML config loader
        │   │   ├── DatabaseTarget.java          # MONGO_NATIVE | ORACLE_JDBC | ORACLE_MONGO_API | ...
        │   │   └── SchemaModel.java             # EMBEDDED
        │   ├── connection/
        │   │   └── ConnectionManager.java       # Connection strings & credentials
        │   ├── schema/
        │   │   ├── SchemaManager.java           # Interface
        │   │   ├── MongoSchemaManager.java      # MongoDB collection + index definitions
        │   │   ├── OracleSchemaManager.java     # JSON collection tables + indexes
        │   │   ├── OracleRelationalSchemaManager.java # 27 relational tables + 31 indexes
        │   │   └── OracleDualityViewSchemaManager.java # JSON Relational Duality Views
        │   ├── datagen/
        │   │   ├── ReferenceRegistry.java       # ID pools for referential integrity
        │   │   ├── TestDataGenerator.java       # Datafaker-based document generator
        │   │   ├── DataLoader.java              # Bulk insert (MongoDB + Oracle JDBC JSON)
        │   │   └── RelationalDataLoader.java    # Document → relational table decomposition
        │   ├── query/
        │   │   ├── QueryDefinition.java         # Enum: Q1-Q9 with metadata
        │   │   ├── QueryParameterGenerator.java # Samples valid parameters from actual data
        │   │   ├── MongoQueryExecutor.java      # MongoDB Native + Oracle MongoDB API
        │   │   ├── OracleJdbcQueryExecutor.java # SQL/JSON against jdbc_* tables
        │   │   ├── OracleRelationalQueryExecutor.java # Standard SQL against rel_* tables
        │   │   ├── OracleDualityViewQueryExecutor.java # SQL/JSON against dv_* views
        │   │   ├── ResultValidator.java         # Cross-target result comparison
        │   │   └── AllQueryRunner.java          # Interactive all-query execution tool
        │   ├── benchmark/
        │   │   ├── BenchmarkRunner.java         # Warm-up + measurement loop
        │   │   ├── LatencyTracker.java          # Nanosecond timing, percentile math
        │   │   ├── BenchmarkResult.java         # p50/p95/p99, throughput, metadata
        │   │   └── QueryDetail.java             # Explain plans, SQL IDs, ORDS URLs
        │   └── report/
        │       ├── HtmlReportGenerator.java     # Benchmark report with Chart.js
        │       └── QueryResultsHtmlGenerator.java # Query result comparison report
        ├── main/resources/
        │   └── logback.xml                      # Logging configuration
        └── test/java/com/helix/benchmark/       # 291 unit tests
            ├── HelixBenchmarkMainTest.java
            ├── benchmark/
            │   ├── BenchmarkResultTest.java
            │   ├── BenchmarkRunnerTest.java
            │   ├── LatencyTrackerTest.java
            │   └── QueryDetailTest.java
            ├── config/
            │   ├── BenchmarkConfigTest.java
            │   ├── DatabaseTargetTest.java
            │   └── SchemaModelTest.java
            ├── connection/
            │   └── ConnectionManagerTest.java
            ├── datagen/
            │   ├── DataLoaderTest.java
            │   ├── ReferenceRegistryTest.java
            │   ├── RelationalDataLoaderTest.java
            │   └── TestDataGeneratorTest.java
            ├── query/
            │   ├── MongoQueryExecutorTest.java
            │   ├── OracleJdbcQueryExecutorTest.java
            │   ├── OracleRelationalQueryExecutorTest.java
            │   ├── OracleDualityViewQueryExecutorTest.java
            │   ├── QueryDefinitionTest.java
            │   └── QueryParameterGeneratorTest.java
            ├── report/
            │   └── HtmlReportGeneratorTest.java
            └── schema/
                ├── SchemaManagerTest.java
                ├── OracleRelationalSchemaManagerTest.java
                └── OracleDualityViewSchemaManagerTest.java
```

---

## Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Java | 21 | Runtime |
| MongoDB Java Driver | 5.3.1 | Native MongoDB + Oracle MongoDB API |
| Oracle JDBC (ojdbc17) | 23.7.0.25.01 | Oracle 23ai JDBC + SQL/JSON |
| HikariCP | 6.2.1 | JDBC connection pooling |
| Datafaker | 2.4.2 | Realistic synthetic data generation |
| Jackson | 2.18.2 | YAML config + JSON serialization |
| Logback | 1.5.16 | Structured logging |
| Chart.js | 4.x (CDN) | HTML report visualizations |
| JUnit 5 | 5.11.4 | Unit testing |
| Mockito | 5.15.2 | Test mocking |
| AssertJ | 3.27.3 | Fluent test assertions |
| MongoDB | 8.2 | Native document database |
| Oracle ADB-Free | 23ai | Oracle with ORDS MongoDB API |
| Docker Compose | 3.8 | Container orchestration |
