# Helix Database Benchmark Harness

A Java benchmark harness that evaluates 9 MongoDB aggregation query patterns across 3 database targets and 2 schema models (6 configurations total), generates ~1.5 GB of synthetic test data, and produces a self-contained HTML report comparing latency and throughput.

---

## Table of Contents

- [Test Matrix](#test-matrix)
- [Schema Model A: Embedded (4 Collections)](#schema-model-a-embedded-4-collections)
  - [Account Collection](#account-collection)
  - [Advisor Collection](#advisor-collection)
  - [BookRoleGroup Collection](#bookrolegroup-collection)
  - [BookRoleInvestor Collection](#bookroleinvestor-collection)
- [Schema Model B: Normalized (1 Collection)](#schema-model-b-normalized-1-collection)
  - [Normalization Rules](#normalization-rules)
  - [advisorsMetadata Structure](#advisorsmetadata-structure)
- [Index Definitions](#index-definitions)
  - [MongoDB Indexes — Model A (Embedded)](#mongodb-indexes--model-a-embedded)
  - [MongoDB Indexes — Model B (Normalized)](#mongodb-indexes--model-b-normalized)
  - [Oracle JDBC Indexes — Model A (Embedded)](#oracle-jdbc-indexes--model-a-embedded)
  - [Oracle JDBC Indexes — Model B (Normalized)](#oracle-jdbc-indexes--model-b-normalized)
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
  - [Metrics Collected](#metrics-collected)
  - [HTML Report](#html-report)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Infrastructure](#infrastructure)
  - [Build and Run](#build-and-run)
  - [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)

---

## Test Matrix

The harness benchmarks every query across 6 configurations (3 targets x 2 models):

| # | Target | Schema Model | Collections/Tables | Driver |
|---|--------|--------------|--------------------|--------|
| 1 | Native MongoDB 8.2 | A: Embedded (4 collections) | account, advisor, bookRoleGroup, bookRoleInvestor | mongodb-driver-sync 5.3 |
| 2 | Native MongoDB 8.2 | B: Normalized (1 collection) | helix | mongodb-driver-sync 5.3 |
| 3 | Oracle 26ai JDBC | A: Embedded (4 tables) | account, advisor, book_role_group, book_role_investor | ojdbc17 23.7 |
| 4 | Oracle 26ai JDBC | B: Normalized (1 table) | helix | ojdbc17 23.7 |
| 5 | Oracle 26ai MongoDB API | A: Embedded (4 collections) | account, advisor, bookRoleGroup, bookRoleInvestor | mongodb-driver-sync 5.3 via ORDS |
| 6 | Oracle 26ai MongoDB API | B: Normalized (1 collection) | helix | mongodb-driver-sync 5.3 via ORDS |

---

## Schema Model A: Embedded (4 Collections)

Model A uses MongoDB's embedded document pattern. Each domain entity lives in its own collection, with advisor details denormalized (embedded) into every parent document.

### Account Collection

Represents a financial account. Advisors are embedded directly.

```
{
  _id:                  String       // Unique account ID (e.g. "1000001558515")
  accountid:            String       // Display ID (e.g. "A0123")
  ssnTin:               String       // Tax identifier
  finInstId:            Long         // Financial institution ID (IBDID)
  clientName:           String       // Client full name
  clientId:             String       // Client identifier
  finInstName:          String       // Institution name
  accountType:          String       // IRA | Roth IRA | 401K | Brokerage | Trust | Joint
  acctName:             String       // Account display name
  viewable:             Boolean      // Viewability flag
  viewableSource:       String       // "Y" or "N"
  setupTmst:            Date         // Account creation date
  updateTmst:           Date         // Last modification date
  entitlements: {
    pxPartyRoleIdList:  Long[]       // Authorized user IDs
    advisoryContext:    String[]     // Authorized book IDs
    pxClient: {
      pxClientId:       String
      pxClientName:     String
      Id:               String       // IBDID
      dataOwnerPartyRoleId: Long     // IBDID
    }
    pxInvestorEntitlements: [{       // Per-investor access grants
      partyRoleId:      Long
      accountRole:      String       // "View Only" | "Full Access"
      accountSource:    String
      accountAccessStatus: String    // "Approved"
      investorId:       String
      accountRoleCode:  String
    }]
  }
  repCodes: [{                       // Representative codes
    advisorRepNumber:   String
    intType:            Integer
    repcodeSource:      String
  }]
  advisors: [{                       // EMBEDDED advisor details
    advisorId:          String       // FK to Advisor._id
    advisorName:        String
    advisorTaxId:       String
    finInstId:          Long
    lastName:           String
    firstName:          String
    state:              String
    city:               String
    zipCode:            String
    country:            String
    businessPhone:      String
    status:             String       // "Active" | "Inactive"
  }]
  advisorHierarchy: [{               // Organizational hierarchy
    partyNodepathNm:    String       // "Firm" | "Region" | "IPPersonTeam"
    partyNodePathValue: String       // Hierarchy node value
  }]
  holdings: [{                       // Portfolio holdings
    fundId:             String
    fundName:           String
    fundTicker:         String       // e.g. "VTI", "SPY"
    mgtName:            String
    dividendRate:       Double
  }]
  category:             String       // "ins" | "inv" | "ret"
  ETLUpdateTS:          String       // ETL timestamp
}
```

### Advisor Collection

Represents a financial advisor / book of business.

```
{
  _id:                      String   // Unique advisor ID (e.g. "1000000000000")
  advisorName:              String   // Full name
  pxId:                     String   // Platform ID (e.g. "F52")
  partyNodeLabelId:         String   // Hierarchy label ID
  advisorTaxId:             String   // Tax identifier
  userType:                 String   // "bcd" | "abc" | "xyz"
  finInstId:                Long     // IBDID
  advisorFullName:          String   // Display name
  advState:                 String   // State code
  advSetupTmst:             Date     // Creation date
  advUpdateTmst:            Date     // Last update
  advAcctMethod:            String   // "avd" | "avg" | "ffo"
  accountViewableMarketValue: Double // Total market value of viewable accounts
  viewableInvestorCount:    Long     // Number of viewable investors
  accountViewableCount:     Long     // Number of viewable accounts
  repCodes: [{
    advisorRepNumber:       String
    intType:                Integer
    repcodeSource:          String
  }]
  holdings: [{
    fundId:                 String
    fundName:               String
    fundTicker:             String
    mgtName:                String
    dividendRate:           Double
  }]
  advisorHierarchy: [{
    partyNodePathNm:        String   // "Firm" | "Region" | "IPPersonTeam"
    partyNodePathValue:     String
  }]
  entitlements: {
    pxPartyRoleIdList:      Long[]   // User IDs
    advisoryContext:        String[] // Book IDs
    pxClient: {
      pxClientId:           String
      pxClientName:         String
      Id:                   String   // IBDID
      dataOwnerPartyRoleId: Long     // IBDID
    }
  }
  state:                    String
  city:                     String
  zip:                      String
  country:                  String   // "USA"
  status:                   String   // "VIEWABLE" | "Active"
  viewableSource:           String   // "Y" | "N"
  ETLUpdateTS:              String
}
```

### BookRoleGroup Collection

Represents a group-level book of business (performance group, household, etc.). Contains embedded advisors, each with a nested investors array.

```
{
  _id:                              String   // "finInstId_groupId"
  investorWriId:                    String
  etlSourceGroup:                   String   // "xyz" | "abc"
  finInstId:                        Long     // IBDID
  personaNm:                        String[] // ["Home Office", "Wove Administrator", ...]
  entity:                           String   // "Group"
  dataOwnerPartyRoleId:             Long     // IBDID
  advisorHierarchy: [{
    partyNodePathValue:             String
  }]
  entitlements: {
    advisoryContext:                String[]  // Book IDs
    pxClient: {
      pxClientNm:                   String
      dataOwnerPartyRoleId:         Long
    }
    pxPartyRoleIdList:             Long[]    // User IDs
  }
  accountCount:                     Long
  totalMarketValue:                 Double
  totalViewableAccountCount:        Long
  totalViewableAccountsMarketValue: Double
  advisors: [{                               // EMBEDDED advisors
    advisorId:                      String
    advisorTaxId:                   String
    finInstId:                      Long
    firstName:                      String
    middleName:                     String
    lastName:                       String
    advisorName:                    String
    bookRoles:                      String[] // ["Home Office", "Primary", ...]
    bookType:                       String   // "WRI" | "ADV"
    totalViewableAccountsMarketValue: Double
    totalViewableAccountCount:      Integer
    investors: [{                            // NESTED investors
      investorId:                   String
    }]
    noOfViewableAccts:              Long
    viewableMarketValue:            Double
    status:                         String   // "Active" | "Inactive"
  }]
  accountGroupName:                 String
  accountGroupId:                   String
  accountGroupType:                 String   // "Performance" | "Standard" | "Custom"
  visibleFlag:                      String   // "Y" | "N"
  portfolioType:                    String   // "A" | "B" | "C"
  ETLUpdateTS:                      String
}
```

### BookRoleInvestor Collection

Represents an investor/client record. The most voluminous collection. Contains embedded advisors with per-advisor metrics.

```
{
  _id:                              String   // "finInstId_investorId"
  partyRoleId:                      Long
  partyId:                          Long     // User ID
  conversionInProgress:             Boolean
  dataOwnerPartyRoleId:             Long     // IBDID
  entitlements: {
    advisoryContext:                String[]  // Book IDs
    pxClients: {
      pxClientNm:                   String
      dataOwnerPartyRoleId:         Long
    }
    pxPartyRoleIdList:             Long[]    // User IDs
  }
  advisorHierarchy: [{
    partyNodePathNm:                String   // "Region" | "Firm" | "IPPersonTeam"
    partyNodePathValue:             String
  }]
  investorId:                       String   // Client ID
  personaNm:                        String[] // ["Home Office", "Wove Administrator", ...]
  entity:                           String   // "Client"
  totalMarketValue:                 Double
  totalAccounts:                    Long
  totalViewableAccountsMarketValue: Double
  totalViewableAccountCount:        Long
  advisors: [{                               // EMBEDDED advisors
    advisorId:                      String   // FK to Advisor._id
    advisorTaxId:                   String
    advisorName:                    String
    state:                          String
    finInstId:                      Long
    lastName:                       String
    firstName:                      String
    middleName:                     String
    advAddrLine1:                   String
    city:                           String
    zipCode:                        String
    country:                        String
    businessPhone:                  String
    fax:                            String
    email:                          String
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
  investorLastName:                 String
  investorFirstName:                String
  investorMiddleName:               String
  investorFullName:                 String   // Used in Q2 regex search
  investorpartyRoleId:              Long
  investorCity:                     String
  investorState:                    String
  investorZipCode:                  String
  investorBirthdate:                Date
  viewableFlag:                     String   // "Y" | "N"
  viewableSource:                   String   // "Y" | "N"
  clientAccess:                     String   // "Invite" | "Full" | "View Only"
  trustFlag:                        String
  riskProfile:                      Object   // Empty or risk data
  synonyms: [{
    partySynonymTypeCd:             String   // "TID" | "XID" | "WID" | "SID"
    partySynonymStr:                String
  }]
  ETLUpdateTS:                      String
}
```

---

## Schema Model B: Normalized (1 Collection)

Model B merges all 4 document types into a single collection named `helix` and introduces a `type` discriminator field. Embedded advisor arrays are replaced by ID references and a denormalized metadata array.

### Normalization Rules

| Model A | Model B |
|---------|---------|
| 4 separate collections | 1 collection `helix` |
| No `type` field | `"type": "Account" \| "Advisor" \| "BookRoleGroup" \| "BookRoleInvestor"` |
| Advisor has no explicit `advisorId` | `"advisorId"` added — copy of `_id` |
| `advisors[]` embedded array | Replaced with `"advisorIds": ["id1", "id2", ...]` |
| Advisor metrics embedded in parent | Denormalized into `"advisorsMetadata[]"` with query-critical fields |
| BookRoleGroup `advisors[].investors[]` | Extracted to top-level `"investorIds": ["id1", ...]` |

### advisorsMetadata Structure

Kept on Account, BookRoleGroup, and BookRoleInvestor to avoid `$lookup` for common queries:

```json
"advisorsMetadata": [
  {
    "advisorId":            "1000000001234",
    "advisorName":          "KEVIN F XYZ",
    "noOfViewableAccts":    5,
    "viewableMarketValue":  4106016.96,
    "bookRoles":            ["Home Office"],
    "status":               "Active"
  }
]
```

This covers all fields referenced in Q1-Q4 `$project` and post-`$unwind` `$match` stages without needing `$lookup`.

---

## Index Definitions

### MongoDB Indexes — Model A (Embedded)

**bookRoleInvestor** (serves Q1-Q4):

| Index Keys | Purpose |
|------------|---------|
| `{investorType: 1, viewableSource: 1}` | Initial filter for Client + viewable |
| `{advisors.advisorId: 1, advisors.noOfViewableAccts: 1}` | Advisor elemMatch |
| `{entitlements.advisoryContext: 1}` | Q3 entitlements filter |
| `{entitlements.pxClient.dataOwnerPartyRoleId: 1}` | Q3 data owner filter |
| `{partyRoleId: 1}` | Q2 party role exact match |
| `{entitlements.pxPartyRoleIdList: 1}` | User ID filter |
| `{advisorHierarchy.partyNodePathNm: 1, advisorHierarchy.partyNodePathValue: 1}` | Hierarchy compound |

**bookRoleGroup** (serves Q5-Q6):

| Index Keys | Purpose |
|------------|---------|
| `{entitlements.advisoryContext: 1}` | Book ID filter |
| `{dataOwnerPartyRoleId: 1, personaNm: 1}` | Q5 owner + persona compound |
| `{totalViewableAccountsMarketValue: 1}` | Market value range scans |
| `{entitlements.pxPartyRoleIdList: 1}` | Q6 user ID filter |
| `{advisorHierarchy.partyNodePathNm: 1, advisorHierarchy.partyNodePathValue: 1}` | Hierarchy compound |

**account** (serves Q7):

| Index Keys | Purpose |
|------------|---------|
| `{entitlements.pxPartyRoleIdList: 1, viewableSource: 1}` | User + viewable compound |
| `{holdings.fundTicker: 1}` | Fund ticker lookup |
| `{advisorHierarchy.partyNodepathNm: 1, advisorHierarchy.partyNodePathValue: 1}` | Hierarchy compound |

**advisor** (serves Q8-Q9):

| Index Keys | Purpose |
|------------|---------|
| `{entitlements.pxClient.dataOwnerPartyRoleId: 1}` | Q8 data owner filter |
| `{advisorHierarchy.partyNodePathValue: 1}` | Q8 hierarchy path |
| `{entitlements.pxPartyRoleIdList: 1}` | Q9 user ID filter |
| `{accountViewableMarketValue: 1}` | Q9 market value range |
| `{advisorHierarchy.partyNodePathNm: 1, advisorHierarchy.partyNodePathValue: 1}` | Hierarchy compound |

### MongoDB Indexes — Model B (Normalized)

All indexes target the single `helix` collection. Every index includes the `type` discriminator as a prefix to partition the index space.

| Index Keys | Purpose |
|------------|---------|
| `{type: 1, advisorId: 1}` | Advisor lookup by type + ID |
| `{type: 1, entitlements.advisoryContext: 1}` | Cross-type book ID filter |
| `{type: 1, entitlements.pxPartyRoleIdList: 1}` | Cross-type user ID filter |
| `{type: 1, investorType: 1, viewableSource: 1}` | Q1-Q4 investor triple filter |
| `{type: 1, advisorsMetadata.advisorId: 1}` | Denormalized advisor ID lookup |
| `{type: 1, dataOwnerPartyRoleId: 1, personaNm: 1}` | Q5 group filter |
| `{type: 1, holdings.fundTicker: 1}` | Q7 fund ticker |
| `{type: 1, accountViewableMarketValue: 1}` | Q9 market value range |
| `{advisorHierarchy.partyNodePathNm: 1, advisorHierarchy.partyNodePathValue: 1}` | Hierarchy compound |
| `{advisorIds: 1}` | FK array lookup |

### Oracle JDBC Indexes — Model A (Embedded)

Oracle uses functional indexes on `json_value()` extractions and `MULTIVALUE INDEX` for array elements.

**book_role_investor:**

| Index | Type | Expression |
|-------|------|------------|
| `idx_bri_inv_type` | Functional | `json_value(data, '$.investorType'), json_value(data, '$.viewableSource')` |
| `idx_bri_adv_id` | Multivalue | `b.data.advisors[*].advisorId.string()` |
| `idx_bri_adv_ctx` | Multivalue | `b.data.entitlements.advisoryContext[*].string()` |
| `idx_bri_party` | Multivalue | `b.data.entitlements.pxPartyRoleIdList[*].number()` |
| `idx_bri_party_role` | Functional | `json_value(data, '$.partyRoleId' RETURNING NUMBER)` |

**book_role_group:**

| Index | Type | Expression |
|-------|------|------------|
| `idx_brg_adv_ctx` | Multivalue | `b.data.entitlements.advisoryContext[*].string()` |
| `idx_brg_owner` | Functional | `json_value(data, '$.dataOwnerPartyRoleId' RETURNING NUMBER)` |
| `idx_brg_persona` | Multivalue | `b.data.personaNm[*].string()` |
| `idx_brg_mkt_val` | Functional | `json_value(data, '$.totalViewableAccountsMarketValue' RETURNING NUMBER)` |
| `idx_brg_party` | Multivalue | `b.data.entitlements.pxPartyRoleIdList[*].number()` |

**account:**

| Index | Type | Expression |
|-------|------|------------|
| `idx_acct_party` | Multivalue | `a.data.entitlements.pxPartyRoleIdList[*].number()` |
| `idx_acct_viewable` | Functional | `json_value(data, '$.viewableSource')` |
| `idx_acct_ticker` | Multivalue | `a.data.holdings[*].fundTicker.string()` |

**advisor:**

| Index | Type | Expression |
|-------|------|------------|
| `idx_adv_owner` | Functional | `json_value(data, '$.entitlements.pxClient.dataOwnerPartyRoleId' RETURNING NUMBER)` |
| `idx_adv_hier` | Multivalue | `a.data.advisorHierarchy[*].partyNodePathValue.string()` |
| `idx_adv_party` | Multivalue | `a.data.entitlements.pxPartyRoleIdList[*].number()` |
| `idx_adv_mkt_val` | Functional | `json_value(data, '$.accountViewableMarketValue' RETURNING NUMBER)` |

### Oracle JDBC Indexes — Model B (Normalized)

All indexes target the single `helix` JSON collection table.

| Index | Type | Expression |
|-------|------|------------|
| `idx_helix_type` | Functional | `json_value(data, '$.type')` |
| `idx_helix_type_advid` | Functional compound | `json_value(data, '$.type'), json_value(data, '$.advisorId')` |
| `idx_helix_inv_filter` | Functional compound | `json_value(data, '$.type'), json_value(data, '$.investorType'), json_value(data, '$.viewableSource')` |
| `idx_helix_advisor_ids` | Multivalue | `h.data.advisorIds[*].string()` |
| `idx_helix_adv_ctx` | Multivalue | `h.data.entitlements.advisoryContext[*].string()` |
| `idx_helix_party_roles` | Multivalue | `h.data.entitlements.pxPartyRoleIdList[*].number()` |
| `idx_helix_hier_nm` | Multivalue | `h.data.advisorHierarchy[*].partyNodePathNm.string()` |
| `idx_helix_hier_val` | Multivalue | `h.data.advisorHierarchy[*].partyNodePathValue.string()` |
| `idx_helix_mkt_val` | Functional compound | `json_value(data, '$.type'), json_value(data, '$.accountViewableMarketValue')` |
| `idx_helix_persona` | Multivalue | `h.data.personaNm[*].string()` |
| `idx_helix_fund_ticker` | Multivalue | `h.data.holdings[*].fundTicker.string()` |

---

## Access Patterns (9 Queries)

Each query is executed against all 6 configurations. Queries Q1-Q4 are aggregation pipelines (unwind + project + sort + limit). Queries Q5-Q9 are find operations.

### Q1: Investor List by Advisor

Returns investors for a specific advisor, sorted by market value. The primary client-list query.

| Parameter | Type | Source |
|-----------|------|--------|
| `advisorId` | String | Random from generated advisors |

**MongoDB pipeline (Model A):**
```
$match   → investorType = "Client" AND viewableSource = "Y"
           AND advisors $elemMatch {advisorId, noOfViewableAccts >= 1}
$unwind  → $advisors
$match   → advisors.advisorId = ? AND advisors.noOfViewableAccts >= 1
$project → 16 investor fields + 3 computed advisor fields
$setWindowFields → totalCount (native MongoDB only)
$sort    → advisor.viewableMarketValue DESC
$limit   → 50
```

**Model B variant:** Replaces `$unwind "$advisors"` with `$unwind "$advisorsMetadata"`, adds `type: "BookRoleInvestor"` to the initial `$match`.

**Oracle JDBC translation:**
```sql
SELECT json_serialize(matched.* PRETTY) FROM (
  SELECT jt.*, COUNT(*) OVER () AS total_count
  FROM book_role_investor b,
       JSON_TABLE(b.data, '$.advisors[*]' COLUMNS (
           advisor_id VARCHAR2(30) PATH '$.advisorId',
           viewable_mv NUMBER PATH '$.viewableMarketValue',
           viewable_accts NUMBER PATH '$.noOfViewableAccts'
       )) jt
  WHERE json_value(b.data, '$.investorType') = 'Client'
    AND json_value(b.data, '$.viewableSource') = 'Y'
    AND jt.advisor_id = ?
    AND jt.viewable_accts >= 1
  ORDER BY jt.viewable_mv DESC
  FETCH FIRST 50 ROWS ONLY
) matched
```

**Oracle MongoDB API note:** `$setWindowFields` is not supported. The harness removes this stage and uses a separate `countDocuments()` call instead.

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

**Oracle JDBC:** Uses `UPPER(json_value(b.data, '$.investorFullName')) LIKE UPPER(?)` with `%term%` wildcards.

---

### Q3: Investor List by Entitlements + Advisor

Extends Q1 with entitlements-based access control — filters by advisory context (book ID) and data owner.

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
| `minMarketValue` | Double | Random 1.0 - 1,000.0 |
| `maxMarketValue` | Double | min + random 1,000 - 50,000 |

**Additional post-unwind filter:**
```
advisors.viewableMarketValue >= ? AND <= ?
```

---

### Q5: Groups by Entitlements, Persona, Market Value

A find query against BookRoleGroup. Filters by authorization context, data owner institution, user persona, visibility, and market value range.

| Parameter | Type | Source |
|-----------|------|--------|
| `advisoryContext` | String | Random book ID |
| `dataOwnerPartyRoleId` | Long | Random IBDID |
| `personaNm` | String | "Home Office" |
| `minMarketValue` | Double | Random 0 - 1,000 |
| `maxMarketValue` | Double | min + random 10,000 - 100,000 |

**MongoDB filter:**
```
entitlements.advisoryContext = ?
dataOwnerPartyRoleId = ?
personaNm = ?
visibleFlag != "N"
totalViewableAccountsMarketValue BETWEEN ? AND ?
```

**Oracle JDBC:**
```sql
WHERE json_exists(b.data, '$.entitlements.advisoryContext[*]?(@ == $ctx)' PASSING ? AS "ctx")
  AND json_value(b.data, '$.dataOwnerPartyRoleId' RETURNING NUMBER) = ?
  AND json_exists(b.data, '$.personaNm[*]?(@ == $pnm)' PASSING ? AS "pnm")
  AND NOT json_value(b.data, '$.visibleFlag') = 'N'
  AND json_value(b.data, '$.totalViewableAccountsMarketValue' RETURNING NUMBER) BETWEEN ? AND ?
```

---

### Q6: Groups by Entitlements + User ID

Similar to Q5 but filters by user party role ID instead of data owner + persona.

| Parameter | Type | Source |
|-----------|------|--------|
| `advisoryContext` | String | Random book ID |
| `pxPartyRoleId` | Long | Random user ID |
| `minMarketValue` | Double | Random 0 - 100 |
| `maxMarketValue` | Double | min + random 100 - 10,000 |

**MongoDB filter:**
```
entitlements.advisoryContext = ?
entitlements.pxPartyRoleIdList = ?
visibleFlag != "N"
totalViewableAccountsMarketValue BETWEEN ? AND ?
```

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

**Oracle JDBC:**
```sql
WHERE json_exists(a.data, '$.entitlements.pxPartyRoleIdList[*]?(@ == $uid)' PASSING ? AS "uid")
  AND json_value(a.data, '$.viewableSource') = 'Y'
  AND json_exists(a.data, '$.holdings[*]?(@.fundTicker == $ticker)' PASSING ? AS "ticker")
```

---

### Q8: Advisors by Hierarchy Path

Finds advisors under a specific organizational hierarchy node for a given institution.

| Parameter | Type | Source |
|-----------|------|--------|
| `dataOwnerPartyRoleId` | Long | Random IBDID |
| `partyNodePathValue` | String | Random hierarchy value |

**MongoDB filter:**
```
entitlements.pxClient.dataOwnerPartyRoleId = ?
advisorHierarchy.partyNodePathValue $in [?]
```

**Oracle JDBC:**
```sql
WHERE json_value(a.data, '$.entitlements.pxClient.dataOwnerPartyRoleId' RETURNING NUMBER) = ?
  AND json_exists(a.data, '$.advisorHierarchy[*]?(@.partyNodePathValue == $val)' PASSING ? AS "val")
```

---

### Q9: Advisors by Market Value Range

Finds advisors visible to a user whose book falls within a market value range.

| Parameter | Type | Source |
|-----------|------|--------|
| `pxPartyRoleId` | Long | Random user ID |
| `minMarketValue` | Double | Random 0 - 1,000,000 |
| `maxMarketValue` | Double | min + random 1M - 40M |

**MongoDB filter:**
```
entitlements.pxPartyRoleIdList = ?
accountViewableMarketValue >= ? AND <= ?
```

---

## Benchmark Workload

### Data Generation

Test data is generated using [Datafaker](https://www.datafaker.net/) with referentially consistent IDs maintained by a `ReferenceRegistry`.

**Volume targets (~1.5 GB total):**

| Collection | Document Count | Avg Doc Size | Subtotal |
|------------|---------------|-------------|----------|
| Advisor | 1,000 | ~4 KB | ~4 MB |
| Account | 100,000 | ~5 KB | ~500 MB |
| BookRoleGroup | 30,000 | ~4 KB | ~120 MB |
| BookRoleInvestor | 150,000 | ~6 KB | ~900 MB |
| **Total** | **281,000** | | **~1.5 GB** |

**Generation order** (preserves referential integrity):

1. **Advisors** (1,000) — IDs stored in registry
2. **BookRoleInvestors** (150,000) — Each gets 1-4 random advisors embedded; investor IDs stored in registry
3. **BookRoleGroups** (30,000) — Each gets 1-3 advisors embedded, each advisor gets 1-5 nested investor refs
4. **Accounts** (100,000) — Each gets 1-2 advisors embedded, investor entitlements reference existing IDs

**Shared reference pools:**

| Pool | Size | Format |
|------|------|--------|
| Advisory Context IDs (book IDs) | 5,000 | `"1000000000000" + i` |
| Party Role IDs (user IDs) | 10,000 | `35000000L + i` |
| Financial Institution IDs (IBDIDs) | 50 | `100L + i` |
| Fund Tickers | 200 | 20 real ETFs (VTI, SPY, QQQ...) + 180 random |
| Hierarchy Path Values | 500 | Mixed: firm IDs, region strings, team IDs |

**Weighted distributions:**
- `investorType`: 90% Client, 10% Prospect
- `viewableSource`: 80% Y, 20% N
- `viewableFlag`: 85% Y, 15% N
- `visibleFlag`: 85% Y, 15% N
- `status`: 90% Active, 10% Inactive

### Execution Protocol

For each of the 54 benchmark runs (9 queries x 6 configurations):

```
1. Generate 200 unique parameter sets using QueryParameterGenerator
   (parameters drawn from ReferenceRegistry pools to ensure non-empty results)

2. Warm-up phase: Execute 50 iterations
   - Results discarded
   - Allows JIT compilation and cache warming

3. Measurement phase: Execute 200 iterations
   - Each iteration timed at nanosecond precision (System.nanoTime)
   - Per-iteration: generate params → execute query → drain results

4. Compute statistics:
   - p50 (median), p95, p99 latencies
   - Average latency
   - Throughput (operations/second)
   - Total elapsed wall-clock time
```

### Metrics Collected

| Metric | Unit | Description |
|--------|------|-------------|
| p50 | ms | Median latency — 50th percentile |
| p95 | ms | 95th percentile latency |
| p99 | ms | 99th percentile latency (tail) |
| Average | ms | Arithmetic mean latency |
| Throughput | ops/sec | Operations per second over measurement window |
| Iterations | count | Number of measured iterations (200) |

### HTML Report

The harness generates a standalone HTML file (`benchmark-report.html`) with all data embedded — no external dependencies except a Chart.js CDN load.

**Report sections:**

1. **Header** — JVM version, OS, generation timestamp
2. **Summary Table** — All 9 queries x all configurations, showing p50/p95/p99 latency and throughput. The fastest configuration per query is highlighted in green.
3. **Latency Bar Charts** — One grouped bar chart per query showing p50 (blue), p95 (yellow), and p99 (red) for each configuration
4. **Throughput Comparison** — Horizontal bar chart with all queries and all configurations side-by-side
5. **Performance Radar** — Radar chart showing normalized p50 performance profile per configuration (higher = faster)

All benchmark data is also embedded as a JavaScript constant (`const DATA = [...]`) for custom analysis.

---

## Getting Started

### Prerequisites

- Java 21+
- Maven 3.9+
- Docker and Docker Compose

### Infrastructure

Start the database containers:

```bash
cd docker
docker compose up -d
```

This launches:
- **MongoDB 8.2** on port `27017` (4 GB memory, WiredTiger cache 1.5 GB)
- **Oracle ADB-Free** on ports `1521` (JDBC), `8443` (ORDS REST), `27018` (MongoDB API)

Wait for Oracle to become healthy (~5 minutes on first start):

```bash
docker compose ps   # Check health status
```

### Build and Run

```bash
cd harness
mvn clean package -DskipTests
java -jar target/helix-benchmark-1.0.0-SNAPSHOT.jar [path/to/config.yaml]
```

Or run tests first:

```bash
mvn test    # 169 unit tests
```

### Configuration

Edit `harness/benchmark-config.yaml` to tune volumes, iterations, and connection strings:

```yaml
benchmark:
  warmUpIterations: 50          # Discarded warm-up runs per query
  measurementIterations: 200    # Measured runs per query
  batchSize: 1000               # MongoDB bulk insert batch size
  jdbcBatchSize: 500            # Oracle JDBC batch size

dataGeneration:
  advisorCount: 1000
  accountCount: 100000
  bookRoleGroupCount: 30000
  bookRoleInvestorCount: 150000
  advisoryContextPoolSize: 5000
  partyRoleIdPoolSize: 10000
  finInstIdPoolSize: 50
  targetSizeGb: 1.5

connections:
  mongoNative:
    uri: "mongodb://localhost:27017"
    database: "helix"
  oracleJdbc:
    url: "jdbc:oracle:thin:@localhost:1521/helix"
    username: "ADMIN"
    password: "Welcome_12345!"
    maxPoolSize: 10
  oracleMongoApi:
    uri: "mongodb://localhost:27018"
    database: "helix"
```

---

## Project Structure

```
helix/
├── docs/                                        # Reference schemas & queries
│   ├── Account.json                             # Account document sample
│   ├── Advisor.json                             # Advisor document sample
│   ├── BookRoleGroup.json                       # BookRoleGroup document sample
│   ├── BookRoleInvestor.json                    # BookRoleInvestor document sample
│   ├── SampleMongoQueries.txt                   # Original query patterns
│   └── erd.html                                 # Entity-Relationship Diagram
├── docker/
│   ├── docker-compose.yml                       # MongoDB 8.2 + Oracle ADB-Free
│   └── mongo-init/01-init.js                    # Database + collection init
└── harness/                                     # Java 21 Maven project
    ├── pom.xml
    ├── benchmark-config.yaml                    # Runtime configuration
    └── src/
        ├── main/java/com/helix/benchmark/
        │   ├── HelixBenchmarkMain.java          # Entry point & orchestrator
        │   ├── config/
        │   │   ├── BenchmarkConfig.java         # YAML config loader
        │   │   ├── DatabaseTarget.java          # MONGO_NATIVE | ORACLE_JDBC | ORACLE_MONGO_API
        │   │   └── SchemaModel.java             # EMBEDDED | NORMALIZED
        │   ├── connection/
        │   │   └── ConnectionManager.java       # Connection string / credential provider
        │   ├── schema/
        │   │   ├── SchemaManager.java           # Interface
        │   │   ├── MongoSchemaManager.java      # createIndex definitions (both models)
        │   │   └── OracleSchemaManager.java     # CREATE JSON COLLECTION TABLE + indexes
        │   ├── datagen/
        │   │   ├── ReferenceRegistry.java       # ID pools for referential integrity
        │   │   ├── TestDataGenerator.java       # Generates all 4 document types
        │   │   └── DataLoader.java              # Bulk insert (MongoDB + Oracle JDBC)
        │   ├── query/
        │   │   ├── QueryDefinition.java         # Enum: Q1-Q9 with metadata
        │   │   ├── QueryParameterGenerator.java # Random valid parameters per query
        │   │   ├── QueryExecutor.java           # Interface
        │   │   ├── MongoQueryExecutor.java      # Native MongoDB + Oracle MongoDB API
        │   │   └── OracleJdbcQueryExecutor.java # SQL/JSON via JDBC
        │   ├── benchmark/
        │   │   ├── BenchmarkRunner.java         # Warm-up + measurement loop
        │   │   ├── LatencyTracker.java          # Nanosecond timing, percentile math
        │   │   └── BenchmarkResult.java         # p50/p95/p99, throughput, metadata
        │   └── report/
        │       └── HtmlReportGenerator.java     # Standalone HTML with Chart.js
        ├── main/resources/
        │   └── logback.xml                      # Logging configuration
        └── test/java/com/helix/benchmark/       # 15 test classes, 169 tests
            ├── HelixBenchmarkMainTest.java
            ├── benchmark/
            │   ├── BenchmarkResultTest.java
            │   ├── BenchmarkRunnerTest.java
            │   └── LatencyTrackerTest.java
            ├── config/
            │   ├── BenchmarkConfigTest.java
            │   ├── DatabaseTargetTest.java
            │   └── SchemaModelTest.java
            ├── connection/
            │   └── ConnectionManagerTest.java
            ├── datagen/
            │   ├── DataLoaderTest.java
            │   ├── ReferenceRegistryTest.java
            │   └── TestDataGeneratorTest.java
            ├── query/
            │   ├── MongoQueryExecutorTest.java
            │   ├── OracleJdbcQueryExecutorTest.java
            │   ├── QueryDefinitionTest.java
            │   └── QueryParameterGeneratorTest.java
            ├── report/
            │   └── HtmlReportGeneratorTest.java
            └── schema/
                └── SchemaManagerTest.java
```

---

## Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Java | 21 | Runtime |
| MongoDB Java Driver | 5.3.1 | Native MongoDB + Oracle MongoDB API |
| Oracle JDBC (ojdbc17) | 23.7.0.25.01 | Oracle 26ai JSON operations |
| HikariCP | 6.2.1 | JDBC connection pooling |
| Datafaker | 2.4.2 | Realistic synthetic data |
| Jackson | 2.18.2 | YAML config + JSON serialization |
| Logback | 1.5.16 | Structured logging |
| Chart.js | 4.x (CDN) | HTML report visualization |
| JUnit 5 | 5.11.4 | Unit testing |
| Mockito | 5.15.2 | Test mocking |
| AssertJ | 3.27.3 | Fluent test assertions |
| MongoDB | 8.2 | Native document database |
| Oracle ADB-Free | Latest | Oracle 26ai with ORDS MongoDB API |
| Docker Compose | 3.8 | Container orchestration |
