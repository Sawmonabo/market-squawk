# SEC company/filing response projection parsed by this application

Contents: [GET routes and envelope shapes](#get-routes-and-envelope-shapes) · [CompanyFacts object](#companyfacts-object) · [CompanyFacts envelope](#companyfacts-envelope) · [Fact](#fact) · [Standard concept catalog](#standard-concept-catalog) · [Submissions issuer](#submissions-issuer) · [Recent filing arrays](#recent-filing-arrays) · [Historical companion descriptor](#historical-companion-descriptor) · [XBRL contexts, units and occurrences](#xbrl-contexts-units-and-occurrences) · [XBRL fact attributes](#xbrl-fact-attributes) · [Missing upstream schema evidence](#missing-upstream-schema-evidence)

[SEC API documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) describes submissions and XBRL JSON. Source routes: [contracts.rs](../../../adapters/market-squawk-adapter-sec/src/client/contracts.rs). Scope: current CompanyFacts, submissions and XML/Inline XBRL projections; this is not the complete SEC upstream response specification. These objects represent as-filed issuer facts and filing evidence, not market quotes or backend valuation outputs. No original SEC JSON/XML body was inspected for examples in this rewrite.

## GET routes and envelope shapes

| URL | Shape / continuation |
|---|---|
| `https://data.sec.gov/submissions/CIK{10-digit-cik}.json` | Issuer object, filings.recent parallel arrays, filings.files companion descriptors |
| `https://data.sec.gov/submissions/{companion-name}.json` | Historical filing arrays; follow each declared file |
| `https://data.sec.gov/api/xbrl/companyfacts/CIK{10-digit-cik}.json` | cik/entityName/facts taxonomy-concept-unit hierarchy |
| `https://www.sec.gov/Archives/edgar/data/{cik}/{accession-no-dashes}/{document}` | Filing document; XML XBRL or Inline XBRL HTML |
| `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip` | Bulk archive of submissions JSON |
| `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip` | Bulk archive of CompanyFacts JSON |

Companion discovery is not an offset cursor. A current file's recent arrays do not establish all historical filings. XML/HTML failures and HTTP refusal bodies must not be interpreted as facts; no universal JSON error schema is established.

## CompanyFacts object

`facts[taxonomy][concept]` is a dynamic concept object. `units[unit]` maps to Fact[]; every numeric concept is retained by generic parsing, not just the examples of standard concepts listed later. Taxonomy/concept names are open source keys.

## CompanyFacts envelope

Prefix `$`. cik is accepted as a JSON number or decimal string locally; entityName is text. The facts hierarchy has dynamic keys.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `cik` | number/string | SEC Central Index Key identifying the reporting entity |
| `entityName` | string | Reporting entity name |
| `facts[taxonomy][concept].units[unit]` | array<object> | Numeric fact occurrences grouped by taxonomy, concept and source unit |

Source: [company_facts.rs](../../../adapters/market-squawk-adapter-sec/src/json/company_facts.rs).

## Fact

Mount `$.facts[taxonomy][concept].units[unit][]`. val is a JSON number; start/end/filed are calendar-date strings. Instant facts omit a start; period facts carry start/end. Unit keys distinguish currency, shares and per-share values. Fiscal context, frame and accession must stay attached to each observation; a later amendment does not erase prior as-filed evidence.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `val` | number | Reported numeric value in the enclosing unit |
| `start` | string/null | Start of duration reporting period; absent for an instant fact |
| `end` | string | Instant date or end of duration reporting period |
| `accn` | string | Accession number of the filing supplying this fact |
| `form` | string | Filing form code, including amendment suffix when supplied |
| `filed` | string | SEC filing civil date, YYYY-MM-DD; not publication time |
| `frame` | string/null | SEC calendar alignment frame; optional classification separate from fiscal period |
| `fy` | unsigned integer/null (local allowance) | Reporting fiscal year as integer |
| `fp` | string/null | Reporting fiscal-period code; complete enum not enforced here |

Source: [company_facts.rs](../../../adapters/market-squawk-adapter-sec/src/json/company_facts.rs).

## Standard concept catalog

These 36 currently mapped standard concepts all use the same Fact shape. They are actual taxonomy/concept keys, not extra JSON properties. They do not exhaust CompanyFacts or define market capitalization, forward EPS or trailing P/E.

| Taxonomy | Concept key | Meaning |
|---|---|---|
| `us-gaap` | `CashAndCashEquivalentsAtCarryingValue` | Cash And Cash Equivalents |
| `us-gaap` | `AccountsReceivableNetCurrent` | Accounts Receivable Net Current |
| `us-gaap` | `InventoryNet` | Inventory Net |
| `us-gaap` | `AssetsCurrent` | Current Assets |
| `us-gaap` | `Assets` | Total Assets |
| `us-gaap` | `LiabilitiesCurrent` | Current Liabilities |
| `us-gaap` | `Liabilities` | Total Liabilities |
| `us-gaap` | `LongTermDebtCurrent` | Current Long Term Debt |
| `us-gaap` | `LongTermDebtNoncurrent` | Noncurrent Long Term Debt |
| `us-gaap` | `StockholdersEquity` | Shareholders Equity |
| `us-gaap` | `StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` | Total Equity Including Noncontrolling Interests |
| `us-gaap` | `Revenues` | Revenue |
| `us-gaap` | `SalesRevenueNet` | Net Sales |
| `us-gaap` | `RevenueFromContractWithCustomerExcludingAssessedTax` | Customer Revenue Excluding Assessed Tax |
| `us-gaap` | `CostOfRevenue` | Cost Of Revenue |
| `us-gaap` | `GrossProfit` | Gross Profit |
| `us-gaap` | `OperatingExpenses` | Operating Expenses |
| `us-gaap` | `OperatingIncomeLoss` | Operating Income |
| `us-gaap` | `NetIncomeLoss` | Net Income |
| `us-gaap` | `NetIncomeLossAvailableToCommonStockholdersBasic` | Common Net Income |
| `us-gaap` | `PreferredStockDividendsAndOtherAdjustments` | Preferred Dividends And Adjustments |
| `us-gaap` | `ProceedsFromIssuanceOfLongTermDebt` | Long Term Borrowing Proceeds |
| `us-gaap` | `RepaymentsOfLongTermDebt` | Long Term Debt Repayments |
| `us-gaap` | `PaymentsOfDividendsPreferredStockAndPreferenceStock` | Preferred Dividends Paid |
| `us-gaap` | `PreferredStockValue` | Preferred Stock Issued Value |
| `us-gaap` | `ProfitLoss` | Profit Or Loss Including Noncontrolling Interests |
| `us-gaap` | `EarningsPerShareBasic` | Basic Earnings Per Share |
| `us-gaap` | `EarningsPerShareDiluted` | Diluted Earnings Per Share |
| `us-gaap` | `NetCashProvidedByUsedInOperatingActivities` | Operating Cash Flow |
| `us-gaap` | `NetCashProvidedByUsedInInvestingActivities` | Investing Cash Flow |
| `us-gaap` | `NetCashProvidedByUsedInFinancingActivities` | Financing Cash Flow |
| `us-gaap` | `PaymentsToAcquirePropertyPlantAndEquipment` | Property Plant And Equipment Purchases |
| `dei` | `EntityCommonStockSharesOutstanding` | Entity Common Shares Outstanding |
| `us-gaap` | `CommonStockSharesOutstanding` | Common Stock Shares Outstanding |
| `us-gaap` | `WeightedAverageNumberOfSharesOutstandingBasic` | Weighted Average Basic Shares |
| `us-gaap` | `WeightedAverageNumberOfDilutedSharesOutstanding` | Weighted Average Diluted Shares |

Source: [company_product.rs](../../../apps/market-squawk/src/application/research/company_product.rs). The financial metric catalog maps these standard concepts; the generic API parser also retains other numeric concepts.

## Submissions issuer

Root metadata and formerNames[] retain issuer identity separately from parallel filing arrays. SIC is an industry classification, not an assumed sector taxonomy. Missing/null allowance below reflects parser behavior only.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `cik` | number/string | SEC Central Index Key of the reporting entity |
| `name` | string | Current reporting entity name |
| `entityType` | string/null | SEC issuer entity classification; complete enum unverified |
| `sic` | string/null | Standard Industrial Classification code as text |
| `sicDescription` | string/null | Display description of the SIC industry code |
| `tickers[]` | string | Each reported trading alias; source array paired with exchanges |
| `exchanges[]` | string | Each listing exchange associated by index with tickers |
| `formerNames[].name` | string | Former legal/entity name |
| `formerNames[].from` | string | Start of former-name interval; source date text |
| `formerNames[].to` | string | End of former-name interval; source date text |

Source: [submissions.rs](../../../adapters/market-squawk-adapter-sec/src/json/submissions.rs).

## Recent filing arrays

Mount `$.filings.recent`. Each index corresponds to one filing across accession/date/form/document/size/XBRL columns. Local parsing validates aligned counts; these are arrays, not an array of per-filing objects. acceptanceDateTime is a timestamp string; filingDate/reportDate are calendar dates. Size is bytes. XBRL indicators admit numeric/bool representation locally.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `accessionNumber[]` | string | SEC filing accession identifier |
| `filingDate[]` | string | Filing civil date, YYYY-MM-DD |
| `reportDate[]` | string | Reported fiscal/financial as-of date; empty text retained as unavailable |
| `acceptanceDateTime[]` | string | SEC acceptance time parsed as RFC 3339; separate from filing date |
| `form[]` | string | Filing form classification including amendment code |
| `primaryDocument[]` | string/null | Filing primary-document filename |
| `size[]` | number/null | Reported filing size in bytes |
| `isXBRL[]` | number/boolean/null | XBRL filing indicator; bool/0/1 allowance is local |
| `isInlineXBRL[]` | number/boolean/null | Inline XBRL filing indicator; bool/0/1 allowance is local |

Source: [submissions.rs](../../../adapters/market-squawk-adapter-sec/src/json/submissions.rs).

## Historical companion descriptor

Mount `$.filings.files[]`. File name, count and covered filing dates describe additional submissions objects, not a guaranteed cursor page.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `name` | string | Provider-declared companion JSON filename |
| `filingCount` | number | Declared count of filings in companion |
| `filingFrom` | string | Earliest declared filing civil date |
| `filingTo` | string | Latest declared filing civil date |

Source: [submissions.rs](../../../adapters/market-squawk-adapter-sec/src/json/submissions.rs).

## XBRL contexts, units and occurrences

XML paths use namespace aliases for readability; actual prefix spelling can differ. xbrli=`http://www.xbrl.org/2003/instance`; xbrldi=`http://xbrl.org/2006/xbrldi`; xsi=`http://www.w3.org/2001/XMLSchema-instance`; xml=`http://www.w3.org/XML/1998/namespace`. Inline namespace varies with version. xbrli is the XBRL instance namespace, xbrldi the dimensions namespace and ix the Inline XBRL namespace. Instance facts reference context/unit IDs; Inline facts may require scale, sign, format and continuations. This is a separate XML/HTML response family, not CompanyFacts JSON.

| Field | Source text encoding | Meaning |
|---|---|---|
| `xbrli:xbrl/xbrli:context/@id` | XML text | Context identifier referenced by facts |
| `xbrli:xbrl/xbrli:context/xbrli:entity/xbrli:identifier` | XML text | Reporting-entity identifier text |
| `xbrli:xbrl/xbrli:context/xbrli:entity/xbrli:identifier/@scheme` | XML text | URI identifying the entity-identifier scheme |
| `xbrli:xbrl/xbrli:context/xbrli:period/xbrli:instant` | XML text | Instant observation date in context period |
| `xbrli:xbrl/xbrli:context/xbrli:period/xbrli:startDate` | XML text | Duration context start date |
| `xbrli:xbrl/xbrli:context/xbrli:period/xbrli:endDate` | XML text | Duration context end date |
| `xbrli:context/xbrli:entity/xbrli:segment/xbrldi:explicitMember or xbrli:context/xbrli:scenario/xbrldi:explicitMember` | XML text | Dimension member QName, with dimension attribute identifying the axis |
| `xbrli:context/xbrli:entity/xbrli:segment/xbrldi:typedMember or xbrli:context/xbrli:scenario/xbrldi:typedMember` | XML text | Typed dimension content, with dimension attribute identifying the axis |
| `xbrli:xbrl/xbrli:unit/xbrli:measure` | XML text | Unit measure QName; compound units can use divide/numerator/denominator |
| `xbrli:xbrl/[taxonomy:concept]/text()` | XML text | Instance fact lexical value before unit/accuracy interpretation |
| `ix:nonFraction/text()` | XML text | Inline numeric fact source text before transformation/scale/sign/continuation |
| `ix:nonNumeric/text()` | XML text | Inline nonnumeric fact source text before continuation/escaping interpretation |

Source: [xbrl.rs](../../../adapters/market-squawk-adapter-sec/src/xbrl.rs).

## XBRL fact attributes

Attribute presence depends on numeric/non-numeric/nil and instance/Inline form. unitRef does not apply to every text fact; xml:lang and xsi:nil are namespace-qualified. The table names fields already audited; it is not a complete XBRL schema specification.

| Field | XML attribute encoding | Meaning |
|---|---|---|
| `[numeric-or-nonnumeric fact]/@contextRef` | XML string | Context ID linking issuer, period and dimensions |
| `[numeric-or-nonnumeric fact]/@unitRef` | XML string | Unit ID for numeric facts; not a numeric value itself |
| `[numeric-or-nonnumeric fact]/@name` | XML string | Inline fact taxonomy/concept QName; instance fact uses element QName |
| `[numeric-or-nonnumeric fact]/@id` | XML string | Fact/Inline node identifier |
| `[numeric-or-nonnumeric fact]/@decimals` | XML string | Declared numeric decimal accuracy; integer or INF lexical form |
| `[numeric-or-nonnumeric fact]/@precision` | XML string | Declared significant-digit accuracy; positive integer or INF |
| `[numeric-or-nonnumeric fact]/@scale` | XML string | Inline base-10 exponent applied to transformed numeric value |
| `[numeric-or-nonnumeric fact]/@sign` | XML string | Inline sign override; local negative-sign handling |
| `[numeric-or-nonnumeric fact]/@format` | XML string | QName identifying Inline transformation rule |
| `[numeric-or-nonnumeric fact]/@continuedAt` | XML string | ID of next Inline continuation node |
| `[numeric-or-nonnumeric fact]/@xsi:nil` | XML string | Namespace-qualified nil indicator, distinct from empty text |
| `[numeric-or-nonnumeric fact]/@xml:lang` | XML string | Namespace-qualified language tag on textual content |

Source: [wire.rs](../../../adapters/market-squawk-adapter-sec/src/xbrl/wire.rs).

## Missing upstream schema evidence

Concept-level label/description are upstream metadata ignored by this numeric projection; this table must not be read as the full CompanyFacts object schema. Their provider presence/null rules are not verified here. Additional submissions issuer/address and filing columns remain outside the audited typed subset. No complete issuer profile, website/sector, all dimensional financial statements or original XML examples are guaranteed by these tables. For fund holdings and annual reports see [SEC fund archives](sec-funds.md).
