# Treasury response schemas

Reviewed 2026-10-09 America/New_York (2026-10-10 UTC). Treasury has two distinct consumed interfaces: Fiscal Data JSON for Average Interest Rates, and daily-rate Atom/OData XML for five families. This reference describes reusable envelopes and dataset fields, not individual selected observations. Presence means **local parser acceptance** unless expressly attributed to upstream documentation.

Evidence: the [fixture manifest](../../../adapters/market-squawk-adapter-treasury/fixtures/manifest.json) classifies the Fiscal JSON as a captured official response and daily-rate fixtures as bounded official excerpts. They are historical examples, not fresh live acquisitions or complete page chains. Fiscal Data's official documentation returned HTTP 403 during this review; current tables are grounded in the parser and retained response.

## Fiscal Data request and envelope

GET `https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates`, with `fields`, `filter`, `sort`, `format=json`, one-based `page[number]` and `page[size]`. The adapter fixes the complete eleven-field projection below and deterministic `record_date,src_line_nbr` sort. Other Fiscal Data datasets have different row schemas and are not implemented by this profile.

| Root field | Wire type | Local presence | Meaning |
|---|---|---|---|
| `data` | AverageRateRow[] | Required | Page of dataset-specific rows |
| `meta` | FiscalMeta | Required | Row counts and logical schema dictionary |
| `links` | FiscalLinks | Required | Page-chain coordinates |

| FiscalMeta field | Wire type | Local presence | Meaning |
|---|---|---|---|
| `count` | integer number | Required | Rows in this page; must equal data length |
| `labels` | `{fieldName: string}` | Required | Human labels for every projected field |
| `dataTypes` | `{fieldName: string}` | Required | Logical field types; distinct from physical JSON string cells |
| `dataFormats` | `{fieldName: string}` | Required | Provider formatting hints for those logical types |
| `total-count` | integer number | Required | Matching rows across the complete selection |
| `total-pages` | integer number | Required, positive locally | Total pages for the selected page size |

Dictionary keys must agree with each other, the projection and every row. Unknown envelope/meta members, duplicate fields and mismatched row dictionaries are rejected.

| FiscalLinks field | Wire type | Local presence | Meaning |
|---|---|---|---|
| `self` | string | Required | Current one-based page and page-size query fragment |
| `first` | string | Required | First page coordinate |
| `prev` | string/null | Optional/null accepted | Prior page; no prior page at origin |
| `next` | string/null | Optional/null accepted | Next page; no continuation at terminal |
| `last` | string | Required | Final page coordinate |

Links in the retained response are query fragments beginning with `&`, not independent trusted URLs. The parser validates page number/size against the request, derives its own canonical next-page token and rejects inconsistent totals, schema or order across pages. Completion requires all pages and exact total rows, not merely a null link on an arbitrary page.

Sources: [Fiscal envelope/parser/tracker](../../../adapters/market-squawk-adapter-treasury/src/fiscal_data.rs), [query profile](../../../adapters/market-squawk-adapter-treasury/src/query.rs), official [API documentation](https://fiscaldata.treasury.gov/api-documentation/), [retained JSON](../../../adapters/market-squawk-adapter-treasury/fixtures/average_interest_rates.json).

## AverageRateRow

Every projected row field is a **JSON string**, including numeric/date fields and the literal missing token `"null"`. Actual JSON null is rejected by this field decoder. Logical metadata must not be mistaken for wire JSON typing.

| Field | Logical type/format | Local presence | Meaning |
|---|---|---|---|
| `record_date` | DATE / `YYYY-MM-DD` | Required | Dataset reporting date; not a release timestamp |
| `security_type_desc` | STRING / `String` | Required | Marketable/nonmarketable security grouping |
| `security_desc` | STRING / `String` | Required | Security category whose average rate is reported |
| `avg_interest_rate_amt` | PERCENTAGE / `10.2%` | Required | Average interest rate in percent units; exact decimal or `"null"` |
| `src_line_nbr` | INTEGER / `10` | Required, positive locally | Source row/line coordinate within reporting date |
| `record_fiscal_year` | YEAR / `YYYY` | Required projection | Fiscal reporting year |
| `record_fiscal_quarter` | QUARTER / `Q` | Required projection | Fiscal reporting quarter |
| `record_calendar_year` | YEAR / `YYYY` | Required projection | Calendar reporting year |
| `record_calendar_quarter` | QUARTER / `Q` | Required projection | Calendar reporting quarter |
| `record_calendar_month` | MONTH / `MM` | Required projection | Calendar month |
| `record_calendar_day` | DAY / `DD` | Required projection | Day of reporting date |

The first five type/format pairs are explicitly checked by the rate normalizer; the remaining dictionary values above come from the retained response, not individual hard-coded conversion checks. The natural key binds `record_date + security_type_desc + security_desc + src_line_nbr`. Format `10.2%` is metadata, not a two-decimal precision ceiling: captured `avg_interest_rate_amt` is the string `3.706` for `record_date` `2026-06-30`. That is historical public-response evidence, not a hypothetical or current quote.

Sources: [rate normalizer](../../../adapters/market-squawk-adapter-treasury/src/rates.rs), [captured field dictionary](../../../adapters/market-squawk-adapter-treasury/fixtures/average_interest_rates.json), official [dataset](https://fiscaldata.treasury.gov/datasets/average-interest-rates-treasury-securities/).

## Daily-rate XML requests

GET `https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml`. Query `data` chooses exactly one family:

| Family | `data` value | Observation property object |
|---|---|---|
| Nominal par curve | `daily_treasury_yield_curve` | NominalCurveProperties |
| Bill rates | `daily_treasury_bill_rates` | BillProperties |
| Long-term rates | `daily_treasury_long_term_rate` | LongTermProperties |
| Real par curve | `daily_treasury_real_yield_curve` | RealCurveProperties |
| Real long-term rates | `daily_treasury_real_long_term` | RealLongTermProperties |

Year uses `field_tdr_date_value=YYYY`; month uses `field_tdr_date_value_month=YYYYMM`; all-history uses `field_tdr_date_value=all&page=N`, beginning at 0. Only all-history uses continuation; official feed documentation describes an empty entry page as terminal, with a default 300 rows/page. The local tracker rejects repeated/skipped pages, duplicate rows/date violations and bounded truncation represented as completion.

Sources: [daily query definitions](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/query.rs), [pagination](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/pagination.rs), official [daily XML feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed).

## Atom Feed, Entry and OData property values

Namespaces: Atom `http://www.w3.org/2005/Atom`; `d` data `http://schemas.microsoft.com/ado/2007/08/dataservices`; `m` metadata `http://schemas.microsoft.com/ado/2007/08/dataservices/metadata`. Prefix spelling is not identity. Properties mount at `feed/entry/content/m:properties/d:{field}`.

| Object/path | Wire form | Local presence | Meaning |
|---|---|---|---|
| Feed `title` | Text element | Required, family title checked | Dataset title |
| Feed `id` | Text element | Required, family identity checked | Canonical feed URL |
| Feed `updated` | Timestamp text | Required | Feed update instant |
| Feed `entry` | Repeated Entry elements | Zero at all-history termination | Dated provider records |
| Entry `id` | Text element | Optional in supported date-identity cases | Provider record URL/ID; reconciled with property ID when both exist |
| Entry `updated` | Timestamp text | Required | Entry update instant; may not exceed feed update |
| Entry `content/m:properties` | Property elements | Required for each parsed entry | Family-specific native fields |
| Property `@m:type` | Attribute text | Required for typed nonnull fields locally | OData logical type: `Edm.Int32`, `Edm.DateTime`, `Edm.Double` |
| Property `@m:null` | Attribute `true` | Allowed for supported missing fields | Explicit provider missingness, without element text |

Updated values are RFC 3339 timestamps with uppercase `T`, a known zone and no whitespace; the local parser rejects lowercase `z` and unknown offset `-00:00`. These are distinct from observation civil dates. Typed dates are lexical `YYYY-MM-DDT00:00:00` without zone and normalize to a **date**, not a fabricated midnight publication instant. Rates are XML decimal text even with `Edm.Double`; the adapter parses exact decimal. Missing rate elements and `m:null=true` are separate retained markers. Present blank nonnull numeric elements are invalid, not zero.

Sources: [XML parser](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/parser.rs), [property schema](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/schema.rs), official [XML changes](https://home.treasury.gov/developer-notice-xml-changes).

## NominalCurveProperties and RealCurveProperties

Each named rate below is an optional/missing-capable `Edm.Double` text element locally, interpreted in percent per year. Fields not yet introduced in a historical schema do not become zero observations.

| Nominal field | Meaning |
|---|---|
| `Id` | Optional `Edm.Int32` source record ID |
| `NEW_DATE` | Required `Edm.DateTime` observation date |
| `BC_1MONTH` | 1-month nominal par yield |
| `BC_1_5MONTH` | 1.5-month nominal par yield |
| `BC_2MONTH` | 2-month nominal par yield |
| `BC_3MONTH` | 3-month nominal par yield |
| `BC_4MONTH` | 4-month nominal par yield |
| `BC_6MONTH` | 6-month nominal par yield |
| `BC_1YEAR`, `BC_2YEAR`, `BC_3YEAR` | 1-, 2-, 3-year nominal par yields |
| `BC_5YEAR`, `BC_7YEAR`, `BC_10YEAR` | 5-, 7-, 10-year nominal par yields |
| `BC_20YEAR`, `BC_30YEAR` | 20-, 30-year nominal par yields |
| `BC_30YEARDISPLAY` | Optional numeric display auxiliary; not interchangeable with `BC_30YEAR` |

| Real curve field | Meaning |
|---|---|
| `DailyTreasuryRealYieldCurveRateDataId` | Optional `Edm.Int32` source record ID |
| `NEW_DATE` | Required `Edm.DateTime` observation date |
| `TC_5YEAR`, `TC_7YEAR`, `TC_10YEAR` | 5-, 7-, 10-year real par yields |
| `TC_20YEAR`, `TC_30YEAR` | 20-, 30-year real par yields |

Source: [closed family vocabulary and date guards](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/schema.rs). Treasury's additive 2025 fields and older missing representations are described by [developer notice](https://home.treasury.gov/developer-notice-xml-changes).

## BillProperties

| Common field | XML text/type | Local presence | Meaning |
|---|---|---|---|
| `DailyTreasuryBillRateDataId` | `Edm.Int32` | Optional with supported date identity | Source record ID |
| `INDEX_DATE` | `Edm.DateTime` | Required | Observation date |
| `QUOTE_DATE` | `Edm.DateTime` | Required, must equal INDEX_DATE | Rate quotation date |
| `CF_NEW_DATE` | Untyped `MM/DD/YYYY` | Required, must agree with date | Provider's formatted date companion |
| `CF_WEEK` | `Edm.Int32` text `YYYYww` | Required, checked against ISO week | Provider week grouping |
| `BOND_MKT_UNAVAIL_REASON` | Untyped text | Optional | Explanation of unavailable market/rates |

The following templates describe the **exact named family** once. Substitute `W` from `4,6,8,13,17,26,52`; e.g. `ROUND_B1_CLOSE_4WK_2`. Each maturity's vocabulary contains these six possible members; historical rows may precede a field's introduction or omit missing-capable members.

| Exact field template | XML form | Local presence | Meaning |
|---|---|---|---|
| `ROUND_B1_CLOSE_{W}WK_2` | `Edm.Double` text | Missing-capable | Bill bank-discount rate, percent |
| `ROUND_B1_YIELD_{W}WK_2` | `Edm.Double` text | Missing-capable | Bill coupon-equivalent yield, percent |
| `CS_{W}WK_CLOSE_AVG` | `Edm.Double` text | Missing-capable | Published bank-discount average, percent |
| `CS_{W}WK_YIELD_AVG` | `Edm.Double` text | Missing-capable | Published coupon-equivalent average, percent |
| `MATURITY_DATE_{W}WK` | `Edm.DateTime` text | Optional; required when that maturity has observed rates | Bill maturity civil date |
| `CUSIP_{W}WK` | Untyped 9-character text | Optional; required when observed rates exist | Quoted bill identity, uppercase letters/digits locally |

Rate measures and bill identifiers are separate; no maturity, CUSIP or discount/coupon-equivalent conversion is inferred from a rate value. The six-week family is an additive schema change.

Source: [BillFieldSpec and validation](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/schema.rs), [historical bill excerpt](../../../adapters/market-squawk-adapter-treasury/fixtures/daily_bill_rates.xml).

## LongTermProperties and RealLongTermProperties

| Family | Field | XML form/local presence | Meaning |
|---|---|---|---|
| Long-term | `Id` | Optional `Edm.Int32`; numeric record identity required somewhere | Source record identity |
| Long-term | `QUOTE_DATE` | Required `Edm.DateTime` | Observation date |
| Long-term | `RATE_TYPE` | Required untyped text | `BC_20year`: 20-year constant maturity; `Over_10_Years`: over-10-year average; `Real_Rate`: real-rate category |
| Long-term | `RATE` | Missing-capable `Edm.Double` text | Percent rate for the selected RATE_TYPE |
| Long-term | `EXTRAPOLATION_FACTOR` | Required untyped decimal or `N/A` | Published curve extrapolation factor, or not applicable |
| Real long-term | `QUOTE_DATE` | Required `Edm.DateTime` | Observation date; supports date-derived local record identity |
| Real long-term | `RATE` | Missing-capable `Edm.Double` text | Real long-term average rate, percent |

Long-term may contain multiple entries for a date, distinguished by provider record identity/rate type. Other families enforce date uniqueness locally. Every family's property vocabulary is closed; unknown economic fields block parsing pending review.

Source: [family schema/identity](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/schema.rs), [entry validation](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/parser.rs).

## Errors and remaining evidence

Non-success statuses and malformed XML/JSON are acquisition failures; no stable complete structured HTTP-error body was verified for either interface. XML schema acceptance does not establish immutable historical vintages or first public availability: retained reacquisitions preserve locally observed revisions. Fiscal Data's common envelope does not establish auction/debt/fiscal row dictionaries; only the Average Interest Rates row schema above is consumed. Fresh complete chain examples, formal upstream per-field required/null guarantees and full family XSDs remain unverified.

Sources: [Fiscal HTTP client](../../../adapters/market-squawk-adapter-treasury/src/client.rs), [daily parser](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/parser.rs).
