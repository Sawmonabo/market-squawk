# SEC fund archive response projection parsed by this application

Contents: [GET objects](#get-objects) · [Archive relationships and value encoding](#archive-relationships-and-value-encoding) · [N-PORT FUND_REPORTED_INFO](#n-port-fund_reported_info) · [N-PORT FUND_REPORTED_HOLDING](#n-port-fund_reported_holding) · [N-PORT IDENTIFIERS](#n-port-identifiers) · [N-CEN FUND_REPORTED_INFO](#n-cen-fund_reported_info) · [N-CEN ETF](#n-cen-etf) · [N-CEN SECURITY_EXCHANGE](#n-cen-security_exchange) · [N-PORT SUBMISSION](#n-port-submission) · [N-CEN SUBMISSION](#n-cen-submission) · [N-PORT REGISTRANT](#n-port-registrant) · [N-CEN REGISTRANT](#n-cen-registrant) · [Metadata-defined table families](#metadata-defined-table-families) · [Missing schema evidence](#missing-schema-evidence)

Scope: the currently typed archive projection and closed table catalog; noncore column names remain metadata-dependent. This is not a complete current SEC table specification.

These are ZIP archives of UTF-8 TSV tables plus tabular metadata, not a JSON fund quote API. [N-PORT downloads](https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets), [N-PORT table specification](https://www.sec.gov/files/nport_readme.pdf), [N-CEN table specification](https://www.sec.gov/files/ncen_readme.pdf).

## GET objects

| URL pattern | Object |
|---|---|
| `https://www.sec.gov/files/dera/data/form-n-port-data-sets/{year}q{quarter}_nport.zip` | N-PORT archive |
| `https://www.sec.gov/files/dera/data/form-n-cen-data-sets/{year}q{quarter}_ncen.zip` | N-CEN archive |
| `https://www.sec.gov/files/nport_readme.pdf` | Official N-PORT table specification |
| `https://www.sec.gov/files/ncen_readme.pdf` | Official N-CEN table specification |

Source: [contracts.rs](../../../adapters/market-squawk-adapter-sec/src/client/contracts.rs). Quarter selection is not cursor pagination. The paired archive metadata/readme defines version-specific layout. A file's posting quarter, filing date and portfolio/report date are distinct.

## Archive relationships and value encoding

SUBMISSION supplies accession/report clocks. N-PORT fund rows join by ACCESSION_NUMBER; holding rows add HOLDING_ID and relate to identifiers, debt/derivative and lending tables. N-CEN fund rows also require FUND_ID for fund-scoped joins. A registrant, series, fund and share class are different identities; a text ticker is not a join key for every table.

All wire cells are text. Empty cells may express missing data according to that column's metadata; literal JSON null is not a TSV value. Source numeric lexemes retain exact decimal representation. The official PDF declares column data type, nullable flag and key membership; local Option fields alone do not prove upstream nullability. In the primary N-PORT readme ACCESSION_NUMBER is nonnullable in core tables; amounts may be nullable. Dates are validated as DD-MON-YYYY against metadata; Y/N flags map to booleans, and identifier strings retain leading zeros. N-PORT holding UNIT identifies the balance unit; currency denomination and U.S.-dollar value are different columns. [Form N-PORT instructions, B.1/C.2 and general instructions](https://www.sec.gov/files/formn-port.pdf) establish U.S.-dollar values and percent rather than fraction units.

**Evidence:** no original quarterly archive body was re-inspected for examples. The current code validates 77 named core columns and retains all metadata-declared columns for 83 closed table names. Their exact per-version noncore column headers/types remain a documented missing schema, not guessed property paths.
Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs).

## N-PORT FUND_REPORTED_INFO

Each row is a record in `FUND_REPORTED_INFO.tsv`. The table lists every currently audited typed source column. The type column is the lexical/semantic contract enforced against archive metadata; transport encoding is TSV text. Upstream nullable flags require the exact generation metadata.

| Field | Metadata/local lexical type | Meaning |
|---|---|---|
| `ACCESSION_NUMBER` | identifier/code string | SEC filing accession identifier (20-character hyphenated form) |
| `SERIES_NAME` | string | Portfolio series name |
| `SERIES_ID` | identifier/code string | SEC portfolio-series identifier |
| `SERIES_LEI` | identifier/code string | Legal Entity Identifier of the portfolio series |
| `TOTAL_ASSETS` | exact decimal; metadata NUMBER | Fund/consolidated total assets in U.S. dollars |
| `TOTAL_LIABILITIES` | exact decimal; metadata NUMBER | Fund/consolidated liabilities in U.S. dollars |
| `NET_ASSETS` | exact decimal; metadata NUMBER | Fund/consolidated net assets in U.S. dollars |

Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs), [fund_publication.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/fund_publication.rs).

## N-PORT FUND_REPORTED_HOLDING

Each row is a record in `FUND_REPORTED_HOLDING.tsv`. The table lists every currently audited typed source column. The type column is the lexical/semantic contract enforced against archive metadata; transport encoding is TSV text. Upstream nullable flags require the exact generation metadata.

| Field | Metadata/local lexical type | Meaning |
|---|---|---|
| `ACCESSION_NUMBER` | identifier/code string | SEC filing accession identifier (20-character hyphenated form) |
| `HOLDING_ID` | integer identifier; metadata NUMBER(scale 0) | Holding identifier |
| `ISSUER_NAME` | string | Legal/display name of the investment issuer |
| `ISSUER_LEI` | identifier/code string | Legal Entity Identifier of holding issuer |
| `ISSUER_TITLE` | string | Issue title |
| `ISSUER_CUSIP` | identifier/code string | CUSIP identifier of the held security |
| `BALANCE` | exact decimal; metadata NUMBER | Position balance; UNIT distinguishes share/principal/other quantity |
| `UNIT` | identifier/code string | Quantity unit |
| `OTHER_UNIT_DESC` | string | Other-unit description |
| `CURRENCY_CODE` | identifier/code string | ISO currency code in which the holding is denominated |
| `CURRENCY_VALUE` | exact decimal; metadata NUMBER | Reported holding value in U.S. dollars; denomination currency is separate |
| `EXCHANGE_RATE` | exact decimal; metadata NUMBER | Exchange rate used to express holding value in U.S. dollars; direction not independently specified here |
| `PERCENTAGE` | exact decimal; metadata NUMBER | Holding value as a percentage of fund net assets; percent units |
| `PAYOFF_PROFILE` | identifier/code string | Long/short/N/A payoff classification |
| `ASSET_CAT` | identifier/code string | Asset category |
| `OTHER_ASSET` | string | Other asset description |
| `ISSUER_TYPE` | identifier/code string | Issuer category |
| `OTHER_ISSUER` | string | Other issuer category |
| `INVESTMENT_COUNTRY` | identifier/code string | ISO country code of issuer organization |
| `IS_RESTRICTED_SECURITY` | Y/N flag; metadata one-character string | Restricted-security flag |
| `FAIR_VALUE_LEVEL` | identifier/code string | U.S. GAAP fair-value hierarchy level |
| `DERIVATIVE_CAT` | identifier/code string | Derivative category |

Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs), [fund_publication.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/fund_publication.rs).

## N-PORT IDENTIFIERS

Each row is a record in `IDENTIFIERS.tsv`. The table lists every currently audited typed source column. The type column is the lexical/semantic contract enforced against archive metadata; transport encoding is TSV text. Upstream nullable flags require the exact generation metadata.

| Field | Metadata/local lexical type | Meaning |
|---|---|---|
| `HOLDING_ID` | integer identifier; metadata NUMBER(scale 0) | Holding identity join |
| `IDENTIFIERS_ID` | integer identifier; metadata NUMBER(scale 0) | Identifier-row key |
| `IDENTIFIER_ISIN` | identifier/code string | International Securities Identification Number |
| `IDENTIFIER_TICKER` | identifier/code string | Ticker association, not authority alone |
| `OTHER_IDENTIFIER` | string | Supplementary source security identifier value |
| `OTHER_IDENTIFIER_DESC` | string | Identifier type |

Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs), [fund_publication.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/fund_publication.rs).

## N-CEN FUND_REPORTED_INFO

Each row is a record in `FUND_REPORTED_INFO.tsv`. The table lists every currently audited typed source column. The type column is the lexical/semantic contract enforced against archive metadata; transport encoding is TSV text. Upstream nullable flags require the exact generation metadata.

| Field | Metadata/local lexical type | Meaning |
|---|---|---|
| `FUND_ID` | identifier/code string | Exact accession/registrant/series compound source coordinate |
| `ACCESSION_NUMBER` | identifier/code string | SEC filing accession identifier (20-character hyphenated form) |
| `FUND_NAME` | string | Fund display name in this annual report |
| `SERIES_ID` | identifier/code string | Portfolio series ID |
| `LEI` | identifier/code string | Legal Entity Identifier of registrant/fund in this table |
| `IS_ETF` | Y/N flag; metadata one-character string | ETF flag |
| `IS_INDEX` | Y/N flag; metadata one-character string | Index-fund flag |
| `MONTHLY_AVG_NET_ASSETS` | exact decimal; metadata NUMBER | Reporting-period monthly average net assets for funds other than money market funds |
| `DAILY_AVG_NET_ASSETS` | exact decimal; metadata NUMBER | Reporting-period daily average net assets for money market funds |

Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs), [fund_publication.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/fund_publication.rs).

## N-CEN ETF

Each row is a record in `ETF.tsv`. The table lists every currently audited typed source column. The type column is the lexical/semantic contract enforced against archive metadata; transport encoding is TSV text. Upstream nullable flags require the exact generation metadata.

| Field | Metadata/local lexical type | Meaning |
|---|---|---|
| `FUND_ID` | identifier/code string | Fund join key |
| `FUND_NAME` | string | Fund display name in this annual report |
| `SERIES_ID` | identifier/code string | SEC identifier of portfolio series |
| `IS_COLLATERAL_REQUIRED` | Y/N flag; metadata one-character string | Collateral requirement |
| `NUM_SHARES_PER_CREATION_UNIT` | exact decimal; metadata NUMBER | Number of fund shares forming a creation unit at period end |
| `REDEEMED_SHARES_PER_CREATION_UNIT` | exact decimal; metadata NUMBER | Redemption-unit share quantity admitted locally; absent from reviewed readme |
| `IS_FUND_IN_KIND_ETF` | Y/N flag; metadata one-character string | In-kind ETF flag |

Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs), [fund_publication.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/fund_publication.rs).

## N-CEN SECURITY_EXCHANGE

Each row is a record in `SECURITY_EXCHANGE.tsv`. The table lists every currently audited typed source column. The type column is the lexical/semantic contract enforced against archive metadata; transport encoding is TSV text. Upstream nullable flags require the exact generation metadata.

| Field | Metadata/local lexical type | Meaning |
|---|---|---|
| `FUND_ID` | identifier/code string | Fund join key |
| `FUND_EXCHANGE` | identifier/code string | Reported exchange |
| `FUND_TICKER_SYMBOL` | identifier/code string | Fund ticker association |

Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs), [fund_publication.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/fund_publication.rs).

## N-PORT SUBMISSION

Each row is a record in `SUBMISSION.tsv`. The table lists every currently audited typed source column. The type column is the lexical/semantic contract enforced against archive metadata; transport encoding is TSV text. Upstream nullable flags require the exact generation metadata.

| Field | Metadata/local lexical type | Meaning |
|---|---|---|
| `ACCESSION_NUMBER` | identifier/code string | SEC filing accession identifier (20-character hyphenated form) |
| `FILING_DATE` | civil date; metadata DD-MON-YYYY | Filed civil date |
| `SUB_TYPE` | identifier/code string | Exact filing form |
| `REPORT_ENDING_PERIOD` | civil date; metadata DD-MON-YYYY | Fiscal year-end |
| `REPORT_DATE` | civil date; metadata DD-MON-YYYY | Portfolio as-of date |
| `IS_LAST_FILING` | Y/N flag; metadata one-character string | Final filing flag |

Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs).

## N-CEN SUBMISSION

Each row is a record in `SUBMISSION.tsv`. The table lists every currently audited typed source column. The type column is the lexical/semantic contract enforced against archive metadata; transport encoding is TSV text. Upstream nullable flags require the exact generation metadata.

| Field | Metadata/local lexical type | Meaning |
|---|---|---|
| `ACCESSION_NUMBER` | identifier/code string | SEC filing accession identifier (20-character hyphenated form) |
| `SUBMISSION_TYPE` | identifier/code string | Filing form |
| `CIK` | identifier/code string | SEC Central Index Key of registrant |
| `FILING_DATE` | civil date; metadata DD-MON-YYYY | Filed date |
| `REPORT_ENDING_PERIOD` | civil date; metadata DD-MON-YYYY | Annual ending date |
| `IS_REPORT_PERIOD_LT_12MONTH` | Y/N flag; metadata one-character string | Reporting period under twelve months |

Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs).

## N-PORT REGISTRANT

Each row is a record in `REGISTRANT.tsv`. The table lists every currently audited typed source column. The type column is the lexical/semantic contract enforced against archive metadata; transport encoding is TSV text. Upstream nullable flags require the exact generation metadata.

| Field | Metadata/local lexical type | Meaning |
|---|---|---|
| `ACCESSION_NUMBER` | identifier/code string | SEC filing accession identifier (20-character hyphenated form) |
| `CIK` | identifier/code string | SEC Central Index Key of registrant |
| `REGISTRANT_NAME` | string | Registrant name, not selected class authority |
| `LEI` | identifier/code string | Legal Entity Identifier of registrant/fund in this table |

Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs).

## N-CEN REGISTRANT

Each row is a record in `REGISTRANT.tsv`. The table lists every currently audited typed source column. The type column is the lexical/semantic contract enforced against archive metadata; transport encoding is TSV text. Upstream nullable flags require the exact generation metadata.

| Field | Metadata/local lexical type | Meaning |
|---|---|---|
| `ACCESSION_NUMBER` | identifier/code string | SEC filing accession identifier (20-character hyphenated form) |
| `CIK` | identifier/code string | SEC Central Index Key of registrant |
| `REGISTRANT_NAME` | string | Reporting investment company name |
| `FILE_NUM` | identifier/code string | Investment Company Act file number |
| `LEI` | identifier/code string | Legal Entity Identifier of registrant/fund in this table |
| `INVESTMENT_COMPANY_TYPE` | identifier/code string | Company legal/organization type |
| `TOTAL_SERIES` | nonnegative integer; metadata NUMBER(scale 0) | Number of portfolio series of the registrant |

Source: [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs).

### Primary declarations versus local projection

The reviewed N-PORT readme declares TOTAL_ASSETS/TOTAL_LIABILITIES/NET_ASSETS and BALANCE/CURRENCY_VALUE/EXCHANGE_RATE/PERCENTAGE as NUMBER(36,12); HOLDING_ID/IDENTIFIERS_ID as NUMBER(38); dates as DATE; flags as CHAR(1). The N-CEN readme declares monthly/daily net assets and creation-unit shares as NUMBER(22). Source numeric precision/scale must come from the exact archive metadata, not a language float default. Reviewed key identifiers ACCESSION_NUMBER/FUND_ID/HOLDING_ID/IDENTIFIERS_ID are nonnullable where designated as keys; other reviewed core columns are nullable. A parser-required identifier can therefore be stricter than the upstream projection.

REDEEMED_SHARES_PER_CREATION_UNIT is present in the local typed contract but absent from the reviewed N-CEN PDF. Its upstream declaration/nullability remains unverified. The PDF and archive generation must be reconciled before treating this field as a guaranteed response column.
Sources: [N-PORT readme, tables5.1–5.3/5.10–5.11](https://www.sec.gov/files/nport_readme.pdf), [N-CEN readme, table5.44](https://www.sec.gov/files/ncen_readme.pdf), [validate_typed_contract](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs).

## Metadata-defined table families

Every column declared by the archive metadata is retained under its exact source header. `{columnName}` is a metadata-dependent placeholder, not a verified field name. The remaining full column schema must be read from that exact archive generation and official readme.

| Form | ZIP table member | Column schema evidence |
|---|---|---|
| N-PORT | `SUBMISSION.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `REGISTRANT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `FUND_REPORTED_INFO.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `INTEREST_RATE_RISK.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `BORROWER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `BORROW_AGGREGATE.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `MONTHLY_TOTAL_RETURN.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `MONTHLY_RETURN_CAT_INSTRUMENT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `FUND_VAR_INFO.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `FUND_REPORTED_HOLDING.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `IDENTIFIERS.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `DEBT_SECURITY.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `DEBT_SECURITY_REF_INSTRUMENT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `CONVERTIBLE_SECURITY_CURRENCY.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `REPURCHASE_AGREEMENT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `REPURCHASE_COUNTERPARTY.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `REPURCHASE_COLLATERAL.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `DERIVATIVE_COUNTERPARTY.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `SWAPTION_OPTION_WARNT_DERIV.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `DESC_REF_INDEX_BASKET.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `DESC_REF_INDEX_COMPONENT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `DESC_REF_OTHER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `FUT_FWD_NONFOREIGNCUR_CONTRACT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `FWD_FOREIGNCUR_CONTRACT_SWAP.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `NONFOREIGN_EXCHANGE_SWAP.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `FLOATING_RATE_RESET_TENOR.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `OTHER_DERIV.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `OTHER_DERIV_NOTIONAL_AMOUNT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `SECURITIES_LENDING.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-PORT | `EXPLANATORY_NOTE.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `SUBMISSION.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `REGISTRANT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `REGISTRANT_WEBSITE.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `LOCATION_BOOKS_RECORD.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `TERMINATED_ORGANIZATION.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `DIRECTOR.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `DIRECTOR_FILE_NUMBER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `CHIEF_COMPLIANCE_OFFICER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `CCO_EMPLOYER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `REGISTRANT_REPORTING_SERIES.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `RELEASE_NUMBER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `PRINCIPAL_UNDERWRITER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `PUBLIC_ACCOUNTANT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `VALUATION_METHOD_CHANGE.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `VALUATION_METHOD_CHANGE_SERIES.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `FUND_REPORTED_INFO.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `SHARES_OUTSTANDING.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `FEEDER_FUNDS.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `MASTER_FUNDS.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `FOREIGN_INVESTMENT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `SECURITY_LENDING.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `SEC_LENDING_IDEMNITY_PROVIDER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `COLLATERAL_MANAGER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `ADVISER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `TRANSFER_AGENT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `PRICING_SERVICE.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `CUSTODIAN.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `SHAREHOLDER_SERVICING_AGENT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `ADMIN.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `BROKER_DEALER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `BROKER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `PRINCIPAL_TRANSACTION.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `LINE_OF_CREDIT_DETAIL.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `LINE_OF_CREDIT_INSTITUTION.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `CREDIT_USER.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `INTER_FUND_LENDING_DETAIL.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `INTER_FUND_BORROWING_DETAIL.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `SECURITY_RELATED_ITEM.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `RIGHTS_OFFERING_FUND.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `LONGTERM_DEBT_DEFAULT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `DIVIDENDS_IN_ARREAR.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `SECURITY_EXCHANGE.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `AUTHORIZED_PARTICIPANT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `ETF.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `DEPOSITOR.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `UIT_ADMIN.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `UIT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `SERIES_CIK.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `SPONSOR.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `TRUSTEE.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `CONTRACT_SECURITY.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `DIVESTMENT.tsv` | Metadata-declared columns; no original header/type example inspected |
| N-CEN | `REGISTRANT_HELDS_SECURITY.tsv` | Metadata-declared columns; no original header/type example inspected |

Source: [model.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/model.rs), [archive.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/archive.rs), [fund_publication.rs](../../../adapters/market-squawk-adapter-sec/src/bulk/fund_publication.rs).

## Missing schema evidence

The 77 typed core columns are complete for the current core parser; they are not the complete fields of every archive table. Exact metadata headers, null flags, enum values and units for all noncore columns need generation-specific inspection. N-CEN portfolio fees, turnover and identifiers cannot be fabricated from an N-PORT holding percentage. Concentration/overlap/sector totals are derived internal outputs, documented separately in [internal contracts](internal-contracts.md).
