# Nasdaq Trader symbol-directory schemas

Contents: [GET files and framing](#get-files-and-framing) · [nasdaqlisted.txt](#nasdaqlistedtxt) · [otherlisted.txt](#otherlistedtxt) · [Local enum and numeric interpretation](#local-enum-and-numeric-interpretation) · [Missing schema evidence](#missing-schema-evidence)

[Official directory definitions](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs). These are complete text files, not REST JSON objects or streaming quotes. No original rows are presented as fresh examples in this reference.

## GET files and framing

| URL | Layout |
|---|---|
| `https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt` | Header, pipe-delimited rows, terminal File Creation Time control row |
| `https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt` | Different header/rows, terminal creation control row |

Each file has its own publication time; there is no cursor. A control row is not a listing record. File creation text uses MMDDYYYYHH:MM; the parser retains this as reference publication context, without inventing an exchange event timestamp or timezone. All wire cells are text; local typed enums are listed below. Non-success/HTML responses are not directory records.
Source: [parser.rs](../../../adapters/market-squawk-adapter-nasdaq-symbols/src/parser.rs).

## nasdaqlisted.txt

Exact source column labels, in header order. Blank values and alternate aliases must retain their source meaning; local strict parser validation does not prove all future enum values are closed.

| Field | Wire cell type | Meaning |
|---|---|---|
| `Symbol` | pipe-delimited string | Nasdaq listing trading alias |
| `Security Name` | pipe-delimited string | Issuer/security display name |
| `Market Category` | enum code text | Nasdaq listing tier, Q/G/S |
| `Test Issue` | Y/N flag text | Y/N flag identifying a testing issue |
| `Financial Status` | enum code text | Listing compliance/bankruptcy classification; dictionary below |
| `Round Lot Size` | unsigned integer text | Number of shares in a round lot; unsigned integer text |
| `ETF` | Y/N flag text | Y/N flag identifying an exchange-traded fund |
| `NextShares` | Y/N flag text | Y/N flag identifying a NextShares product |

Source: [parser.rs](../../../adapters/market-squawk-adapter-nasdaq-symbols/src/parser.rs).

## otherlisted.txt

Exact source column labels, in header order. Blank values and alternate aliases must retain their source meaning; local strict parser validation does not prove all future enum values are closed.

| Field | Wire cell type | Meaning |
|---|---|---|
| `ACT Symbol` | pipe-delimited string | Alias used in ACT reporting |
| `Security Name` | pipe-delimited string | Issuer/security display name |
| `Exchange` | enum code text | Listing venue code; dictionary below |
| `CQS Symbol` | pipe-delimited string | Alias used by the Consolidated Quotation System |
| `ETF` | Y/N flag text | Y/N flag identifying an exchange-traded fund |
| `Round Lot Size` | unsigned integer text | Number of shares in a round lot; unsigned integer text |
| `Test Issue` | Y/N flag text | Y/N flag identifying a testing issue |
| `NASDAQ Symbol` | pipe-delimited string | Nasdaq-system alias for the externally listed security |

Source: [parser.rs](../../../adapters/market-squawk-adapter-nasdaq-symbols/src/parser.rs).

## Local enum and numeric interpretation

| Column | Current parser values |
|---|---|
| Market Category | Q Global Select; G Global Market; S Capital Market |
| Test Issue / ETF / NextShares | Y/N indicators |
| Financial Status | N normal; D deficient; E delinquent; Q bankrupt; G deficient+bankrupt; H deficient+delinquent; J delinquent+bankrupt; K all three |
| Exchange in otherlisted | A NYSE American; N NYSE; P NYSE Arca; M NYSE Texas; F Texas Stock Exchange; Z Cboe BZX; V IEX |
| Round Lot Size | Decimal text parsed to unsigned share count |

These mappings are grounded in current source; the official definitions page can change independently. Symbols across ACT/CQS/Nasdaq columns are distinct aliases. ETF/NextShares flags are listing context, not complete fund identity or NAV.

## Missing schema evidence

Historical membership, a complete security lifecycle and stable error-body formats are not established. Options/bond directories have no audited source-field contract in this reference; they must not be inferred from the equity file.
