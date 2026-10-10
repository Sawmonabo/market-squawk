# IEX HIST catalog, download and binary schemas

Reviewed **2026-10-10** against the current working-tree catalog/downloader/decoder. HIST is a selected on-demand T+1 cold lane, with exact feed/date/version and byte admission. It supplies IEX venue evidence, not live data, consolidated history or a JSON quote API. No catalog/API/download request, runtime action or credentials were used in this review.

Sources: [selected architecture](../../architecture/market-data-provider-architecture.md), [catalog](../../../adapters/market-squawk-adapter-iex-hist/src/catalog.rs), [download/materialization transport](../../../adapters/market-squawk-adapter-iex-hist/src/transport.rs), [binary decoder](../../../adapters/market-squawk-adapter-iex-hist/src/decode.rs), [version/value models](../../../adapters/market-squawk-adapter-iex-hist/src/model.rs). Older selected-provider prose predates these implementations and is not evidence that the current adapter is absent.

## Catalog envelope

`GET https://iextrading.com/api/1.0/hist` is the code-owned discovery route. Its established shape comes from retained catalog evidence and the current parser; no stable, fully documented upstream API schema or pagination contract is claimed. Official [HIST format/procurement description](https://iextrading.com/trading/alerts/2017/014/) identifies versioned PCAP files, rather than JSON financial observations.

Body: object with dynamic date keys: `{<YYYYMMDD>: array<Descriptor>}`. No `data`, `results`, `next` or count wrapper exists in the admitted parser.

| Descriptor field | Admitted wire type | Meaning |
|---|---|---|
| `link` | string | Exact returned immutable-generation download URL |
| `date` | string, `YYYYMMDD` | Trade date, must match enclosing key |
| `feed` | string | `TOPS`, `DEEP`, `DPLS` or `DPLC` |
| `version` | string | Exact catalog feed-version label |
| `protocol` | string | `IEXTP1` |
| `size` | decimal integer string | Advertised provider-object byte count, not expanded PCAP size |

All six members are required/non-null by the local closed descriptor parser. Duplicate date keys, malformed dates, duplicate descriptor identity, unrecognized fields/versions or inconsistent filename/link reject a catalog. Bounds (4 MiB body, 5,000 dates, 10,000 descriptors, eight/date) are local parser policy, not upstream capacity/retention guarantees. The envelope is a whole bounded generation; iteration through dates is not server pagination.

## Exact file families

Source: [catalog family validation](../../../adapters/market-squawk-adapter-iex-hist/src/catalog.rs), [decoder-version mapping](../../../adapters/market-squawk-adapter-iex-hist/src/model.rs), official [TOPS 1.66](https://storage.googleapis.com/assets-bucket/exchange/assets/IEX%20TOPS%20Specification%20v1.66.pdf), [IEX specification directory](https://www.iex.io/resources/equities/trading/documents).

| Feed / catalog version | Admitted object suffix | Current binary interpretation |
|---|---|---|
| `TOPS` / `1.6` | `<date>_IEXTP1_TOPS1.6.pcap.gz` | TOPS-1.66 stable message prefixes |
| `DEEP` / `1.0` | `<date>_IEXTP1_DEEP1.0.pcap.gz` | DEEP-1.08 stable prefixes |
| `DPLS` / `1.0` | `<date>_IEXTP1_DPLS1.0.pcap.gz` | DEEP+-1.04 order messages |
| `DPLC` / `1` | `<date>_IEXTP1_DPLC1.0.pcap` | DEEP+-1.04; uncompressed object despite catalog version `1` |
| `TOPS` / `1.5` | Catalog can recognize it | Not represented by the current `FeedVersion` decode contract |

Catalog versions are not exact specification revision strings. The decoder binds IEX-TP-1.26 independently of feed version. DPLS/DPLC are exact discovered catalog families; do not invent a `DEEP+` JSON feed label or substitute one family's channel topology for another.

## Download response and resumability

The selected descriptor URL must be HTTPS `www.googleapis.com`, under `/download/storage/v1/b/iex/o/data%2Ffeeds%2F<date>%2F<filename>`, with exactly `generation=<positive decimal>` and `alt=media`. It is validated provider-returned evidence, not a URL to synthesize from a requested date. Source: [URL checks](../../../adapters/market-squawk-adapter-iex-hist/src/catalog.rs), [transport](../../../adapters/market-squawk-adapter-iex-hist/src/transport.rs).

| HTTP field / representation | Wire type | Meaning |
|---|---|---|
| Body | binary bytes | Selected `.pcap.gz` or `.pcap`, never an array of market rows |
| `Content-Length` | ASCII integer | Bytes in this response; reconcile with selected/resumed range |
| `Content-Range` | HTTP range text | Resumed byte coordinates and complete object length |
| `ETag` | opaque header string | Exact server object validator when supplied |
| `Content-Type`, `Content-Encoding` | header strings | Representation checks; file gzip and HTTP content encoding are separate |

The adapter expects 200 for a new transfer and 206 for an admitted range continuation. It validates response headers, exact object size and identity against retained progress. A failed/resumed job is not a complete file. Local SHA-256 is calculated over retained bytes; the catalog has no admitted upstream SHA/MD5/checksum member. Gzip decompression is incremental and checks corruption, trailing bytes/extra members and expansion admission. DPLC passes through uncompressed PCAP. Redirects, unreviewed links, unsolicited encodings, wrong ranges, truncation and integrity drift fail rather than returning a successful empty generation.

## PCAP container

Source: [current classic-PCAP parser](../../../adapters/market-squawk-adapter-iex-hist/src/decode.rs). These are decoder-supported binary layouts, not a claim that every possible PCAP/link/network format is supported. Offsets below are zero-based bytes; integer byte order follows PCAP magic. No binary member is nullable.

| Global header offset | Bytes / type | Meaning / admitted constraint |
|---|---|---|
| 0 | 4, magic | Little/big endian; microsecond/nanosecond variants |
| 4, 6 | 2 each, unsigned integer | Version major/minor 2.4 |
| 8 | 4, signed integer | Timezone correction; locally zero |
| 12 | 4, unsigned integer | Accuracy; locally zero |
| 16 | 4, unsigned integer | Capture snapshot length; locally 64–65,535 |
| 20 | 4, unsigned integer | Link type; locally 1 (Ethernet) |

| Record header offset | Bytes / type | Meaning |
|---|---|---|
| 0 | 4, unsigned integer | Capture Unix seconds |
| 4 | 4, unsigned integer | Capture subsecond count in magic-selected units |
| 8 | 4, unsigned integer | Captured packet bytes |
| 12 | 4, unsigned integer | Original packet bytes; local parser requires equality |

The decoder extracts Ethernet/at most two 802.1Q or 802.1ad VLAN tags → IPv4 without options → UDP → IEX-TP. It checks the IPv4 checksum and any nonzero UDP checksum; zero UDP checksum is admitted. Unsupported framing, fragments and truncation fail. Capture time is separate from IEX send time and each message's event time.

## IEX-TP outbound header and message framing

Source: [IEX transport specification directory](https://www.iex.io/resources/equities/trading/documents), [current offset parser](../../../adapters/market-squawk-adapter-iex-hist/src/decode.rs). All following multibyte IEX integers are little endian.

| Header offset | Bytes / type | Meaning |
|---|---|---|
| 0 | 1, unsigned | Version, locally 1 |
| 1 | 1, unsigned | Reserved, locally zero |
| 2 | 2, unsigned | Protocol ID: TOPS `0x8003`, DEEP `0x8004`, DEEP+ `0x8005` |
| 4 | 4, unsigned | Channel ID, validated for selected family |
| 8 | 4, unsigned | Session ID |
| 12 | 2, unsigned | Payload byte length |
| 14 | 2, unsigned | Message count |
| 16 | 8, signed | Stream byte offset |
| 24 | 8, signed | First message sequence number |
| 32 | 8, signed | Send Unix nanoseconds |

Forty-byte header precedes payload. Each message is framed by its two-byte unsigned length followed by that many bytes. Local TOPS/DEEP/DPLS channel is 1; DPLC admits channels 1–16 with separately validated distribution roles. A heartbeat has zero message count/payload and retains continuity coordinates; it is not a market event. Current decoder requires a session start at sequence 1/offset 0 and then exact per-channel progression. Session reset, gap, duplicate/out-of-order coordinate, send-time regression or inconsistent count/length reject continuity. No remote gap-fill is inferred from HIST files.

## Shared native message prefix and numeric conventions

Sources: [TOPS primary specification](https://storage.googleapis.com/assets-bucket/exchange/assets/IEX%20TOPS%20Specification%20v1.66.pdf), [native model and decoder](../../../adapters/market-squawk-adapter-iex-hist/src/decode.rs). Tables below explicitly describe currently decoded fixed prefixes. Primary DEEP/DEEP+ document pages were reachable during review but did not expose their underlying PDFs through the rendered response; code's selected revision/offset evidence is therefore retained separately from fresh upstream-PDF verification.

| Offset / field | Bytes / type | Meaning |
|---|---|---|
| 0 / message type | 1, ASCII byte | Family discriminator below |
| 1 / flags or status | 1, unsigned/ASCII | Type-specific bit flags or enum |
| 2 / timestamp | 8, signed integer | Unix nanoseconds of original message |
| 10 / symbol (except system) | 8, ASCII | Left-justified, space-padded instrument symbol |

`Price` means signed 64-bit fixed point in 1/10,000 dollar units; no float conversion. `Size` means unsigned 32-bit shares. IDs are signed 64-bit integers in these decoded messages, validated nonnegative locally. All fields are physically present; zero/blank values have type-specific meaning rather than JSON missing/null semantics. Local clocks must lie in admitted trade-date bounds, and event time may not exceed send time.

| Message / prefix bytes | Payload after common prefix: offset, type and meaning | Feed |
|---|---|---|
| `S` / 10 | Byte 1 system event: `O` start messages, `S` start system hours, `R` start regular market, `M` end regular market, `E` end system hours, `C` end messages; no symbol | All |
| `D` / 31 | 18 `Size` round-lot size; 22 `Price` adjusted prior close; 30 byte LULD tier 0/1/2; byte 1 security flags | All |
| `H` / 22 | Byte 1 trading status `H/O/P/T` halted/order acceptance/paused/trading; 18 four-byte space-padded reason | All |
| `I` / 18 | Byte 1 retail indicator: blank/A/B/C none/buy/sell/both | All |
| `O` / 18 | Byte 1 operational status `O/N` halted/not halted | All |
| `P` / 19 | Byte 1 short-sale-test active 0/1; byte 18 blank/A/C/D/N no test/activated/continued/deactivated/unavailable | All |
| `E` / 18 | Byte 1 `O/C` opening/closing process complete | DEEP/DEEP+ |
| `Q` / 42 | 18 `Size` bid; 22 `Price` bid; 30 `Price` ask; 38 `Size` ask; byte 1 quote flags | TOPS |
| `8`, `5` / 30 | Buy/sell price level: 18 `Size`, 22 `Price`; byte 1 event-complete 0/1 | DEEP |
| `T`, `B` / 38 | Trade/trade break: 18 `Size`, 22 `Price`, 30 eight-byte trade ID; byte 1 sale-condition flags | All |
| `X` / 26 | 18 `Price`; byte 1 `Q/M` official opening/closing price | TOPS/DEEP |
| `a` / 38 | Add: 18 eight-byte order ID, 26 `Size`, 30 `Price`; byte 1 `8/5` buy/sell | DEEP+ |
| `M` / 38 | Modify: 18 order ID, 26 replacement `Size`, 30 replacement `Price`; byte 1 bit 0 maintains priority | DEEP+ |
| `R` / 26 | Delete: 18 order ID; byte 1 reserved zero | DEEP+ |
| `L` / 46 | Execute: 18 order ID, 26 executed `Size`, 30 `Price`, 38 trade ID; byte 1 sale-condition flags | DEEP+ |
| `C` / 18 | Clear symbol book; byte 1 reserved zero | DEEP+ |

Quote zero price/size pairs denote an absent side; inconsistent pairs/crossed quotes are rejected. Price-level zero size removes the level at positive price. Trades, adds/modifies and executions require positive prices/sizes locally. Trade breaks retain original trade identity rather than adding another positive trade. The parser retains original flag bytes; the documented TOPS meanings below do not establish every DEEP+ flag extension.

Source: [TOPS 1.66 Appendix A](https://storage.googleapis.com/assets-bucket/exchange/assets/IEX%20TOPS%20Specification%20v1.66.pdf).

| Flag byte | Set-bit masks / meanings; clear-bit meaning |
|---|---|
| Security directory | `0x80` test, `0x40` when-issued, `0x20` ETP; clear means false |
| Quote | `0x80` unavailable for trading; clear active. `0x40` extended session; clear regular |
| Trade report/break | `0x80` ISO, `0x40` extended hours, `0x20` odd lot, `0x10` trade-through exempt, `0x08` single-price cross; clear means opposite |

Sale-condition flags affect last-sale/high-low eligibility; positive execution price alone does not authorize replacing a displayed regular-session last sale. Complete eligibility and feed-specific flag handling remain specification-dependent.

### Auction `A`, 80-byte prefix

Source: [TOPS auction specification](https://storage.googleapis.com/assets-bucket/exchange/assets/IEX%20TOPS%20Specification%20v1.66.pdf), [auction parser](../../../adapters/market-squawk-adapter-iex-hist/src/decode.rs).

| Offset / field | Type | Meaning |
|---|---|---|
| 1 / auction type | ASCII byte | O/C/I/H/V opening/closing/IPO/halt/volatility |
| 18 / paired shares | Size | Auction-book shares paired at reference price |
| 22 / reference price | Price | Auction reference |
| 30 / indicative clearing price | Price | Indicative match price |
| 38 / imbalance shares | Size | Unmatched quantity |
| 42 / imbalance side | ASCII byte | B/S/N buy/sell/none |
| 43 / extension number | unsigned byte | Auction extension count |
| 44 / scheduled auction time | unsigned 32-bit integer | Unix seconds, unlike nanosecond prefix |
| 48 / auction-book clearing price | Price | Book-only clearing price |
| 56 / collar reference price | Price | Collar base |
| 64 / lower collar; 72 / upper collar | Price each | Auction limits |

Unused collar fields are zero upstream. These zeros indicate inapplicability, not missing bytes or a zero-valued executable price.

## Unknown fields, errors and evidence gaps

Native messages can grow by appended fields. The decoder records mapped prefix bytes and preserves extensions; unknown message types become an explicit `Unmapped` event with type/length/digest, not invented quotes. This is partial projection, not complete upstream schema support. File content, selected spec versions and decoder identity remain bound to publication evidence.

Remaining schema evidence: a fresh stable first-party catalog contract; complete DEEP/DEEP+ flag dictionaries and fresh DEEP/DEEP+/transport PDF verification; current retention/server-resume/error-body guarantees; and real selected-file observations. TOPS 1.66 was retrieved from IEX's official asset bucket. No observed binary example or connectivity/decoder-throughput/product-completion claim is made here. T+1 discovery/download/publication availability remains separate from original message timestamps.
