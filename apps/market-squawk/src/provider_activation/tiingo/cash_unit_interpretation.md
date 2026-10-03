# Inferred fixed-pair Tiingo distribution unit, revision 1

Root-admitted 2026-09-16 engineering interpretation: USD per one original share, inferred from Tiingo's documented CRSP adjustment methodology and CRSP's per-share distribution definition. It is not a Tiingo-authored currency field.

This interpretation applies only to the opaque source-admitted SPY CUSIP 78462F103 / VTI CUSIP 922908769 pair, with actual immutable Fund identity, USD quote currency, ARCX listing, matching native ticker and NYSE ARCA metadata, exact strict Tiingo EOD contract and current entitlement. Instrument, actual contract mapping identity, this revision/payload and actual availability are retained in TiingoEodCashUnitEvidence. Other instruments receive no inferred unit.

Original native divCash and splitFactor values remain unchanged. Missing stays missing and explicit zero stays no-event. No payment date, reinvestment, tax adjustment or complete action-family coverage is inferred. Actual splits and mixed events require the existing action ledger.

Reviewed source basis: https://www.tiingo.com/documentation/end-of-day ; https://www.tiingo.com/documentation/corporate-actions/dividends ; https://www.tiingo.com/documentation/general/changelog ; https://www.crsp.org/wp-content/uploads/2023/08/CRSP10-User-Guide.pdf page 5.
