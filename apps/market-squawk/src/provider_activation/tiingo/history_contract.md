# Tiingo daily EOD adjusted surface interpretation

Reviewed 2026-09-16 against https://www.tiingo.com/documentation/end-of-day .
The endpoint provides distinct raw and adjusted price and volume fields. Tiingo explicitly adopts CRSP split and dividend adjustment methodology. The adjusted surface therefore has the canonical All adjustment class; the raw surface stays Raw. Daily date is a nominal financial date. This interpretation does not turn that date into a candle timestamp or claim a payment date.

This reviewed artifact does not assert a cash distribution currency or share denominator. Those require their own source semantic authority.
