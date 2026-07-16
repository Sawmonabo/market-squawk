# Changelog

All notable changes to this project are documented here.

## 0.1.0 - 2026-07-15

- Added public Coinbase Exchange Level 2, heartbeat, and match capture.
- Added acknowledged CRC-framed raw journaling with corruption, truncation, and concurrent-writer detection.
- Added fail-closed order-book quality, staleness, reconnect, and crossed-book handling.
- Added fixed-point online features, deterministic paper strategy evaluation, and risk limits.
- Added journal replay through the same Coinbase decoder and engine state transitions.
- Added bounded local MCP tools over stdio with strict arguments and rate limiting.
- Added local CLI, offline mock source, tests, verification scripts, and GitHub Actions CI.
