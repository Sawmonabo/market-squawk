# GitHub Actions pin review — 2026-07-16

This review records the immutable GitHub Action commits selected for the Quarter 1 verification
workflow. Each reference was resolved from its official GitHub repository, checked through the
GitHub commits API, and then pinned by its full 40-character commit identifier. Floating release or
major-version references are not used by the workflow.

| Action | Reviewed release/source | Immutable commit | Verification decision |
|---|---|---|---|
| `actions/checkout` | [`v6.0.2`](https://github.com/actions/checkout/releases/tag/v6.0.2) | [`de0fac2e4500dabe0009e67214ff5f5447ce83dd`](https://github.com/actions/checkout/commit/de0fac2e4500dabe0009e67214ff5f5447ce83dd) | Official release commit, GitHub-verified, published 2026-01-09. Credentials are not persisted because this job only reads the checkout. |
| `dtolnay/rust-toolchain` | [Generic action source](https://github.com/dtolnay/rust-toolchain/blob/fa04a1451ff1842e2626ccb99004d0195b455a88/action.yml) | [`fa04a1451ff1842e2626ccb99004d0195b455a88`](https://github.com/dtolnay/rust-toolchain/commit/fa04a1451ff1842e2626ccb99004d0195b455a88) | GitHub-verified generic action commit from 2026-06-30. The workflow supplies the corrected `toolchain: 1.97.1` input explicitly, plus `rustfmt` and Clippy. |
| `Swatinem/rust-cache` | [`v2.9.1`](https://github.com/Swatinem/rust-cache/releases/tag/v2.9.1) | [`c19371144df3bb44fab255c43d04cbc2ab54d1c4`](https://github.com/Swatinem/rust-cache/commit/c19371144df3bb44fab255c43d04cbc2ab54d1c4) | Official release commit, GitHub-verified, published 2026-03-12. |

Resolution was cross-checked with each repository's official Git refs. The immutable commit remains
stable even when a release branch or major-version tag moves; future action upgrades therefore
require an explicit source review and repository change.
