# GitHub Actions pin review — 2026-07-16

This review records the immutable GitHub Action commits selected for the verification workflow.
Each reference was resolved from its official GitHub repository, checked through the GitHub
commits API, and then pinned by its full 40-character commit identifier. Floating release or
major-version references are not used by the workflow. The desktop-build additions were reviewed
on 2026-07-28.

| Action | Reviewed release/source | Immutable commit | Verification decision |
|---|---|---|---|
| `actions/checkout` | [`v6.0.2`](https://github.com/actions/checkout/releases/tag/v6.0.2) | [`de0fac2e4500dabe0009e67214ff5f5447ce83dd`](https://github.com/actions/checkout/commit/de0fac2e4500dabe0009e67214ff5f5447ce83dd) | Official release commit, GitHub-verified, published 2026-01-09. Credentials are not persisted because this job only reads the checkout. |
| `actions/setup-node` | [`v6.4.0`](https://github.com/actions/setup-node/releases/tag/v6.4.0) | [`48b55a011bda9f5d6aeb4c2d9c7362e8dae4041e`](https://github.com/actions/setup-node/commit/48b55a011bda9f5d6aeb4c2d9c7362e8dae4041e) | Official release commit, GitHub-verified, published 2026-04-20. CI selects the exact current Node 24 LTS patch and the committed pnpm lockfile. |
| `actions/upload-artifact` | [`v7.0.1`](https://github.com/actions/upload-artifact/releases/tag/v7.0.1) | [`043fb46d1a93c77aae656e7c1c64a875d1fc6a0a`](https://github.com/actions/upload-artifact/commit/043fb46d1a93c77aae656e7c1c64a875d1fc6a0a) | Official release commit, GitHub-verified, published 2026-04-10. Only native installer outputs are retained, for seven days. |
| `dtolnay/rust-toolchain` | [Generic action source](https://github.com/dtolnay/rust-toolchain/blob/fa04a1451ff1842e2626ccb99004d0195b455a88/action.yml) | [`fa04a1451ff1842e2626ccb99004d0195b455a88`](https://github.com/dtolnay/rust-toolchain/commit/fa04a1451ff1842e2626ccb99004d0195b455a88) | GitHub-verified generic action commit from 2026-06-30. The workflow supplies the corrected `toolchain: 1.97.1` input explicitly, plus `rustfmt` and Clippy. |
| `pnpm/action-setup` | [`v6.0.8`](https://github.com/pnpm/action-setup/releases/tag/v6.0.8) | [`0e279bb959325dab635dd2c09392533439d90093`](https://github.com/pnpm/action-setup/commit/0e279bb959325dab635dd2c09392533439d90093) | The signed `v6.0.8` release tag resolves to this immutable commit. The action reads the exact pnpm version from the desktop package manifest. |
| `Swatinem/rust-cache` | [`v2.9.1`](https://github.com/Swatinem/rust-cache/releases/tag/v2.9.1) | [`c19371144df3bb44fab255c43d04cbc2ab54d1c4`](https://github.com/Swatinem/rust-cache/commit/c19371144df3bb44fab255c43d04cbc2ab54d1c4) | Official release commit, GitHub-verified, published 2026-03-12. |
| `tauri-apps/tauri-action` | [`v0.6.2`](https://github.com/tauri-apps/tauri-action/releases/tag/v0.6.2) | [`84b9d35b5fc46c1e45415bdb6144030364f7ebc5`](https://github.com/tauri-apps/tauri-action/commit/84b9d35b5fc46c1e45415bdb6144030364f7ebc5) | Official Tauri release commit published 2026-03-14. The release tag is not GitHub-verified, so the workflow trusts only the reviewed immutable source commit and grants it no release-write permission. |

Resolution was cross-checked with each repository's official Git refs. The immutable commit remains
stable even when a release branch or major-version tag moves; future action upgrades therefore
require an explicit source review and repository change.
