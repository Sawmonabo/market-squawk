# Tauri desktop MPL dependency notice

Market Squawk's Tauri 2 desktop application uses the following unmodified, file-level copyleft
components under the Mozilla Public License 2.0 (`MPL-2.0`):

| Component | Version | Cargo package SHA-256 | Packaged source |
| --- | --- | --- | --- |
| `cssparser` | 0.36.0 | `dae61cf9c0abb83bd659dab65b7e4e38d8236824c85f0f804f173567bda257d2` | [crates.io](https://crates.io/api/v1/crates/cssparser/0.36.0/download) |
| `cssparser-macros` | 0.6.1 | `13b588ba4ac1a99f7f2964d24b3d896ddc6bf847ee3855dbd4366f058cfcd331` | [crates.io](https://crates.io/api/v1/crates/cssparser-macros/0.6.1/download) |
| `dtoa-short` | 0.3.5 | `cd1511a7b6a56299bd043a9c167a6d2bfb37bf84a6dfceaba651168adfb43c87` | [crates.io](https://crates.io/api/v1/crates/dtoa-short/0.3.5/download) |
| `option-ext` | 0.2.0 | `04744f49eae99ab78e0d5c0b603ab218f515ea8cfe5a456d7629ad883a3b6e7d` | [crates.io](https://crates.io/api/v1/crates/option-ext/0.2.0/download) |
| `selectors` | 0.36.1 | `c5d9c0c92a92d33f08817311cf3f2c29a3538a8240e94a6a3c622ce652d7e00c` | [crates.io](https://crates.io/api/v1/crates/selectors/0.36.1/download) |

The complete MPL 2.0 license text is available from
[Mozilla](https://www.mozilla.org/en-US/MPL/2.0/). These components are not modified by Market
Squawk. Their exact packaged source remains available through the links above. The MPL permits
these files to be combined with differently licensed files in a larger work; Market Squawk's other
files do not become MPL-covered merely by being compiled with them.

A desktop release must carry this notice in its third-party notices and retain the exact source
links. Any future modification to an MPL-covered file must make that modified source available
under MPL-2.0. The corresponding `cargo-deny` exceptions are deliberately scoped to the exact five
package versions above.
