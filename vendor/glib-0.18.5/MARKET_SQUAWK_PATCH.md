# Market Squawk glib 0.18.5 soundness backport

This directory is the crates.io `glib` 0.18.5 package whose published checksum is
`233daaf6e83ae6a12a52055f568f9d7cf4671dabb78ff9560ab6da230ce00ee5`.

Market Squawk applies only the two-token mutability correction in
`src/variant_iter.rs` from the upstream, maintainer-approved fix:

- upstream pull request: <https://github.com/gtk-rs/gtk-rs-core/pull/1343>
- verified upstream fix commit:
  <https://github.com/gtk-rs/gtk-rs-core/commit/b5a4071e439bef2b5eea76c3aa25e5ae84839e34>
- RustSec advisory: <https://rustsec.org/advisories/RUSTSEC-2024-0429.html>

The correction changes the C out-parameter from `&p` to `&mut p`, matching the upstream fix. The
crate version remains 0.18.5 because Tauri 2's current Linux GTK3 graph requires the 0.18 API.
RustSec therefore still identifies the version range; Market Squawk's audit exception is valid only
while this exact local patch remains active. Remove the patch and its exception as soon as Tauri
ships a compatible dependency graph containing the upstream correction.
