# Market Squawk release inputs

This directory contains reviewed inputs for constructing the public Market Squawk release. It is
not a generated-artifact directory.

- `release-components.json` pins the exact uv, CPython, and PyArrow artifacts accepted for each
  supported platform.
- `install.sh` is the POSIX release template. The publication job replaces every token with one
  exact tag and the verified SHA-256 digest of each macOS and Linux bootstrap before uploading it.

`scripts/build_python_release.py` first creates the sealed CPython product.
`scripts/build_complete_release.py` then combines that product with the native desktop, capture
helper, installer, uv, licenses, and notices. The result is a deterministic complete ZIP, a
component-level release manifest, the platform bootstrap, and `SHA256SUMS`.

Generated release outputs must remain outside the source tree. They are published only by the
tag-bound release workflow after native package and installed-product verification.
