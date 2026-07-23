# Local ONNX inference operations

Market Squawk's required ONNX implementation is `TractOnnxBackend`. It is self-contained Rust,
loads only an admitted in-memory model bundle, and requires no external native library, Python
process, account, service, or network request. Build the modeling crate with `onnx-tract` to use it.
The required tract dependency inventory includes one exact MPL-2.0 transitive component; its source
and distribution notice are recorded in
[`tract-onnx-notice.md`](../licenses/tract-onnx-notice.md).

The `onnx-runtime` feature is an optional CPU acceleration path. It adds the pinned Rust `ort`
binding with default features disabled and dynamic loading enabled. It does not bundle, download,
copy, discover, or install ONNX Runtime. If the optional library is absent, rejected, fails warm-up,
or fails inference, the already-constructed tract backend remains available. Optional-runtime
inference failures automatically use that exact tract generation.

## Admitted native component

The current policy admits ONNX Runtime 1.24.4 on Linux arm64 and x86_64 ELF targets. The worker
copies the descriptor-verified library into an immutable sealed memory file and loads that exact
descriptor through `/proc/self/fd`; the descriptor remains open for the session lifetime. External
ONNX Runtime loading fails closed on platforms without this descriptor-bound loader. The required
tract backend remains available there.

The CPU shared library must be supplied by the operator under a dedicated local root. Retain the
upstream MIT license with the library and inventory it using
[`onnx-runtime-notice.md`](../licenses/onnx-runtime-notice.md). The tracked
[`onnx-runtime-policy.json`](../verification/onnx-runtime-policy.json) pins the runtime/API version,
size ceiling, supported platforms, environment contract, and notice digest. Upstream references:

- [ONNX Runtime 1.24.4 release](https://github.com/microsoft/onnxruntime/releases/tag/v1.24.4)
- [ONNX Runtime MIT license](https://github.com/microsoft/onnxruntime/blob/v1.24.4/LICENSE)
- [Official shared-library build guidance](https://onnxruntime.ai/docs/build/inferencing.html)

## One-time local admission

Use a root dedicated to the optional runtime. The library and evidence paths must be regular files
under that canonical root; symlinked path components are rejected. This example layout is portable
apart from the platform-specific library name:

```text
/absolute/path/onnx-runtime-root/
├── lib/libonnxruntime.so
├── LICENSE
└── admission.json
```

Configure exact local paths and the independently recorded library digest:

```bash
export MARKET_SQUAWK_ONNX_RUNTIME_ROOT=/absolute/path/onnx-runtime-root
export MARKET_SQUAWK_ONNX_RUNTIME="$MARKET_SQUAWK_ONNX_RUNTIME_ROOT/lib/libonnxruntime.so"
export MARKET_SQUAWK_ONNX_RUNTIME_EVIDENCE="$MARKET_SQUAWK_ONNX_RUNTIME_ROOT/admission.json"
export MARKET_SQUAWK_ONNX_RUNTIME_SHA256=<64-lowercase-hex-digest>

python3 scripts/verify_onnx_runtime.py \
  --policy docs/verification/onnx-runtime-policy.json \
  --library "$MARKET_SQUAWK_ONNX_RUNTIME"
```

The verifier performs bounded local reads, confirms SHA-256, binary format, architecture, exact
runtime version through `OrtGetApiBase`, tracked policy, and notice identity, then atomically writes
canonical admission evidence. Its JSON output includes the exact policy and evidence hashes needed
by application configuration. It performs no network operation and never modifies the supplied
library.

For an exact release candidate, bind the report to the frozen Git commit and tree:

```bash
python3 scripts/verify_onnx_runtime.py \
  --head "$HEAD_SHA" \
  --tree "$TREE_SHA" \
  --policy docs/verification/onnx-runtime-policy.json \
  --library "$MARKET_SQUAWK_ONNX_RUNTIME" \
  --report "$EVIDENCE_DIR/onnx-runtime.json"
```

## Application construction contract

Application composition must construct the tract backend first. Optional setup then follows this
closed sequence:

1. Open `ControlledOnnxRuntimeRoot` from the configured root.
2. Construct `ExternalOnnxRuntimeReference` from the library-relative path, evidence-relative path,
   exact library/evidence/policy hashes, version `1.24.4`, and current platform enum.
3. Call `ControlledOnnxRuntimeRoot::admit` before any native code is loaded.
4. Call `ExternalOnnxRuntimeBackend::try_from_tract(&tract_backend, admission)`.
5. Publish the optional backend only after external and tract zero-input warm-up results agree.

`try_from_tract` borrows the required `Arc<TractOnnxBackend>`; a construction failure cannot consume
the fallback. Each request has one configured total deadline. The optional worker receives the first
half and tract retains the second half. Response receipt, request-writer completion, and final result
publication each recheck the absolute deadline; a late or uncertain result has no authority. A
dispatched failure drops the generation's input and signals child termination before transfer to the
bounded worker-owned reaper, where process waits and thread joins occur. Expiry before dispatch
restores the retained generation even when it occurs after the caller takes ownership.
`OnnxRuntimeEvidence` binds the exact model and graph policy to a versioned worker-runtime semantics
digest covering the admitted helper executable, protocol bounds, compute and intermediate limits,
target resource-containment profile, startup/deadline rules, and reaper behavior. The warm-up identity
also binds that digest and the finite warm-up score. If the total request deadline is exhausted, that
request produces no action and the next request still has a usable tract fallback. Neither backend
can emit an order directly; model output still passes through strategy and the sole risk boundary.

## Failure diagnosis

- `InvalidReference`, `Symlink`, or `RootEscape`: correct the configured canonical root and relative
  paths; do not point through aliases.
- `EvidenceDigest`, `EvidenceMismatch`, or `LibraryDigest`: rerun admission only after intentionally
  updating the exact library and configuration hashes.
- `UnsupportedPlatform`: use the required tract backend; external ONNX Runtime is admitted only on
  supported Linux targets.
- `Platform` or `Environment`: use the CPU library built for the current Linux architecture and
  ensure its ordinary local loader dependencies are present.
- `Session`, `WarmUp`, `Parity`, or graph-policy errors: keep the tract backend active and inspect
  the model/operator/static-shape contract before attempting optional acceleration again.

Replacing the runtime library, policy, notice, evidence, model generation, version, or platform
invalidates the previous optional admission. Generate new evidence; never reuse an earlier tuple.

Release packaging must retain both ONNX notices: the tract notice whenever the required backend is
shipped, and the native-runtime notice only when the optional operator-supplied library is included.
