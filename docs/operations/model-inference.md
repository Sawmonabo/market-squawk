# Model training, admission, and inference operations

This runbook covers the sealed Python training release, native candidate handoff, immutable model
admission, native and ONNX inference, evaluation evidence, and restart recovery.

| Field | Value |
| --- | --- |
| Document type | Operations runbook |
| Audience | Local research operators, model reviewers, release engineers, and incident responders |
| Status | Current |
| Last substantive review | 2026-08-03 |
| Review basis | Current installed service, durable-job, model, and ONNX contracts; not release approval evidence |

## Contents

- [Scope](#scope)
- [Safety and authority boundaries](#safety-and-authority-boundaries)
- [Preconditions and current product limits](#preconditions-and-current-product-limits)
- [Use durable model work in the installed product](#use-durable-model-work-in-the-installed-product)
- [Prepare the verified Python training release](#prepare-the-verified-python-training-release)
- [Train and export a native candidate](#train-and-export-a-native-candidate)
- [Prepare an admission request](#prepare-an-admission-request)
- [Admit a native bundle](#admit-a-native-bundle)
- [Admit an ONNX bundle](#admit-an-onnx-bundle)
- [Inspect admitted models](#inspect-admitted-models)
- [Predict and evaluate](#predict-and-evaluate)
- [Runtime and no-action behavior](#runtime-and-no-action-behavior)
- [Optional external ONNX Runtime evidence](#optional-external-onnx-runtime-evidence)
- [Success evidence](#success-evidence)
- [Idempotency, rollback, and recovery](#idempotency-rollback-and-recovery)
- [Failure modes](#failure-modes)
- [Local state locations](#local-state-locations)
- [Related documentation, code, and evidence](#related-documentation-code-and-evidence)
- [External sources](#external-sources)

## Scope

This page documents:

- building and selecting the verified Python training release;
- opening one exact point-in-time Python dataset export;
- deterministic native linear or logistic training outside the live path;
- independent authority review and Rust-validated candidate export;
- `model admit <REQUEST> --confirm`;
- `model list` and `model metadata <MODEL>`;
- `model predict <REQUEST>` and `model evaluate <REQUEST> --confirm`;
- the tract-based ONNX graph policy and persistent sibling-worker lifecycle; and
- restart validation, immutable replay, no-action failure, and recovery.

It does not place Python in the live event-to-decision path, make a model output an order, or grant
authority by naming a path. Dataset build output exposes the exact Python export digest, and the
sealed release builder binds the final application, validator, ONNX worker, and supported
`market-squawk-train` driver.

## Safety and authority boundaries

- Python research and training execute outside the live path. Production native inference runs in
  Rust; ONNX inference runs through a bounded Rust-owned helper. Neither path imports Python.
- A training proposal is not admission authority. An independent operator must review the exact
  proposal bytes and place the accepted authority file under a controlled root disjoint from the
  Market Squawk data root.
- The model candidate lives below `<data-root>/artifacts`. `candidateDirectory` is a relative
  capability path; the CLI does not accept an arbitrary absolute candidate path.
- Candidate metadata, artifact, training run, authority, dataset export, dataset selection, catalog
  identity, training environment, feature semantics, and runtime policy are digest-bound.
- `model admit` is a durable mutation and requires `--confirm`. Prediction is read-only.
  `model evaluate` also requires `--confirm` because it retains bounded process-local evidence.
- Model results report `executionAuthority: "none"`. A positive or negative model decision is
  research/strategy input only; it still must pass the sole strategy and risk boundaries before
  any order can exist.
- Every backend failure has `no_action` behavior. Do not convert unavailable, late, nonfinite,
  mismatched, or uncertain inference into a trade.
- Never edit the durable admission index, candidate files, dataset admission, or authority bytes
  to repair a model. Recovery restores the exact authority tuple or admits a new immutable bundle.

## Preconditions and current product limits

Set controlled roots:

```bash
export DATA_ROOT=/absolute/path/to/market-squawk-data
export TRAINING_RELEASE_ROOT=/absolute/path/to/active/complete-release
export AUTHORITY_ROOT=/absolute/path/to/model-authority
```

`AUTHORITY_ROOT` must be outside and disjoint from `DATA_ROOT`. The candidate output root must be
inside the prepared artifact root, for example:

```bash
mkdir -p "$DATA_ROOT/artifacts/models/research-linear-v1"
```

Before admission:

- `market-squawk --data-dir "$DATA_ROOT" config validate` and `doctor` must succeed;
- the selected training release must be installed and unchanged;
- the exact dataset export receipt, selection cutoff, selection digest, and catalog identity must
  already exist in accepted evidence;
- the candidate must have passed its adjacent Rust validator;
- the authority file must be an independently reviewed, regular, no-follow file of at most
  256 KiB; and
- ONNX admission additionally requires the exact sibling `market-squawk-onnx-worker` beside the
  running application executable.

The verified release installs `market-squawk-train`, the supported production driver. It accepts
one closed, bounded configuration; deterministically fits `linear` or `logistic`; emits either the
Gemm regression graph or Gemm-plus-Sigmoid binary-probability graph as a self-contained opset-13
ONNX artifact; validates the candidate with the release-bound adjacent Rust validator; and invokes
only the exact release-bound application and ONNX worker for admission. Model family and artifact
format are separate authorities: `modelKind` is `linear` or `logistic`, while `artifactFormat` is
`onnx`.

## Use durable model work in the installed product

The installed Desktop Models workflow starts `Model.StartTraining` and `Model.StartForecast` as
durable service jobs. The service, not the WebView or a connected MCP/CLI client, owns the exact
workspace, captured request, worker lifecycle, cancellation, output admission, and terminal
receipt. A client disconnect therefore does not discard accepted training or forecast work.

### Prerequisites and authority checks

Before starting work, use the Models page or exact typed service status to confirm the active
workspace, selected signed training release, exact dataset/feature/label identities, model input
contract, applicable model/admission authority, and available job/storage budget. The request is a
code-owned closed schema; no raw filesystem path, interpreter path, arbitrary Python command, or
arbitrary ONNX runtime is supplied by the UI or MCP client. A training proposal still requires
independent authority review before immutable admission.

### Procedure, evidence, and recovery

1. In **Models**, select the workflow whose displayed identities and source-time evidence match
   the intended research question. Review validation, expected input/output, cancellation boundary,
   and the explicit confirmation prompt.
2. Confirm the start. `Model.StartTraining` or `Model.StartForecast` returns a durable job receipt;
   record its `jobId`, workspace/service generation, input identity, and initial event sequence.
3. Watch it from **Operations** or the Models page. `Job.Get`, bounded `Job.Watch`, `Job.Cancel`,
   `Job.Confirm`, and `Job.Retry` retain the service state. Progress is a named monotonic phase;
   percentage appears only when objective units exist. An accepted start is not completion.
4. On `Completed`, open the returned result/controlled artifact and exact model or forecast
   identity. Forecast review uses `Model.GetForecast`, `Model.ListForecasts`, and
   `Model.GetForecastOutcomes`. Training still needs the reviewed immutable admission flow below
   before a new bundle is usable.

Success is a terminal completed job with bounded result identity and a subsequent typed read that
returns the same evidence after client reconnection. An interrupted, failed, cancelled, or
`AwaitingConfirmation` job has not produced a usable model merely because temporary worker output
exists. Cancellation is cooperative: request it through `Job.Cancel`, then wait for terminal
evidence; do not kill a worker or delete staged output.

After a restore, workspace switch, update, or stale-generation response, reconnect before reading
or mutating and never reuse earlier requests, previews, job handles, or confirmations. If the
service reports `Interrupted` or recovery required, preserve its phases, diagnostics, inputs, and
artifact references. Use `Job.Retry` only when the returned contract declares it recoverable;
otherwise repair the first failed training-release, dataset, policy, disk, or worker-identity
authority and start a new governed request. No incomplete output is admitted or shown as a forecast.

## Prepare the verified Python training release

An installed product release carries its selected managed Python and uv runtime plus the locked
Python environment for its target. The Desktop automatically selects the active immutable release
root. A headless process must pass that same active root through
`--training-release-root`; do not select the retained previous version or an arbitrary virtual
environment.

Release maintainers build one target-native `release-cp314` product. The first command prepares
only the exact public artifacts pinned in `distribution/release-components.json`; the second
builds offline:

```bash
TARGET=aarch64-apple-darwin
ARTIFACT_ROOT=/absolute/path/to/python-release
COMPONENT_ROOT=/absolute/path/to/release-components

MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK=1 \
python3 -I scripts/build_python_release.py \
  --lock python/wheelhouse-lock.json \
  --target "$TARGET" \
  --artifact-root "$ARTIFACT_ROOT" \
  --component-root "$COMPONENT_ROOT" \
  --prepare-cache-only

python3 -I scripts/build_python_release.py \
  --lock python/wheelhouse-lock.json \
  --target "$TARGET" \
  --artifact-root "$ARTIFACT_ROOT" \
  --component-root "$COMPONENT_ROOT" \
  --offline

export TRAINING_RELEASE_ROOT="$ARTIFACT_ROOT/release-cp314"
```

The one-time cache preparation is the only step allowed to acquire the hash-pinned public inputs.
Retain its evidence. Do not let the offline build resolve an unpinned package.

`TARGET` must match the host and be one of:

```text
aarch64-apple-darwin
x86_64-apple-darwin
x86_64-pc-windows-msvc
x86_64-unknown-linux-gnu
```

Native publisher signing is a release-authority choice, not a local default. The release workflow
adds `--sign-native` only when the corresponding Apple or Windows credential authority exists;
otherwise the target remains `provenance-only` and is still bound by checksums, the closed release
manifest, and GitHub attestations.

One selected release root must contain and bind at least:

```text
<training-release-root>/
├── bin/
│   ├── python
│   ├── market-squawk
│   ├── market-squawk-model-validator
│   └── market-squawk-onnx-worker
└── share/market-squawk/
    ├── training-environment.json
    └── market-squawk-release.json
```

The tree and commands in this runbook use Unix names. On Windows, the managed interpreter is
`<training-release-root>\python.exe`, installed programs use the `.exe` suffix, and the training
driver is under `Scripts\`.

The verifier also checks the installed interpreter, native extension, wheels, distribution
`RECORD` entries, native trust declaration, release manifest, and the release-bound application,
validator, and ONNX worker identities. The running application and its sibling worker must be
those exact installed files. File presence alone is not success evidence.

Use the release's isolated interpreter for an authority-free smoke check:

```bash
"$TRAINING_RELEASE_ROOT/bin/python" -I - <<'PY'
from decimal import Decimal
from market_squawk.finance import OperationContext, simple_returns

result = simple_returns(
    [Decimal("100.00"), Decimal("101.25")],
    [1_000_000_000, 2_000_000_000],
    "USD",
    context=OperationContext(60_000, 100_000),
)
print(result.values)
PY
```

This proves only that the sealed Python environment can run one bounded kernel. It does not prove
that the application binary accepts the release or that a model is admitted.

Every process that opens the model namespace must run the exact application installed in the
selected verified release and select that same release root:

```bash
"$TRAINING_RELEASE_ROOT/bin/market-squawk" \
  --data-dir "$DATA_ROOT" \
  --training-release-root "$TRAINING_RELEASE_ROOT" \
  --output json \
  model list
```

On a fresh data root, omitting the release produces a truthful empty model inventory. Once any
durable model admission exists, omitting or mismatching it makes startup fail with
`durable model admissions require the configured signed training release`; the product will not
hide durable models behind an empty inventory.

## Supported ONNX first-use flow

Create a private configuration file containing the exact retained dataset, feature, label, and
model identities. The schema is closed; every illustrative value below must be replaced with its
accepted evidence before it is used:

```json
{
  "schemaVersion": 1,
  "dataset": {
    "root": "/absolute/path/to/market-squawk-data",
    "exportSha256": "<exact-export-sha256>",
    "asOfUnixNanos": 1753228800000000000,
    "maximumRows": 100000,
    "maximumBytes": 268435456
  },
  "training": {
    "features": [{
      "name": "<registered-feature-name>",
      "version": 1,
      "input_schema_sha256": "<exact-input-schema-sha256>",
      "semantic_sha256": "<exact-feature-semantics-sha256>"
    }],
    "label": {
      "kind": "label",
      "scope": "instrument",
      "corporate_action_sensitivity": "requires_adjustment",
      "name": "<dataset-label-name>",
      "version": 1
    },
    "seed": 17,
    "missingPolicy": "reject",
    "modelId": "<lowercase-non-nil-uuid>",
    "bundleId": "research-linear",
    "bundleVersion": 1,
    "modelKind": "linear",
    "artifactFormat": "onnx"
  },
  "operation": {
    "timeoutMilliseconds": 60000,
    "maximumOperations": 50000000
  },
  "onnx": {
    "opset": 13,
    "inferenceDeadlineMilliseconds": 1000,
    "fallback": "no_action"
  }
}
```

The proposal command trains from the exact point-in-time dataset and exclusively creates the
canonical authority proposal. Its output path must not already exist:

```bash
"$TRAINING_RELEASE_ROOT/bin/market-squawk-train" propose \
  --config /absolute/path/to/training.json \
  --output /absolute/path/to/review/bundle-authority.proposal.json
```

Stop here. An independent operator reviews those exact proposal bytes and, if accepted, places
them unchanged below the disjoint authority root:

```bash
install -m 600 \
  /absolute/path/to/review/bundle-authority.proposal.json \
  "$AUTHORITY_ROOT/research-linear-v1.bundle-authority.json"
```

Finalize retrains deterministically, rejects any authority-byte mismatch, creates only the named
relative parent below `$DATA_ROOT/artifacts`, validates the exact `model.onnx`, and exclusively
writes the closed admission request:

```bash
"$TRAINING_RELEASE_ROOT/bin/market-squawk-train" finalize \
  --config /absolute/path/to/training.json \
  --authority "$AUTHORITY_ROOT/research-linear-v1.bundle-authority.json" \
  --candidate-parent models/research-linear-v1 \
  --request /absolute/path/to/review/research-linear-v1.admission.json
```

The candidate contains `model.onnx`, `bundle.json`, and `training-run.json`. Metadata, training
run, independent authority, admission policy, and durable runtime index all bind
`outputSemantics: "regression"` for `linear` or `"binary_probability"` for `logistic`. The latter
is admitted only when graph preflight proves that the terminal producer is `Sigmoid`; returned
tract and optional external-runtime scores are checked again for finiteness and the inclusive
`[0,1]` probability range before decision thresholds are applied.

Admit with the same signed driver. It hashes the application, ONNX worker, and validator before
invocation, compares them with the signed training-environment receipt, and rechecks them after:

```bash
"$TRAINING_RELEASE_ROOT/bin/market-squawk-train" admit \
  --config /absolute/path/to/training.json \
  --request /absolute/path/to/review/research-linear-v1.admission.json
```

Retain the proposal, accepted authority, candidate receipt identities, admission request/result,
dataset receipt, and signed release evidence as one audit set. To train logistic classification,
change only the reviewed `modelKind` to `logistic` when the exact label is binary; do not relabel a
linear fit or an ONNX representation after training.

## Train and export a native candidate

This section becomes runnable only when accepted evidence supplies:

- the exact dataset `exportSha256`;
- the selection cutoff in Unix nanoseconds;
- the feature identities admitted by the dataset and production feature registry; and
- a candidate output directory below `<data-root>/artifacts` that exists but has no `candidate`
  child.

The Python API enforces at most 100,000 examples, 1,024 features, 2,000,000 retained cells, and
50,000,000 estimated training operations. It supports `linear` and `logistic`, a deterministic
64-bit seed, and missing policy `reject` or `drop_row`.

### 1. Open one exact dataset receipt and form a proposal

Use the sealed interpreter. The following outline uses the shipping API; replace every environment
value with retained evidence rather than another digest with a similar name:

```python
import os
from pathlib import Path

from market_squawk import (
    OperationContext,
    TrainingRun,
    UtcNanoseconds,
    open_dataset,
    training_environment_receipt,
)
from market_squawk.finance import feature_contracts


def make_proposal():
    context = OperationContext(60_000, 1_000_000)
    dataset = open_dataset(
        Path(os.environ["MARKET_SQUAWK_DATASET_ROOT"]),
        os.environ["MARKET_SQUAWK_DATASET_EXPORT_SHA256"],
        UtcNanoseconds(int(os.environ["MARKET_SQUAWK_DATASET_AS_OF_UNIX_NANOS"])),
        max_rows=100_000,
        max_bytes=256 * 1024 * 1024,
        context=context,
    )
    feature_name = os.environ["MARKET_SQUAWK_TRAINING_FEATURE"]
    feature = next(
        value
        for value in feature_contracts(
            context=OperationContext(60_000, 1_000_000)
        )
        if value["name"] == feature_name
    )
    label = next(value for value in dataset.components if value.kind == "label")
    run = TrainingRun(
        dataset=dataset,
        features=[feature],
        label=label,
        seed=int(os.environ["MARKET_SQUAWK_TRAINING_SEED"]),
        missing_policy="reject",
        environment=training_environment_receipt(),
        model_id=os.environ["MARKET_SQUAWK_MODEL_ID"],
        bundle_id=os.environ["MARKET_SQUAWK_BUNDLE_ID"],
        bundle_version=int(os.environ["MARKET_SQUAWK_BUNDLE_VERSION"]),
    )
    return run.fit_evaluate(
        model_kind="linear",
        context=OperationContext(60_000, 50_000_000),
    )
```

`open_dataset` independently verifies the catalog admission, exact export, selected
content-addressed objects, Arrow/Parquet schema, row lineage, split policy, component contract,
selection cutoff, result bounds, and current catalog identity. The training run verifies the
receipt again before fitting and before export.

Choose a feature that is both in the Python `feature_contracts` result and in the exact dataset
component contract. Replace `linear` with `logistic` only when that is the reviewed model kind.

### 2. Stop for independent authority review

The returned `TrainingProposal` exposes:

- `authority_bytes`, the exact canonical authority request;
- `authority_sha256`;
- `training_run_sha256`; and
- the untrusted candidate bytes awaiting independent authorization.

Write `authority_bytes` once to a bounded review location outside both the release and data roots,
record `authority_sha256`, and stop. A separate operator or catalog authority must verify at least
the model/bundle identity, dataset/export/selection tuple, universe, training period, label,
features, training code/environment, validation metrics, artifact digest, and training-run digest.

If accepted, the independent operator places those exact bytes as, for example:

```text
<authority-root>/research-linear-v1/bundle-authority.json
```

Do not edit and rehash the proposal to make it acceptable. Reject it and create a new training
proposal from corrected inputs.

### 3. Recreate and export the accepted proposal

Recreate the proposal deterministically with the same installed release, dataset receipt, seed,
features, label, model kind, and identities. Then use the exact independent authority:

```python
import os
from pathlib import Path

from market_squawk import BundleAuthorityRef, OperationContext

# `make_proposal` is the exact function from the preceding step.
proposal = make_proposal()
authority = BundleAuthorityRef.exact(
    Path(os.environ["MARKET_SQUAWK_AUTHORITY_ROOT"]),
    "research-linear-v1/bundle-authority.json",
    proposal.authority_sha256,
)
receipt = proposal.export(
    Path(os.environ["MARKET_SQUAWK_CANDIDATE_OUTPUT_ROOT"]),
    authority,
    context=OperationContext(60_000, 1_000_000),
)
print(receipt)
```

For the layout used above, set:

```bash
export MARKET_SQUAWK_DATASET_ROOT="$DATA_ROOT"
export MARKET_SQUAWK_DATASET_EXPORT_SHA256=<exact-export-sha256>
export MARKET_SQUAWK_DATASET_AS_OF_UNIX_NANOS=<exact-cutoff>
export MARKET_SQUAWK_TRAINING_FEATURE=<exact-feature-name>
export MARKET_SQUAWK_TRAINING_SEED=17
export MARKET_SQUAWK_MODEL_ID=<lowercase-non-nil-uuid>
export MARKET_SQUAWK_BUNDLE_ID=research-linear
export MARKET_SQUAWK_BUNDLE_VERSION=1
export MARKET_SQUAWK_AUTHORITY_ROOT="$AUTHORITY_ROOT"
export MARKET_SQUAWK_CANDIDATE_OUTPUT_ROOT="$DATA_ROOT/artifacts/models/research-linear-v1"
```

`proposal.export` writes exactly:

```text
<candidate-output-root>/candidate/
├── artifact.json
├── bundle.json
└── training-run.json
```

It calls the exact `market-squawk-model-validator` installed beside the release interpreter before
atomically publishing `candidate`. Retain the `BundleReceipt`, especially metadata, artifact,
training-run, authority, dataset-export, dataset-selection, and catalog-identity digests, plus
`validated_by_rust: true`.

For new production ONNX work, use the supported sealed-driver flow above. The lower-level native
API remains available for reviewed native-only integrations; a copied fixture or ad hoc notebook
is not release evidence.

## Prepare an admission request

The request is one closed schema-version-1 JSON object of at most 8 MiB. It is opened as an
unchanged, bounded, regular file without following symlinks.

For the native candidate above, use:

```json
{
  "schemaVersion": 1,
  "candidateDirectory": "models/research-linear-v1/candidate",
  "metadata": {
    "relativePath": "bundle.json",
    "sha256": "<receipt-metadata-sha256>"
  },
  "authority": {
    "path": "/absolute/path/to/model-authority/research-linear-v1/bundle-authority.json",
    "sha256": "<receipt-authority-sha256>"
  },
  "dataset": {
    "exportSha256": "<receipt-dataset-export-sha256>",
    "asOfUnixNanos": 1753228800000000000,
    "selectionSha256": "<receipt-dataset-selection-sha256>",
    "catalogIdentitySha256": "<receipt-catalog-identity-sha256>"
  },
  "backend": {
    "kind": "native"
  }
}
```

The timestamp above illustrates the integer field and must be replaced by the receipt's exact
selection cutoff.

`candidateDirectory` is relative to `<data-root>/artifacts`, at most 512 bytes and 32 components.
Each component is at most 255 bytes and contains only lowercase ASCII letters, digits, `-`, `_`, or
`.`. It cannot be absolute or contain `..`, `\`, or `:`.

The authority `path` is an explicit safe absolute file path. Its parent root must be disjoint from
the data root. Candidate-internal authority is rejected.

## Admit a native bundle

Run:

```bash
"$TRAINING_RELEASE_ROOT/bin/market-squawk" \
  --data-dir "$DATA_ROOT" \
  --training-release-root "$TRAINING_RELEASE_ROOT" \
  --output json \
  model admit /absolute/path/to/native-admission.json \
  --confirm
```

Before publication, the runtime revalidates the signed training environment, candidate root,
metadata and referenced files, independent authority bytes, exact Python dataset selection,
catalog identity, feature registry, bundle contract, native format, and fixed resource/deadline
bounds. It builds a complete proposed registry/backend set, commits the canonical durable index,
and only then atomically swaps process state.

Native linear and logistic backends run in-process in Rust. Their feature count, exact order,
semantic digests, normalizers, finite values, dataset identity, decision thresholds, and bundle
coordinate are checked before a result exists.

A successful new admission reports:

```json
{
  "modelId": "...",
  "bundleId": "...",
  "bundleVersion": 1,
  "disposition": "inserted",
  "metadataSha256": "...",
  "artifactSha256": "...",
  "trainingRunSha256": "...",
  "authoritySha256": "...",
  "datasetSelectionSha256": "..."
}
```

Retain this result with the candidate receipt, admission request, independent authority, dataset
receipt, training release evidence, and exact application identity.

## Admit an ONNX bundle

The same admission sequence applies to an already validated ONNX candidate, with this backend:

```json
{
  "kind": "onnx",
  "modelSha256": "<exact-onnx-artifact-sha256>",
  "opset": 18,
  "inputShape": [1, 16],
  "outputShape": [1],
  "outputSemantics": "regression",
  "inferenceDeadlineMillis": 1000,
  "fallback": "no_action"
}
```

The shapes and opset above are examples. They must match the exact graph and bundle. The fixed
admission policy requires:

- a self-contained ONNX protobuf of at most 64 MiB;
- exactly one static float input and one static scalar output;
- rank 1–8, positive static dimensions, and at most 1,000,000 input-plus-output elements;
- one standard ONNX opset from 13 through 24;
- at most 1,024 executable nodes and 256 declared tensors;
- a nonzero deadline of at most 5 seconds;
- `fallback: "no_action"`; and
- only these operators: `Add`, `Cast`, `Clip`, `Concat`, `Div`, `Gather`, `Gemm`, `Identity`,
  `MatMul`, `Mul`, `ReduceMean`, `Relu`, `Reshape`, `Sigmoid`, `Softmax`, `Sqrt`, `Sub`, `Tanh`,
  and `Transpose`.

External tensor data, custom domains, dynamic/absent shapes, nonfinite tensor values, training
state, sparse initializers, functions, and nested graphs are rejected. The worker independently
limits aggregate intermediate elements, intermediate tensors, and compute to 50,000,000 units.

### Sibling worker requirement

The sealed builder installs `market-squawk-onnx-worker` beside the exact signed
`market-squawk` executable. At process startup, the application:

1. verifies that the running application and sibling worker are the canonical installed release
   paths;
2. rejects a symlink or non-regular helper and enforces a 256 MiB ceiling;
3. hashes the unchanged helper in two passes and compares it with the signed release-manifest
   digest;
4. copies it to a private content-addressed temporary generation;
5. rechecks the copied digest and invokes it by absolute path, never `PATH`;
6. starts one worker generation while constructing each ONNX backend; and
7. preflights, compiles, and warms the exact model before publishing that backend.

The admitted helper remains running and handles bounded inference requests. It is not spawned once
per event or prediction. When a training release is configured, a missing or mismatched signed
helper fails startup even for an empty or native-only model inventory; this preserves the complete
installed release identity before any durable admission is opened.

With an exact conforming candidate and helper already present, admit it using the same command:

```bash
"$TRAINING_RELEASE_ROOT/bin/market-squawk" \
  --data-dir "$DATA_ROOT" \
  --training-release-root "$TRAINING_RELEASE_ROOT" \
  --output json \
  model admit /absolute/path/to/onnx-admission.json \
  --confirm
```

The current product builds `TractOnnxBackend` for this request. It does not select the optional
external ONNX Runtime path described later.

## Inspect admitted models

List every admitted immutable generation:

```bash
"$TRAINING_RELEASE_ROOT/bin/market-squawk" \
  --data-dir "$DATA_ROOT" \
  --training-release-root "$TRAINING_RELEASE_ROOT" \
  --output json \
  model list
```

Each summary includes model/bundle identity, metadata and artifact hashes, format/version, training
dataset manifest, and `no_action` fallback.

Inspect one model:

```bash
"$TRAINING_RELEASE_ROOT/bin/market-squawk" \
  --data-dir "$DATA_ROOT" \
  --training-release-root "$TRAINING_RELEASE_ROOT" \
  --output json \
  model metadata <MODEL_UUID>
```

`model metadata` selects the highest admitted bundle version for that model. It includes feature
order and semantic/input-schema digests, normalizers, complete training dataset/export/selection
identity, universe, training period, label, training code and environment, metrics, thresholds,
intended use, limitations, and fallback.

Use `model list` and retained admission receipts when the exact older bundle coordinate matters.
There is no CLI flag that makes `model metadata` select an earlier version.

## Predict and evaluate

Create a closed input JSON object:

```json
{
  "modelId": "018f3c2a-91ab-7ccd-b3de-123456789abc",
  "input": {
    "bundleId": "research-linear",
    "bundleVersion": 1,
    "featureValues": [0.25]
  }
}
```

`input` must contain exactly `bundleId`, `bundleVersion`, and `featureValues`. Supply 1–1,024 finite
numbers in the exact order reported by `model metadata`; the count must equal the admitted feature
count. The model UUID and exact bundle coordinate must already exist.

Predict without retaining evaluation evidence:

```bash
"$TRAINING_RELEASE_ROOT/bin/market-squawk" \
  --data-dir "$DATA_ROOT" \
  --training-release-root "$TRAINING_RELEASE_ROOT" \
  --output json \
  model predict /absolute/path/to/model-input.json
```

Evaluate the same inference and retain its bounded process-local evidence:

```bash
"$TRAINING_RELEASE_ROOT/bin/market-squawk" \
  --data-dir "$DATA_ROOT" \
  --training-release-root "$TRAINING_RELEASE_ROOT" \
  --output json \
  model evaluate /absolute/path/to/model-input.json \
  --confirm
```

Both results bind the exact model/bundle, training dataset manifest, feature semantic digests,
finite score, confidence, and decision (`negative`, `no_action`, or `positive`). They also state:

```json
{
  "executionAuthority": "none",
  "inferenceFailureBehavior": "no_action"
}
```

Evaluation additionally returns admitted validation metrics and:

```json
{
  "evaluationEvidence": {
    "sequence": 1,
    "digest": "<sha256>",
    "retention": "bounded_process_local"
  }
}
```

Local composition retains at most 4,096 such evaluation records in a process-local ring. They do
not survive restart and there is no read command for the opaque retained records at this commit.
Preserve the returned receipt externally when it is required as audit evidence.

## Runtime and no-action behavior

At startup and after every admission, the production runtime reconstructs the complete durable
generation set before publishing any model service state:

- the canonical two-copy index is bounded to 64 generations and 7 MiB under standard composition;
- the model registry is bounded to 512 MiB;
- each dataset verification is bounded to 100,000 rows and 256 MiB;
- aggregate validation has a standard 30-second window and rejects any configured window over
  60 seconds; and
- one corrupt record, missing required worker, dataset mismatch, backend failure, or deadline
  rejects the complete proposed runtime rather than publishing a partial registry.

Native backends stay in process. Each tract ONNX backend owns the worker generation it started
during construction. An inference request has one admitted deadline. A late response, protocol
failure, nonfinite result, helper failure, unavailable backend, cancellation, or request mismatch
produces no model output and therefore no action.

Before a helper is spawned, a globally bounded cleanup owner is reserved. If a dispatched request
fails, the parent requests termination and transfers the child plus unfinished I/O threads to that
owner. The inference caller does not block waiting for cleanup. If termination cannot be confirmed,
the disposition remains uncertain and no fallback execution is authorized.

Model results cannot emit an order directly. A consumer must bind the exact model identities and
then pass the resulting signal through the normal strategy and sole risk authority. Failure,
uncertainty, or absence remains `no_action`.

## Optional external ONNX Runtime evidence

The modeling library contains an optional `onnx-runtime` CPU acceleration implementation in
addition to required tract. It pins ONNX Runtime 1.24.4 for Linux arm64 and x86_64 ELF targets,
loads a descriptor-verified shared library from a sealed memory file, and permits tract fallback
only when the external request was never dispatched or helper termination is confirmed.

This is **not wired into current `market-squawk` product composition**. The product's ONNX admission
path always constructs `TractOnnxBackend`; it does not read the environment values below or select
the external backend. The verification procedure is therefore useful only for library integration
or retained release evidence. It does not enable optional acceleration in the shipping CLI.

For that limited purpose, use a dedicated local root:

```text
/absolute/path/onnx-runtime-root/
├── lib/libonnxruntime.so
├── LICENSE
└── admission.json
```

Configure exact paths and the independently recorded digest:

```bash
export MARKET_SQUAWK_ONNX_RUNTIME_ROOT=/absolute/path/onnx-runtime-root
export MARKET_SQUAWK_ONNX_RUNTIME="$MARKET_SQUAWK_ONNX_RUNTIME_ROOT/lib/libonnxruntime.so"
export MARKET_SQUAWK_ONNX_RUNTIME_EVIDENCE="$MARKET_SQUAWK_ONNX_RUNTIME_ROOT/admission.json"
export MARKET_SQUAWK_ONNX_RUNTIME_SHA256=<64-lowercase-hex-digest>

python3 scripts/verify_onnx_runtime.py \
  --policy docs/verification/onnx-runtime-policy.json \
  --library "$MARKET_SQUAWK_ONNX_RUNTIME"
```

For an exact release candidate, bind the report to the frozen Git commit and tree:

```bash
python3 scripts/verify_onnx_runtime.py \
  --head "$HEAD_SHA" \
  --tree "$TREE_SHA" \
  --policy docs/verification/onnx-runtime-policy.json \
  --library "$MARKET_SQUAWK_ONNX_RUNTIME" \
  --report "$MARKET_SQUAWK_ONNX_RUNTIME_EVIDENCE"
```

The verifier performs bounded local reads, checks SHA-256, ELF architecture, exact runtime version
through `OrtGetApiBase`, tracked policy, and notice identity, then writes canonical evidence. It
does not fetch, install, or modify the supplied library.

Replacing the library, platform, policy, notice, evidence, model, or helper invalidates the
previous tuple. Retain the [ONNX Runtime notice](../licenses/onnx-runtime-notice.md) if the optional
library is distributed. Always retain the [tract notice](../licenses/tract-onnx-notice.md) when
shipping required ONNX support.

## Success evidence

A successful admission has all of:

1. Exit status `0`.
2. `disposition: "inserted"` for a new immutable generation, or `already_admitted` only for a
   byte-identical, fully revalidated replay.
3. Model, bundle, version, metadata, artifact, training-run, authority, and dataset-selection
   values matching the accepted candidate/authority receipts.
4. `model list` containing that exact bundle after a fresh process start.
5. `model metadata <MODEL>` showing the expected generation when it is the newest version.
6. A bounded prediction using the exact feature count/order and bundle coordinate.
7. Output with finite score/confidence, exact training/feature identity,
   `executionAuthority: "none"`, and `inferenceFailureBehavior: "no_action"`.
8. For ONNX, successful process restart with the exact sibling worker re-admitted and the graph
   compiled/warmed before model inventory becomes available.

An evaluation receipt is additional process-local evidence, not a replacement for admission or
restart proof.

## Idempotency, rollback, and recovery

An exact re-admission revalidates every authority and returns
`disposition: "already_admitted"`. The same bundle ID/version with different bytes, a model ID
mapped to a different bundle series, a bundle series mapped to a different model, or reuse of a
candidate directory is an immutable conflict.

There is no CLI de-admit, delete, or “set current” operation:

- A failed admission leaves the current durable and in-process runtime unchanged. Correct the
  source candidate/evidence and use a new immutable coordinate; do not patch an existing candidate.
- A previously admitted bundle remains addressable for prediction by exact model ID, bundle ID,
  and bundle version even after a newer version exists. `model metadata` still selects the newest.
- If a newer bundle is unacceptable, stop consumers from selecting it and return them to the exact
  prior coordinate. There is no registry mutation that makes the prior version newest.
- If startup requires a missing training release, restore the exact signed release and matching
  application identity. Do not erase the durable model index to obtain an empty inventory.
- If an ONNX helper is missing or changed, restore the exact admitted application/helper package
  before startup. A new helper identity requires a new reviewed release/admission path.
- For catalog, artifact, or control-state loss, stop writers and follow
  [Backup and recovery](backup-and-recovery.md). Candidate artifacts, catalog state, model control
  state, training release, authority documents, application/helper binaries, and configuration are
  one recovery evidence set even though they occupy separate roots.

## Failure modes

| Symptom | Meaning | Safe response |
| --- | --- | --- |
| `production model runtime is not configured` | No verified training release was selected for admission | Configure the exact signed release and restart the command |
| Durable admissions require the signed release | Existing model control state cannot truthfully be reconstructed without it | Restore/configure the exact release; do not delete the index |
| Request file/path rejected | It is oversized, unsafe, symlinked, changed, or not a regular file | Place a reviewed regular copy at one explicit safe path |
| Invalid SHA-256 or schema version | Request fields are not canonical lowercase SHA-256 or schema 1 | Recreate from exact receipts; never substitute or normalize an unknown digest |
| Authority path/file rejected | Path is not absolute, file is not bounded/no-follow, root overlaps data root, or bytes changed | Restore the exact independently reviewed authority under a disjoint controlled root |
| Candidate root unavailable | `candidateDirectory` escaped its grammar or does not exist below `artifacts` | Correct the relative coordinate; do not widen filesystem authority |
| Dataset admission mismatch | Export, as-of cutoff, selection, catalog identity, manifest, or rows differ | Restore the exact dataset/catalog evidence or train a new candidate |
| Training environment mismatch | Installed release, application, validator, ONNX worker, wheel, `RECORD`, signature, or foundation differs | Restore the exact signed release set; rebuild through the supported release path |
| Backend policy mismatch | Native/ONNX request does not match the bundle format or ONNX artifact digest | Correct the request from the accepted bundle receipt |
| ONNX policy rejection | Graph violates digest, opset, shape, operator, tensor, node, finite-value, or bound policy | Produce a new conforming graph and full candidate evidence; do not relax policy |
| Missing ONNX worker | The configured signed release is incomplete or the exact sibling helper is absent | Restore the complete signed release before reopening the model namespace |
| Prediction not found | Exact model/bundle/version is not admitted | Inspect `model list`; do not silently fall forward to another version |
| Invalid prediction input | Wrong closed fields, feature count/order, nonfinite value, or zero version | Recreate input from exact `model metadata` |
| Inference unavailable/deadline | Backend/helper failed, result was late, or termination was uncertain | Treat as `no_action`; inspect and restore the exact runtime before retry |
| Evaluation evidence disappeared after restart | Retention is deliberately process-local | Use the externally retained command result; do not claim durable evaluation storage |

## Local state locations

All relative paths below are under the selected data root:

| Path | Purpose | Operator rule |
| --- | --- | --- |
| `artifacts/models/<generation>/candidate/` | Exact candidate metadata, artifact, and training run | Immutable after validation/admission |
| `artifacts/objects/sha256/<first-two-hex>/<sha256>.parquet` | Exact training dataset objects | Never edit, rename, or delete manually |
| `catalog.sqlite3` and active SQLite sidecars | Dataset export/selection and catalog identity authority | Back up and restore as one consistency domain |
| `control/model/runtime-admissions/` | Canonical two-copy durable model runtime index | Application-owned; accepted recovery restores the exact retained authority set |

External but required evidence:

| Root | Purpose |
| --- | --- |
| `<training-release-root>` | Signed application, ONNX worker, validator, interpreter, package, manifests, signatures, and foundation |
| `<authority-root>` | Independent bundle authority, disjoint from the data root |
| Optional ONNX Runtime root | Library-level optional native runtime and evidence; not selected by current CLI |

## Related documentation, code, and evidence

Documentation:

- [Dataset build and query operations](datasets-and-query.md)
- [Configuration and secrets](configuration-and-secrets.md)
- [Backup and recovery](backup-and-recovery.md)
- [Troubleshooting](troubleshooting.md)
- [CLI reference](../reference/cli.md)
- [MCP reference](../reference/mcp.md)
- [Deployment architecture](../architecture/deployment.md)
- [Live execution plane](../architecture/live-execution-plane.md)
- [Security and trust boundaries](../architecture/security-and-trust-boundaries.md)
- [Mutable delivery ledger](../plans/delivery-ledger.md)
- [tract dependency notice](../licenses/tract-onnx-notice.md)
- [Optional ONNX Runtime notice](../licenses/onnx-runtime-notice.md)
- [Optional ONNX Runtime policy](../verification/onnx-runtime-policy.json)

Reviewed code:

- [`scripts/build_python_release.py`](../../scripts/build_python_release.py)
- [`python/market_squawk/data.py`](../../python/market_squawk/data.py)
- [`python/market_squawk/training.py`](../../python/market_squawk/training.py)
- [`python/market_squawk/bundle.py`](../../python/market_squawk/bundle.py)
- [`apps/market-squawk/src/local_product/cli_model.rs`](../../apps/market-squawk/src/local_product/cli_model.rs)
- [`apps/market-squawk/src/application/model.rs`](../../apps/market-squawk/src/application/model.rs)
- [`apps/market-squawk/src/application/model/runtime.rs`](../../apps/market-squawk/src/application/model/runtime.rs)
- [`crates/market-squawk-modeling/src/admission.rs`](../../crates/market-squawk-modeling/src/admission.rs)
- [`crates/market-squawk-modeling/src/native.rs`](../../crates/market-squawk-modeling/src/native.rs)
- [`crates/market-squawk-modeling/src/onnx.rs`](../../crates/market-squawk-modeling/src/onnx.rs)
- [`crates/market-squawk-modeling/src/onnx/policy.rs`](../../crates/market-squawk-modeling/src/onnx/policy.rs)
- [`crates/market-squawk-modeling/src/onnx/worker.rs`](../../crates/market-squawk-modeling/src/onnx/worker.rs)
- [`scripts/verify_onnx_runtime.py`](../../scripts/verify_onnx_runtime.py)

## External sources

Direct upstream sources were rechecked on 2026-07-23:

- [ONNX concepts and model structure](https://onnx.ai/onnx/intro/concepts.html)
- [tract ONNX repository](https://github.com/sonos/tract)
- [ONNX Runtime 1.24.4 release](https://github.com/microsoft/onnxruntime/releases/tag/v1.24.4)
- [ONNX Runtime MIT license](https://github.com/microsoft/onnxruntime/blob/v1.24.4/LICENSE)
- [Official ONNX Runtime shared-library build guidance](https://onnxruntime.ai/docs/build/inferencing.html)

Upstream documents define the interchange format and runtime projects. Market Squawk's exact
bundle schema, signed release, feature registry, graph allowlist, helper containment, deadlines,
admission index, and no-action rules remain the controlling production contract.
