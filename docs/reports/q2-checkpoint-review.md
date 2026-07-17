# Quarter 2 Tasks 5–8 checkpoint review and remediation ledger

<!-- q2-checkpoint-state
candidate-id: q2-integrated-remediation-2026-07-16
audit-anchor: 651a01e120dfe27a598b9475296733d238d870b7
review-target: repository-head
lifecycle: remediation-in-progress
prior-r01-r15: closed-as-framed
active-findings: Q2-I01,Q2-I02,Q2-I03,Q2-I04,Q2-I05,Q2-I06,Q2-I07,Q2-I08,Q2-I09,Q2-I10,Q2-I11,Q2-M01,Q2-M02
-->

## Document control

- Review date: 2026-07-16
- Exact rejected checkpoint: `581d4fdfcc44e04812dcfc35232a335ca0b592a8`
- Scope: Tasks 5–8 source authority, capture, live processing, deterministic sharding, snapshots,
  runtime lifecycle, application isolation, verification, and prohibited-capability review
- Reviewers: two independent read-only review lanes with separate authority/lifecycle and
  routing/memory/snapshot threat models
- Disposition: **rejected; remediation and fresh review required**

Lane completion and focused green tests did not satisfy the quarter checkpoint. Both reviewers
examined the exact integrated commit and rejected it. No Critical execution-authority bypass was
substantiated, but the union contains production-significant Important findings and Minor
hardening findings. This ledger is append-only: remediation closes findings with code, tests, and
replacement-commit evidence rather than deleting the original result.

## Deterministic gate findings

The exact rejected commit failed three independent parts of `scripts/verify.sh`:

1. stale line-pinned legacy-brand allowances;
2. a public rustdoc link to a private constant under `RUSTDOCFLAGS=-D warnings`; and
3. a CLI smoke identity that predated the explicit diagnostic-boundary wording.

These failures were corrected in `bbe1cb4` and `c0c9d67`. The candidate then passed the complete
workspace verification wrapper. Security hardening commit `20ad084` added and tested:

- exact-versioned internal workspace path dependencies;
- a fail-closed generated-artifact checker;
- a Cargo-deny policy covering all features and six Tier 1 target triples;
- denied unknown registries/Git sources, wildcard dependencies, native TLS/OpenSSL, and
  OpenTelemetry crates;
- explicit reviewed permissive-license policy and narrow crate/version exceptions;
- full RustSec advisory and `cargo audit --deny warnings` gates; and
- Gitleaks working-tree and 129-commit history scans with only generated build/worktree/scratch
  paths excluded and one exact rule/path/line historical prose false positive allowed.

The audit-policy candidate passed `./scripts/verify.sh`, `cargo deny check`,
`cargo audit --deny warnings`, working-tree Gitleaks, history Gitleaks, and `git diff --check` before
commit. All gates will be rerun against the final replacement commit; pre-commit candidate evidence
is not treated as final exact-head evidence.

## Production findings ledger

| ID | Severity | Finding | Required closure |
| --- | --- | --- | --- |
| Q2-R01 | Important | Nested order-book/change allocations are omitted from decoded/current-batch retained-byte admission. | Central recursive closed-shape accounting, normalized capacity semantics, all-payload tests, and an admission boundary regression. |
| Q2-R02 | Important | Future-dated caller-authored health evidence can qualify authority before observation and poison the health cursor. | Registry/supervisor-sealed trusted time, lower and upper temporal bounds, atomic rejection, restore and honest-successor tests. |
| Q2-R03 | Important | Budget cooldown, refusal, disable, poison, or terminal overflow does not synchronously revoke current health/source authority. | Exact-allocation-derived budget health, observer invalidation, old-lease rejection, and recovery through a new epoch. |
| Q2-R04 | Important | Provider/account budgets are shared only inside one registry instance. | One process/composition-authoritative coordinator, atomic restore merge/conflict behavior, and cross-registry concurrency/cooldown tests. |
| Q2-R05 | Important | Caller-selected budget account labels are not bound to audited authorization identity and can split one real quota. | Derive provider/account scope from authorization mode/basis; reject missing, extra, or invented aliases; test public and user-authorized modes. |
| Q2-R06 | Important | A recoverable first-observation rejection can retain incomplete provenance and later make snapshot publication actor-fatal. | Truthful rejected provenance or exclusion from committed state, observable quarantine, two-route survival, and immediate clean-shutdown tests. |
| Q2-R07 | Important | Startup memory charges one book per route while a processor can own many stream books. | One validated stream limit shared by processor/config/snapshot/estimator and full per-stream persistent-state accounting. |
| Q2-R08 | Important | Snapshot peak accounting omits simultaneous seed and final DTO construction plus scratch while the current generation remains published. | Prefer one final construction representation or charge every overlapping scaling term; deterministic structural and concurrent-shard tests. |
| Q2-R09 | Important | A continuously ready snapshot timer is ahead of both data/control queues in biased selection and can starve them. | Coalesced pending timer work with bounded scheduling fairness and paused-time control/data/cancellation regressions. |
| Q2-R10 | Important | Public snapshot `Deserialize` bypasses private-field constructors, collection bounds, and relational invariants. | Make runtime snapshots output-only or implement bounded private wire visitors and checked reconstruction; compile-time or adversarial decode tests. |
| Q2-R11 | Important | Capture shutdown can return after detaching an uninterruptibly blocked OS writer thread. | Preserve a reapable supervised timeout state, separate authority revocation from worker termination, fence replacement, and gate append/flush tests. |
| Q2-R12 | Minor | Epoch-overflow registration mutates the budget pool before returning failure. | Move all fallible work before mutation or use transactional rollback; exact before/after state regression. |
| Q2-R13 | Minor | Aggregate snapshot reads can be configured permanently impossible when reader permits are below shard count. | Reject the configuration because aggregate reads are a required control-plane capability; below/exact/contention tests. |
| Q2-R14 | Minor | CI has no Windows coverage for platform-sensitive path, journal, lock, and capture behavior. | Pinned Linux/macOS/Windows jobs, full Linux gate, and locked platform-sensitive cross-platform checks. |
| Q2-R15 | Hardening | Snapshot event-budget wording implies an exact bound although one batch can overshoot it. | Make cadence exact without breaking batch atomicity, or expose truthful bounded trigger semantics and regression evidence. |

The false memory bounds, current-health authority gap, budget-scope multiplication paths, detached
writer, and unchecked snapshot deserialization invalidate production-readiness claims until code
and tests close them. They are not accepted as documentation-only limitations.

## Parallel remediation ownership

- Source lane: Q2-R01–R05 and Q2-R12.
- Live lane: Q2-R06–R10, Q2-R13, and Q2-R15.
- Platform lane: Q2-R11 and Q2-R14.
- Root integration: dependency/audit policy, claim correction, controlled cherry-picks, full exact
  verification/audits, and two fresh independent re-reviews.

The lanes work in isolated worktrees from `20ad084`; none may use the shared root as an unreviewed
merge surface. Every lane is test-first and must report exact commit and command evidence.

## Prohibited-capability review

Neither reviewer found identity/account rotation, TLS/browser fingerprint concealment, CAPTCHA or
anti-bot bypass, blocking-evasion proxy rotation, or distributed quota-evasion machinery. However,
Q2-R04 and Q2-R05 prevent certification of the stronger structural “quotas cannot be multiplied”
claim until the process-wide coordinator and audited account binding are implemented. The remedy is
one authoritative identity and budget scope, never rotation or circumvention.

## Persisted tool references

- [Cargo-deny checks and configuration](https://embarkstudios.github.io/cargo-deny/)
- [RustSec advisory database](https://github.com/RustSec/advisory-db)
- [Gitleaks configuration and allowlist model](https://github.com/gitleaks/gitleaks#configuration)
- [GitHub Actions security hardening](https://docs.github.com/en/actions/security-for-github-actions/security-guides/security-hardening-for-github-actions)

Final approval requires a clean replacement commit, the complete locked workspace/release/doc
gate, dependency/license/advisory/credential/generated-artifact audits, remediation tests for every
ledger item, and two fresh independent reviews with no unresolved severity.

## Integrated replacement review at `651a01e` (2026-07-16)

The original Q2-R01–R15 findings were substantively closed and integrated through exact commit
`651a01e120dfe27a598b9475296733d238d870b7`. That commit passed the complete local verification
wrapper, Cargo-deny, Cargo-audit, working-tree and history Gitleaks scans, brand and generated-
artifact checks, and clean/unchanged exact-head assertions. Hosted macOS and Windows results were
not observed and are not claimed.

Q2-R01–R15 are closed as framed at `651a01e`; the table above is retained as append-only historical
evidence and does not describe the active defect set.

Three fresh independent read-only reviewers examined that same frozen commit across source
authority/persistence, concurrency/memory/lifecycle, and architecture/security/documentation. The
replacement checkpoint was **rejected** with zero Critical, eleven Important, and three Minor
reports, deduplicated into thirteen remediation contracts:

| ID | Severity | Finding |
| --- | --- | --- |
| Q2-I01 | Important | Health-epoch exhaustion preserves the prior executable session authority. |
| Q2-I02 | Important | Provider/account budget identity remains caller-aliasable instead of registry-canonical. |
| Q2-I03 | Important | A fresh process can reset provider-budget enforcement state. |
| Q2-I04 | Important | Raw transport receive time is adapter-authored but treated as trusted freshness evidence. |
| Q2-I05 | Important | The registry clock does not retain and enforce a wall high-water. |
| Q2-I06 | Important | Live runtime memory omits simultaneously reachable order-book processing allocations. |
| Q2-I07 | Important | Snapshot reader/publication/generation metadata scales outside the runtime estimate. |
| Q2-I08 | Important | Capture admission omits complete session identity and uniquely retained generation/bundle allocations. |
| Q2-I09 | Important | Application shutdown can wait forever for a non-cooperative or backpressured source task. |
| Q2-I10 | Important | MCP checks its 1 MiB line limit only after the complete line has been allocated. |
| Q2-I11 | Important | Authoritative architecture/gap/plan/checkpoint/progress documents describe conflicting candidates and status. |
| Q2-M01 | Minor | Persisted provider-budget policies are emitted in randomized map order. |
| Q2-M02 | Minor | Public diagnostic market wording can be mistaken for canonical execution-quality coverage. |

Q2-I08 incorporates the separate Minor raw-capture capacity undercount because both require one
closed capture object-graph accounting contract. No reviewer found an execution-authority bypass,
provider-access evasion implementation, hot-path analytical I/O, or reopening of an original
R01–R15 defect as framed.

The controlling design and TDD/DAG plan are:

- `docs/superpowers/specs/2026-07-16-q2-integrated-checkpoint-remediation-design.md`
- `docs/superpowers/plans/2026-07-16-q2-integrated-checkpoint-remediation.md`

The current disposition remains **rejected; integrated remediation and exact-head re-review are
required**. No Q2 approval is implied by focused lane tests or the earlier green gate.
