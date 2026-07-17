# Authority lifecycle model checking

Market Squawk model-checks the production packed authority-session lifecycle kernel with
[Loom 0.7.2](https://docs.rs/loom/0.7.2/loom/). The model uses the same private
`LifecycleWord` transition implementation as the production `AtomicU64`; only the atomic adapter,
shared pointer, threads, and test-only writer counter are replaced with Loom types.

Run the checked-in gate from the repository root:

```bash
./scripts/check_authority_lifecycle_loom.sh
```

The script compiles every `market-squawk-sources` target under `cfg(loom)` with strict Clippy and
then runs the lifecycle model in release mode on one test runner thread. `scripts/verify.sh` invokes
the same gate, so the model cannot silently disappear from the documented release verification.

## Model boundary

The model has four Loom threads in total: the model runner and three racing actors for admitted
terminalization, independent fatal terminalization, and clean close. It permits at most 1,000
branches per execution and explores schedules with at most two preemptions. It proves the modeled
invariants within that bounded state space:

- clean close and terminal persistence cannot both win;
- a terminal transition has at most one writer;
- every admitted operation is released before the modeled execution ends; and
- the final phase is either `TerminalPersisted` or `Closed`.

This model supplements deterministic concurrency tests and property tests. It is not a proof of
the complete persistence system: mutexes, durable store I/O, registry reconciliation, and runtime
composition are covered by their deterministic tests rather than this atomic kernel model.

Loom also does not model every execution allowed by the full C11 memory model. In particular, its
[documented unsupported features](https://github.com/tokio-rs/loom/blob/loom-0.7.2/README.md#unsupported-features)
include load-buffering behaviors, and Loom replacement types must be used for an operation to enter
the model. The bounded passing result must therefore never be described as exhaustive proof of all
hardware executions or of synchronization outside `LifecycleWord`.
