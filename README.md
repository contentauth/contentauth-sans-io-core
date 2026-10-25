# contentauth-sans-io-core

[![CI](https://github.com/contentauth/contentauth-sans-io-core/actions/workflows/ci.yml/badge.svg)](https://github.com/contentauth/contentauth-sans-io-core/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/contentauth/contentauth-sans-io-core/branch/main/graph/badge.svg)](https://codecov.io/gh/contentauth/contentauth-sans-io-core)
[![crates.io](https://img.shields.io/crates/v/contentauth-sans-io-core.svg)](https://crates.io/crates/contentauth-sans-io-core)
[![Documentation](https://docs.rs/contentauth-sans-io-core/badge.svg)](https://docs.rs/contentauth-sans-io-core)

An experimental design for host-driven, sans-I/O workflows for the C2PA and
CAWG SDKs, leaving I/O, concurrency, and scheduling to the host application.

The goal is to let a library describe work without choosing how files,
networking, time, or signing are provided, or which threads or async runtime
execute that work. Those decisions belong to the host, ideally in the
application's own language and runtime. A workflow exposes external
operations through an explicit request/reply protocol rather than
performing I/O, blocking on a host operation, or spawning threads itself.
The host may be synchronous, asynchronous, single-threaded, or multi-threaded.

This crate is shared infrastructure for Content Authenticity's C2PA and
CAWG crates, not a general-purpose state-machine or workflow framework.
It contains no domain-specific validation or signing logic: concrete
workflows live in their own crates and define their own request
vocabularies, settings, and results.

The API is experimental and may change between releases.

## Usage

Add the crate to your project:

```toml
[dependencies]
contentauth-sans-io-core = "0.1"
```

Implement `Request` for your operation vocabulary, write your workflow as an
`async fn` that asks its `Host` for replies, and pass it to
`CoroutineWorkflow::new`. This is the preferred approach for most workflows:
the compiler manages the state machine, and the host still drives it through
the synchronous `Workflow` interface without an async runtime.

Use `ManualWorkflowCore` instead when you need an explicit, inspectable state
machine. See the [API documentation](https://docs.rs/contentauth-sans-io-core)
for complete examples of both approaches.

## Interaction contract

When a workflow cannot make further progress without a host operation, it
yields control and reports the operations it needs its host to perform. In
the coroutine style, an `.await` may suspend the workflow, but `advance`
returns `Step::AwaitHost` rather than blocking the host's thread waiting for
a reply. The host remains free to do other work, service those operations —
concurrently and in any order, if it likes — and report each outcome back.
Calling `advance` again lets the workflow consume whatever outcomes have
arrived and make further progress.

```text
             ┌────────────────────────────────────────────────┐
             │                      HOST                      │
             │   owns files, the network, keys, the clock,    │
             │            and all async scheduling            │
             └────┬───────────────────────▲───────────────────┘
        advance() │                       │ fulfill(id, reply)
                  ▼                       │
             ┌────────────────────────────────────────────────┐
             │                    WORKFLOW                    │
             │  host-driven progress + request/reply tracking │
             └────────────────────────────────────────────────┘
```

1. Create the workflow.
2. Call `advance`. It returns `Step::AwaitHost` when waiting on the host, or
   `Step::Complete` once the workflow has finished.
3. While `AwaitHost`: service any subset of `outstanding_requests` and
   report each outcome via `fulfill`, then call `advance` again. Outcomes
   may be reported in any order.
4. On `Complete`: consume the workflow with `finish` to obtain its result.

### Threading

A workflow is **not re-entrant**. `CoroutineWorkflow` uses internal synchronization
for the mailbox shared with its future; ordinary serialized host calls do
not contend on that lock. This does not lock the workflow's API or make
concurrent calls safe. Any host that employs a thread model must ensure
the workflow's methods are never called simultaneously from multiple threads (nor
re-entered from within a call in progress on the same workflow). Crashes or
unpredictable behavior are likely if this constraint is violated. Calls on
*different* workflows may safely run at the same time; only calls on the
*same* workflow must not. A workflow may also move to another thread between
calls. In safe Rust the
borrow checker does much of this for you (`advance` and `fulfill` take
`&mut self`, `finish` takes `self`), but hosts that share a workflow through
interior mutability or reach it through a raw-handle binding must enforce
it themselves.

See the `Workflow` trait in [`src/workflow.rs`](src/workflow.rs) for the
interaction contract and `CoroutineWorkflow` in
[`src/coroutine.rs`](src/coroutine.rs) for the recommended implementation.

## Implementing a workflow

`Workflow` is the common host-facing contract, not an implementation style.
Both styles below expose the same synchronous `advance` / `fulfill` /
`finish` interface; neither requires the host to use Rust async or a
particular runtime. Concrete domain workflows implement this trait in
their own crates.

### Recommended: coroutine-style `async fn`

Prefer `CoroutineWorkflow` ([`src/coroutine.rs`](src/coroutine.rs)) for most
workflows. The workflow asks its `Host` handle for what it needs and `.await`s
the reply; the compiler generates the state machine, including the locals,
loops, `?` early returns and sub-workflow calls a hand-written one would
have to encode. This avoids maintaining explicit transitions and repeated
reply-handling code as the workflow grows.

`advance` polls the future once with a no-op waker — there is no executor,
runtime, or background thread. `fulfill` notifies the reply's registered
waker so collections such as `FuturesUnordered` and `join_all` know which
children are ready, but only `advance` polls the workflow. Issuing several
requests before awaiting any puts them in front of the host at once.

The compiler's state is opaque, but a workflow names its current stage
with `Host::set_phase`, exposed through `Workflow::phase` and preserved in
failure reports. It must await only `PendingReply`s (or futures built from them).
Awaiting anything else would never be woken; `advance` reports that as
`ProtocolError::WorkflowStalled` instead of hanging.

`Host::ask(request, decode)` combines issuing, awaiting, and decoding one
reply; the decoder receives the request ID for vocabulary-specific errors
and host-failure policy. Use `Host::request` to issue multiple requests
before awaiting them.

When a `CoroutineWorkflow` completes or fails, it cancels remaining requests and
discards stored replies and wakers. Its outstanding list is then empty.
Hosts should stop servicing those requests and discard late results;
operations already performed are not undone. Dropping one `PendingReply`
while the workflow is still running does not cancel its request.

### Alternative: explicit state machine

Use `ManualWorkflowCore` ([`src/workflow.rs`](src/workflow.rs)) when you need
explicit, inspectable state. The workflow keeps a phase enum with one variant
per point where it waits on its host, each carrying every local that must
survive the wait, and `advance` matches on it. The helper handles request
tracking and lifecycle bookkeeping, but you write the transitions and
reply-handling paths yourself.

The distinction is how library logic is authored, not synchronous versus
asynchronous execution by the host. Both implementations are state
machines; the preferred coroutine style lets Rust's compiler generate one.

## Composing workflows

[`Nested`](src/nested.rs) drives any `Workflow` from a `CoroutineWorkflow`'s async
function, including hand-written state machines and other coroutine
workflows. Repeatedly await `nested.next()` to obtain an inner request,
then either answer it with `nested.fulfill(id, reply)` or forward it using
`nested.forward(id, host.request(outer_request), map_reply)`. Once `next`
returns `None`, `nested.finish()` returns the inner result.

The adapter handles request-ID translation and delivers whichever forwarded
replies are ready, so multiple requests remain concurrently outstanding and
the host may answer in any order. There is still no runtime or I/O inside
the engine. An unanswered inner request that cannot make progress produces
`ProtocolError::UnansweredRequest`; inner errors propagate to the caller.
`nested.phase()` exposes the inner diagnostic stage for the outer workflow.

The inner request vocabulary must implement `Clone`; forwarding's reply
mapper is a function pointer (a function or non-capturing closure).

## Debugging a workflow

A state machine that stops with an error can be hard to diagnose: by the
time the host sees the error, the workflow has moved on (or poisoned
itself), and the call stack that produced it is gone. Two things answer
"where did it go wrong, and why?":

* **Where it is, and where it failed.** `Workflow::phase` describes the
  workflow's current phase in words — `"Locating > Scanning > Marker at
  byte 2"`, `"Building > AwaitingSignature"` — with a workflow nested inside
  another named after the outer phase. A failed workflow keeps reporting the
  phase it failed in, and every later call returns
  `ProtocolError::WorkflowFailed(Failure)`, which carries that phase and the
  original error's text rather than a bare "failed earlier".
  `CoroutineWorkflow` records both automatically using the most recent
  `Host::set_phase` (initially `"Start"`). A hand-written workflow calls
  `ManualWorkflowCore::mark_failed(phase, &err)` on every error exit instead.
  Only the first failure is kept. Phase wording is for people and logs, not
  a stable interface: don't branch on it.
* **What the host said to it.** Since a workflow performs no I/O, its
  behavior depends on nothing but the replies it is given. Wrap a workflow
  in a `Recorder` — itself a `Workflow`, so the host does not change — and
  it keeps a `Transcript` of every `advance` (the phase, what it returned,
  the requests it issued) and every `fulfill` (the reply, and whether it
  was accepted). A transcript prints as a readable one-line-per-call log,
  and `replay` drives a fresh workflow through it, reproducing the run
  exactly — on another machine, under a debugger, or against a fix — or
  reporting the first call where the workflow behaved differently
  (`Divergence`). A transcript holds every reply in full, asset bytes
  included, so record only where its size and contents are acceptable.

Wrap your concrete workflow with `Recorder::new(workflow)`, drive it as
usual, and inspect `workflow.transcript()` when diagnosing a failure.

Synchronous hosts can use `workflow.respond(|request| answer(request))?`
after each `AwaitHost`. This borrows each request, obtains its owned reply,
and calls `fulfill` before servicing the next request: no request clones
or intermediate reply batch are needed. It stops on the first protocol
error, leaves earlier replies delivered, and does not call `advance`.
Host-operation failures are carried in the vocabulary's reply type.
Async hosts and hosts that service only a subset or choose another order
continue to use `outstanding_requests` / `fulfill` directly.

## Building

```sh
cargo test --all-features
cargo clippy --all-features --all-targets -- -Dwarnings
RUSTDOCFLAGS=-Dwarnings cargo doc --no-deps --all-features
```

Minimum supported Rust version: 1.96.0.

Code format uses pinned nightly rustfmt, including the separate fuzz harness:

```sh
rustup toolchain install nightly-2026-01-16 --component rustfmt
cargo +nightly-2026-01-16 fmt --all
cargo +nightly-2026-01-16 fmt --manifest-path fuzz/Cargo.toml
```

TOML formatting uses [Taplo](https://taplo.tamasfe.dev/) with
the repository's `.taplo.toml` configuration:

```sh
cargo install taplo-cli --locked
taplo fmt
taplo fmt --check
```

## Testing and fuzzing

The engine's bookkeeping is the one piece every workflow shares, so it is
tested adversarially. [`tests/support/model.rs`](tests/support/model.rs)
holds two *model-based* harnesses. Each takes a script of host calls —
including illegal ones: unknown or foreign IDs, wrong reply payloads,
replies after completion or failure — runs it against the real engine and
against a deliberately naive reference model, and asserts they agree after
every step:

* `run_core` drives `ManualWorkflowCore` directly, as a workflow would;
* `run_workflow` drives a complete `Workflow` through the public
  `advance` / `fulfill` / `outstanding_requests` / `finish` contract.

Two front ends feed them:

* [`tests/model_based.rs`](tests/model_based.rs) uses
  [`proptest`](https://docs.rs/proptest) for structured, shrinking
  generation. It runs in plain `cargo test`.
* [`fuzz/`](fuzz) uses [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz)
  (libFuzzer) for coverage-guided generation from raw bytes. It is a
  separate, unpublished package because it needs nightly. From the
  repository root, run:

  ```sh
  cargo install cargo-fuzz --locked --version 0.13.2
  cargo +nightly fuzz run workflow_core tests/corpus/workflow_core
  cargo +nightly fuzz run fan_out_workflow tests/corpus/fan_out_workflow
  ```

  The tracked seeds live in [`tests/corpus`](tests/corpus) so the packaged
  integration tests also have their fixtures. Fuzzing adds discoveries to
  the chosen corpus directory; commit only deliberate, minimized inputs.
  CI runs each target for 60 seconds on every PR and runs five-minute
  campaigns weekly and on manual dispatch. A crash
  artifact replays with `cargo +nightly fuzz run <target> <artifact>`;
  turn it into a named case in `seed_scripts` in `tests/model_based.rs`
  once fixed.

To measure coverage locally:

```sh
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --locked
cargo llvm-cov --all-features --tests --lcov --output-path lcov.info
```

CI tests stable Rust on Linux, macOS, and Windows, tests the minimum supported
Rust version, checks both default and `test-util` configurations, and builds
the library for `wasm32-unknown-unknown` and `wasm32-wasip2`. Formatting,
Clippy, documentation, dependency auditing, and a publish dry-run are also
checked. Integration and model-based tests are included in coverage.

The optional `test-util` feature exposes `RequestTracker::fulfill_unchecked`
for downstream tests of defensive reply validation. It is not intended for
production host implementations.

## Releases

This repository publishes only `contentauth-sans-io-core`. Release-plz
publishes eligible releases and opens or updates a release PR on pushes to
`main`. PR titles are checked with `commitlint-rs` and `.commitlintrc.yml`
so release notes and version bumps can be generated. Use an unscoped
`fix`, `feat`, `chore`, `ci`, or `docs` type, followed by a capitalized
description without a trailing period or space, for example
`ci: Verify TOML formatting`. Automated release and dependency-update
titles have explicit exceptions in the PR-title workflow.

To check a title locally:

```sh
cargo install commitlint-rs --locked
printf '%s\n' 'ci: Verify TOML formatting' | commitlint -g .commitlintrc.yml
```

Coverage uploads run only for same-repository PRs and trusted branch events;
fork PRs still run tests and generate coverage without secrets. Release
automation runs only on `main`; the fuzz harness is never published.

## License

Licensed under either the [Apache License, Version 2.0](LICENSE-APACHE) or
the [MIT license](LICENSE-MIT), at your option.
