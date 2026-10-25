// Copyright 2026 Adobe. All rights reserved.
// This file is licensed to you under the Apache License,
// Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
// or the MIT license (http://opensource.org/licenses/MIT),
// at your option.

// Unless required by applicable law or agreed to in writing,
// this software is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR REPRESENTATIONS OF ANY KIND, either express or
// implied. See the LICENSE-MIT and LICENSE-APACHE files for the
// specific language governing permissions and limitations under
// each license.

//! Host-driven, sans-I/O workflows for C2PA and CAWG crates.
//!
//! The host application owns I/O, concurrency, and scheduling, ideally in
//! its own language and runtime. A workflow exposes external operations
//! (files, networking, the clock, signing) through an explicit request/reply
//! protocol; it never performs I/O, blocks on a host operation, or spawns
//! threads itself.
//!
//! This is shared infrastructure for Content Authenticity's C2PA and CAWG
//! crates, not a general-purpose workflow framework. The core contains no
//! domain-specific validation or signing logic.
//!
//! Prefer [`CoroutineWorkflow`] for most workflows: write an `async fn` that
//! requests operations through [`Host`] and awaits the replies, and let the
//! compiler generate the state machine. The host still uses the synchronous
//! [`Workflow`] interface; no async runtime is required. Use
//! [`ManualWorkflowCore`] when you need an explicit, inspectable state machine.
//!
//! Concretely, this crate provides:
//!
//! * [`Request`] — the trait a workflow's own request vocabulary implements,
//!   relating each request kind to the reply that answers it.
//! * [`HostRequest`] and [`RequestId`] — the request envelope and its
//!   correlation ID.
//! * [`RequestTracker`] — bookkeeping for requests a workflow has issued but
//!   its host has not yet fulfilled.
//! * [`ProtocolError`] — the host-binding-is-buggy error vocabulary common
//!   to every workflow (an unknown request ID, a mismatched reply, driving a
//!   workflow past a terminal state).
//! * [`Step`] and [`Workflow`] — the `advance` / `outstanding_requests` /
//!   `fulfill` / `finish` interaction contract itself, as a trait so
//!   host-side code can be written once against any workflow built here.
//! * [`CoroutineWorkflow`], [`Host`], and [`PendingReply`] — the preferred way
//!   to implement a workflow: write an `async fn` that awaits its host's
//!   replies, and let the compiler generate the state machine. No async
//!   runtime is involved.
//! * [`ManualWorkflowCore`] — the alternative for a hand-written state
//!   machine: request tracking and terminal-state bookkeeping, leaving its
//!   explicit phases and transitions to the workflow author.
//! * [`Nested`] — a [`Workflow`] driven from inside a [`CoroutineWorkflow`], so a
//!   workflow that composes others can answer their requests itself or
//!   forward them to its own host from a plain `async` loop.
//!
//! A concrete workflow — reading and validating a C2PA manifest store or
//! generating and signing one — lives in its own crate built on top of
//! this one, with its own request vocabulary, settings, and result type.
//! See [`CoroutineWorkflow`] for a complete example of the preferred style,
//! or [`ManualWorkflowCore`] for a hand-written state machine.

#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::unwrap_used)]
#![deny(missing_docs)]
#![deny(unsafe_code)]

mod coroutine;
mod error;
mod nested;
mod recorder;
mod request;
#[cfg(test)]
mod test_workflow;
mod tracker;
mod types;
mod workflow;

pub use coroutine::{CoroutineWorkflow, Host, PendingReply};
pub use error::{Failure, ProtocolError};
pub use nested::Nested;
pub use recorder::{replay, Divergence, Event, Outcome, Recorder, Transcript};
pub use request::{HostRequest, Request};
pub use tracker::RequestTracker;
pub use types::RequestId;
pub use workflow::{ManualWorkflowCore, Step, Workflow};
