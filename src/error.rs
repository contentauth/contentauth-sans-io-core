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

//! The protocol-level error vocabulary common to every workflow built on
//! this engine.

use std::fmt;

use crate::types::RequestId;

/// A misuse of the [`Workflow`](crate::Workflow) API, as opposed to a failure
/// of the work the workflow carries out.
///
/// Every one of these but [`Self::WorkflowStalled`] and
/// [`Self::UnansweredRequest`] means the host binding
/// is buggy — fulfilling a request that was never issued or was already
/// answered, answering with the wrong payload type, or driving a workflow past a terminal state —
/// never that the data the workflow is working on is somehow wrong. A
/// concrete workflow's own `Error` type is expected to fold this in (for
/// example, via `#[from]`) alongside whatever domain-specific failures its
/// own logic can produce.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// The host fulfilled a request ID that is not outstanding in this
    /// workflow (never issued, or already fulfilled).
    #[error("no outstanding request with ID {0}")]
    UnknownRequest(RequestId),

    /// The host's reply payload does not match what the request asked for.
    #[error("reply payload does not match {id}: expected {expected}")]
    ReplyMismatch {
        /// ID of the request that was incorrectly fulfilled.
        id: RequestId,

        /// Description of the reply payload the request expects.
        expected: &'static str,
    },

    /// The workflow has already reached a terminal state; it cannot be
    /// advanced and no further requests can be fulfilled.
    #[error("workflow is already complete")]
    WorkflowComplete,

    /// The workflow's result was requested before the workflow reached a
    /// terminal state.
    #[error("workflow has not reached a terminal state")]
    WorkflowNotComplete,

    /// The workflow ended in an error on an earlier call and cannot be used
    /// further.
    ///
    /// A failed workflow typically leaves its internal state
    /// half-built, so handing anything out as if it were a result would be
    /// misleading. Workflows built with [`ManualWorkflowCore`](crate::ManualWorkflowCore)
    /// poison themselves on error: the original error is returned when it
    /// happens, and every later call reports this instead — carrying the
    /// phase the workflow failed in and the original error's message, so a
    /// host that only sees this later report still learns where and why.
    #[error("{0}")]
    WorkflowFailed(Failure),

    /// A [`CoroutineWorkflow`](crate::CoroutineWorkflow) is waiting, yet has nothing
    /// outstanding that the host could answer to unblock it.
    ///
    /// This is a bug in the workflow rather than its host: it awaited
    /// something other than a [`PendingReply`](crate::PendingReply).
    #[error("workflow is waiting on something other than its host")]
    WorkflowStalled,

    /// A workflow took a request from a [`Nested`](crate::Nested) workflow
    /// and asked for the next one without answering it, leaving the inner
    /// workflow with nothing that could ever unblock it.
    ///
    /// Like [`Self::WorkflowStalled`], a bug in the workflow, not its host.
    #[error("nested request {0} was taken but never answered or forwarded")]
    UnansweredRequest(RequestId),
}

/// Where and why a workflow failed, as remembered by
/// [`ManualWorkflowCore::mark_failed`](crate::ManualWorkflowCore::mark_failed) and
/// reported by [`ProtocolError::WorkflowFailed`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct Failure {
    /// The phase the workflow was in when it failed, as reported by
    /// [`Workflow::phase`](crate::Workflow::phase). Empty if unknown.
    pub phase: String,

    /// The message of the error that ended the workflow. Empty if unknown.
    pub cause: String,
}

impl Failure {
    /// Records a failure in `phase` caused by `cause`.
    pub fn new(phase: impl Into<String>, cause: &dyn fmt::Display) -> Self {
        Self {
            phase: phase.into(),
            cause: cause.to_string(),
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("workflow failed on an earlier call")?;
        if !self.phase.is_empty() {
            write!(f, ", in {}", self.phase)?;
        }
        if !self.cause.is_empty() {
            write!(f, ": {}", self.cause)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_name_the_request_they_concern() {
        assert_eq!(
            ProtocolError::UnknownRequest(RequestId(3)).to_string(),
            "no outstanding request with ID request #3"
        );
        assert_eq!(
            ProtocolError::ReplyMismatch {
                id: RequestId(1),
                expected: "Widget",
            }
            .to_string(),
            "reply payload does not match request #1: expected Widget"
        );
    }

    #[test]
    fn terminal_state_messages_use_workflow_terminology() {
        assert_eq!(
            ProtocolError::WorkflowComplete.to_string(),
            "workflow is already complete"
        );
        assert_eq!(
            ProtocolError::WorkflowNotComplete.to_string(),
            "workflow has not reached a terminal state"
        );
    }

    #[test]
    fn workflow_failed_reports_where_and_why_when_known() {
        assert_eq!(
            ProtocolError::WorkflowFailed(Failure::default()).to_string(),
            "workflow failed on an earlier call"
        );
        assert_eq!(
            ProtocolError::WorkflowFailed(Failure::new("Reading > AwaitingOcsp", &"revoked"))
                .to_string(),
            "workflow failed on an earlier call, in Reading > AwaitingOcsp: revoked"
        );
        assert_eq!(
            ProtocolError::WorkflowFailed(Failure::new("", &"revoked")).to_string(),
            "workflow failed on an earlier call: revoked"
        );
    }
}
