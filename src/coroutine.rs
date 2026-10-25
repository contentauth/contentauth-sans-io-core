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

//! The preferred way to implement a workflow: write an `async fn`, so the
//! compiler — not a hand-written phase enum — generates its state machine.
//!
//! [`CoroutineWorkflow`] is a [`Workflow`] like any other: the host sees the same
//! `advance` / `outstanding_requests` / `fulfill` / `finish` contract, the
//! same request vocabulary, and the same [`RequestId`]s. Only the inside
//! differs. Instead of one enum variant per point where the workflow waits
//! on its host, each carrying every local that must survive the wait, the
//! workflow is an ordinary `async fn` that asks its [`Host`] for what it
//! needs and `.await`s the reply. Locals, loops, early returns via `?`,
//! borrows held across a wait, and calls into other `async fn`s (sub-
//! workflows) all work as they do in any Rust function.
//!
//! None of this needs an async runtime. A [`PendingReply`] is ready exactly
//! when the host has fulfilled its request. Reply wakers notify enclosing
//! combinators that their children are ready, without polling the workflow.
//! Only [`Workflow::advance`] polls the root future, with a no-op waker,
//! until every branch is waiting on the host — [`Step::AwaitHost`].
//!
//! A workflow must only `.await` [`PendingReply`]s (or futures built from
//! them, such as another workflow `async fn`). A future that waits on
//! anything else — a timer, a channel, an async runtime's I/O — would
//! never be woken, and [`Workflow::advance`] reports it as
//! [`ProtocolError::WorkflowStalled`] rather than hang.
//!
//! Call [`Host::set_phase`] before each stage to expose its name through
//! [`Workflow::phase`]. Errors retain that phase and their original message
//! in [`Failure`], including on later `advance`, `fulfill`, or `finish`
//! calls. [`crate::Recorder`] and [`crate::replay`] work unchanged.

use std::{
    fmt,
    future::Future,
    mem,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Waker},
};

use crate::{
    error::{Failure, ProtocolError},
    request::{HostRequest, Request},
    types::RequestId,
    workflow::{Step, Workflow},
};

/// Requests issued and replies received, shared between a [`CoroutineWorkflow`] and
/// the [`Host`] handle its future holds.
///
/// Kept apart from [`CoroutineWorkflow::outstanding`] so that issuing a request
/// never needs the workflow itself: a request issued from inside the future
/// lands here, and the workflow moves it into its own outstanding list —
/// which it can then lend out by reference — once the poll returns.
struct Mailbox<Req: Request> {
    phase: String,
    next_id: u64,
    issued: Vec<HostRequest<Req>>,
    replies: Vec<(RequestId, Req::Reply)>,
    wakers: Vec<(RequestId, Waker)>,
}

type Shared<Req> = Arc<Mutex<Mailbox<Req>>>;

/// Locks the mailbox. Poisoning cannot leave it inconsistent — every
/// critical section is a single push, pop, or increment — so a poisoned
/// lock is taken as is.
fn lock<Req: Request>(shared: &Shared<Req>) -> MutexGuard<'_, Mailbox<Req>> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The handle a workflow's `async fn` uses to ask its host for something.
///
/// Cheap to clone; every clone issues into the same workflow.
pub struct Host<Req: Request> {
    shared: Shared<Req>,
}

impl<Req: Request> Clone for Host<Req> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<Req: Request> fmt::Debug for Host<Req> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Host").finish_non_exhaustive()
    }
}

impl<Req: Request> Host<Req> {
    /// Issues a request, waits for its reply, and decodes it using `decode`.
    ///
    /// The decoder receives the request ID for diagnostics. The vocabulary
    /// owns reply decoding and host-failure policy; the engine does not
    /// assume a particular failure variant.
    ///
    /// Like an `async fn`, this issues nothing until first polled. To issue
    /// several requests immediately, use [`Self::request`] instead.
    pub async fn ask<T, E>(
        &self,
        request: Req,
        decode: impl FnOnce(RequestId, Req::Reply) -> Result<T, E>,
    ) -> Result<T, E> {
        let pending = self.request(request);
        let id = pending.id();
        decode(id, pending.await)
    }

    /// Names the current workflow phase for [`Workflow::phase`] and failure
    /// reports. Set this before entering a new stage, including before any
    /// fallible work in that stage. The initial phase is `"Start"`.
    ///
    /// This is diagnostic text, not a stable interface. Sub-workflows can
    /// include their enclosing stage using `"Outer > Inner"`.
    pub fn set_phase(&self, phase: impl Into<String>) {
        lock(&self.shared).phase = phase.into();
    }

    /// Issues a request and returns a future that resolves to the host's
    /// reply.
    ///
    /// The request is issued immediately, not when the future is first
    /// polled: issuing several before awaiting any of them is how a
    /// workflow puts several requests in front of its host at once.
    pub fn request(&self, kind: Req) -> PendingReply<Req> {
        let mut mailbox = lock(&self.shared);
        let id = RequestId(mailbox.next_id);
        mailbox.next_id += 1;
        mailbox.issued.push(HostRequest { id, kind });

        PendingReply {
            id,
            shared: Arc::clone(&self.shared),
        }
    }
}

/// A request a workflow has issued, as a future that resolves to the
/// host's reply once [`Workflow::fulfill`] has delivered it.
///
/// The reply has already passed [`Request::accepts`] for this request by
/// the time it resolves.
#[must_use = "a request whose reply is never awaited is still sent to the host"]
pub struct PendingReply<Req: Request> {
    id: RequestId,
    shared: Shared<Req>,
}

impl<Req: Request> PendingReply<Req> {
    /// The ID the host will see this request under — for a workflow's own
    /// error reports.
    pub fn id(&self) -> RequestId {
        self.id
    }
}

impl<Req: Request> fmt::Debug for PendingReply<Req> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingReply")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<Req: Request> Future for PendingReply<Req> {
    type Output = Req::Reply;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Req::Reply> {
        let mut mailbox = lock(&self.shared);
        match mailbox.replies.iter().position(|(id, _)| *id == self.id) {
            Some(index) => Poll::Ready(mailbox.replies.remove(index).1),
            None => {
                match mailbox.wakers.iter_mut().find(|(id, _)| *id == self.id) {
                    Some((_, waker)) => waker.clone_from(cx.waker()),
                    None => mailbox.wakers.push((self.id, cx.waker().clone())),
                }
                Poll::Pending
            }
        }
    }
}

impl<Req: Request> Drop for PendingReply<Req> {
    fn drop(&mut self) {
        let waker = {
            let mut mailbox = lock(&self.shared);
            mailbox
                .wakers
                .iter()
                .position(|(id, _)| *id == self.id)
                .map(|index| mailbox.wakers.swap_remove(index).1)
        };
        drop(waker);
    }
}

type BoxedWorkflow<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

enum State<T, E> {
    Running(BoxedWorkflow<T, E>),
    Complete(T),
    Failed(Failure),
}

/// A [`Workflow`] implemented as an `async fn` rather than a hand-written
/// state machine.
///
/// This is the preferred approach for most workflows: the compiler manages
/// state across waits, loops, and sub-workflow calls, avoiding hand-written
/// transitions and repeated reply-handling code. No async runtime is needed;
/// the host drives it through the synchronous [`Workflow`] interface. Use
/// [`ManualWorkflowCore`](crate::ManualWorkflowCore) instead when you need an
/// explicit, inspectable state machine.
///
/// On completion or failure, pending requests are cancelled: outstanding
/// requests, stored replies, and registered wakers are discarded. Hosts
/// should stop servicing them and discard any late results. This does not
/// undo host operations already performed. Dropping a single
/// [`PendingReply`] while running does not cancel its request.
///
/// A concrete workflow wraps one of these and delegates the
/// [`Workflow`] methods to it:
///
/// ```
/// use contentauth_sans_io_core::{
///     CoroutineWorkflow, Host, ProtocolError, Request, Step, Workflow,
/// };
///
/// #[derive(Debug)]
/// enum Ask {
///     Double(u32),
/// }
///
/// #[derive(Debug)]
/// enum Answer {
///     Doubled(u32),
/// }
///
/// impl Request for Ask {
///     type Reply = Answer;
///
///     fn expected_reply(&self) -> &'static str {
///         "Doubled"
///     }
///
///     fn accepts(&self, _: &Answer) -> bool {
///         true
///     }
/// }
///
/// // The whole workflow: ask for two numbers to be doubled at once, then
/// // add the results. No phase enum, no `take_reply`.
/// async fn sum_of_doubles(host: Host<Ask>, a: u32, b: u32) -> Result<u32, ProtocolError> {
///     let first = host.request(Ask::Double(a));
///     let second = host.request(Ask::Double(b));
///     let (Answer::Doubled(x), Answer::Doubled(y)) = (first.await, second.await);
///     Ok(x + y)
/// }
///
/// let mut workflow = CoroutineWorkflow::new(|host| sum_of_doubles(host, 2, 5));
/// assert_eq!(workflow.advance()?, Step::AwaitHost);
///
/// // Both requests are outstanding; answer them in reverse.
/// for request in workflow
///     .outstanding_requests()
///     .iter()
///     .rev()
///     .map(|r| {
///         (
///             r.id,
///             match r.kind {
///                 Ask::Double(n) => n * 2,
///             },
///         )
///     })
///     .collect::<Vec<_>>()
/// {
///     workflow.fulfill(request.0, Answer::Doubled(request.1))?;
/// }
///
/// assert_eq!(workflow.advance()?, Step::Complete);
/// assert_eq!(workflow.finish()?, 14);
/// # Ok::<(), ProtocolError>(())
/// ```
pub struct CoroutineWorkflow<Req: Request, T, E> {
    shared: Shared<Req>,
    outstanding: Vec<HostRequest<Req>>,
    state: State<T, E>,
}

impl<Req, T, E> CoroutineWorkflow<Req, T, E>
where
    Req: Request + Send + 'static,
    Req::Reply: Send,
{
    /// Creates a workflow that runs the future `start` returns.
    ///
    /// `start` is called at once, with the [`Host`] the workflow asks
    /// through; the future it returns is not polled until the first
    /// [`Workflow::advance`], so an `async fn` workflow does no work — and
    /// reports no error — before then.
    pub fn new<F, Fut>(start: F) -> Self
    where
        F: FnOnce(Host<Req>) -> Fut,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let shared = Arc::new(Mutex::new(Mailbox {
            phase: "Start".into(),
            next_id: 0,
            issued: Vec::new(),
            replies: Vec::new(),
            wakers: Vec::new(),
        }));

        let future = start(Host {
            shared: Arc::clone(&shared),
        });

        let mut workflow = Self {
            shared,
            outstanding: Vec::new(),
            state: State::Running(Box::pin(future)),
        };
        workflow.collect_issued();
        workflow
    }

    /// Records a reply without the payload validation [`Workflow::fulfill`]
    /// performs, for a concrete workflow's tests of its own
    /// defense-in-depth check against a reply of the wrong kind.
    #[cfg(any(test, feature = "test-util"))]
    pub fn fulfill_unchecked(&mut self, id: RequestId, reply: Req::Reply) {
        self.outstanding.retain(|r| r.id != id);
        self.deliver(id, reply);
    }

    fn deliver(&self, id: RequestId, reply: Req::Reply) {
        let waker = {
            let mut mailbox = lock(&self.shared);
            mailbox.replies.push((id, reply));
            mailbox
                .wakers
                .iter()
                .position(|(waiting, _)| *waiting == id)
                .map(|index| mailbox.wakers.swap_remove(index).1)
        };
        // A custom waker may access the mailbox; never call it under the lock.
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Moves requests the future issued into the list the host sees.
    fn collect_issued(&mut self) {
        let issued = mem::take(&mut lock(&self.shared).issued);
        self.outstanding.extend(issued);
    }
}

impl<Req: Request, T, E> CoroutineWorkflow<Req, T, E> {
    fn clear_pending(&mut self) {
        self.outstanding.clear();
        let pending = {
            let mut mailbox = lock(&self.shared);
            (
                mem::take(&mut mailbox.issued),
                mem::take(&mut mailbox.replies),
                mem::take(&mut mailbox.wakers),
            )
        };
        drop(pending);
    }
}

impl<Req: Request, T, E> Drop for CoroutineWorkflow<Req, T, E> {
    fn drop(&mut self) {
        // Collection wakers may retain futures that themselves hold the mailbox.
        self.clear_pending();
    }
}

impl<Req, T, E> Workflow for CoroutineWorkflow<Req, T, E>
where
    Req: Request + Send + 'static,
    Req::Reply: Send,
    T: Send,
    E: From<ProtocolError> + fmt::Display,
{
    type Error = E;
    type Output = T;
    type Request = Req;

    /// Runs the workflow until it finishes or every branch of it is
    /// waiting on the host.
    ///
    /// An error from the workflow is returned once, and poisons it:
    /// every later call reports [`ProtocolError::WorkflowFailed`].
    fn advance(&mut self) -> Result<Step, E> {
        let future = match &mut self.state {
            State::Running(future) => future,
            State::Complete(_) => return Ok(Step::Complete),
            State::Failed(failure) => {
                return Err(ProtocolError::WorkflowFailed(failure.clone()).into())
            }
        };

        let poll = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        self.collect_issued();

        match poll {
            Poll::Ready(Ok(output)) => {
                self.state = State::Complete(output);
                self.clear_pending();
                Ok(Step::Complete)
            }

            Poll::Ready(Err(err)) => {
                self.state = State::Failed(Failure::new(self.phase(), &err));
                self.clear_pending();
                Err(err)
            }

            // Waiting, with nothing the host could answer to unblock it.
            Poll::Pending if self.outstanding.is_empty() => {
                self.state =
                    State::Failed(Failure::new(self.phase(), &ProtocolError::WorkflowStalled));
                self.clear_pending();
                Err(ProtocolError::WorkflowStalled.into())
            }

            Poll::Pending => Ok(Step::AwaitHost),
        }
    }

    fn outstanding_requests(&self) -> &[HostRequest<Req>] {
        &self.outstanding
    }

    fn phase(&self) -> String {
        match &self.state {
            State::Running(_) => lock(&self.shared).phase.clone(),
            State::Complete(_) => "Complete".into(),
            State::Failed(failure) => failure.phase.clone(),
        }
    }

    /// Delivers a reply, after the same checks
    /// [`ManualWorkflowCore::fulfill`](crate::ManualWorkflowCore::fulfill) makes.
    fn fulfill(&mut self, id: RequestId, reply: Req::Reply) -> Result<(), E> {
        match &self.state {
            State::Running(_) => {}
            State::Complete(_) => return Err(ProtocolError::WorkflowComplete.into()),
            State::Failed(failure) => {
                return Err(ProtocolError::WorkflowFailed(failure.clone()).into())
            }
        }

        let index = self
            .outstanding
            .iter()
            .position(|r| r.id == id)
            .ok_or(ProtocolError::UnknownRequest(id))?;

        if !self.outstanding[index].kind.accepts(&reply) {
            return Err(ProtocolError::ReplyMismatch {
                id,
                expected: self.outstanding[index].kind.expected_reply(),
            }
            .into());
        }

        self.outstanding.remove(index);
        self.deliver(id, reply);
        Ok(())
    }

    fn finish(self) -> Result<T, E> {
        let mut this = self;
        match mem::replace(&mut this.state, State::Failed(Failure::default())) {
            State::Complete(output) => Ok(output),
            State::Running(_) => Err(ProtocolError::WorkflowNotComplete.into()),
            State::Failed(failure) => Err(ProtocolError::WorkflowFailed(failure).into()),
        }
    }
}

impl<Req: Request, T, E> fmt::Debug for CoroutineWorkflow<Req, T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.state {
            State::Running(_) => "Running",
            State::Complete(_) => "Complete",
            State::Failed(_) => "Failed",
        };

        f.debug_struct("CoroutineWorkflow")
            .field("state", &state)
            .field("outstanding", &self.outstanding)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::{
        future::pending,
        sync::atomic::{AtomicUsize, Ordering},
        task::Wake,
    };

    use futures_util::{
        future::join_all,
        stream::{FuturesUnordered, StreamExt},
    };

    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Ask {
        Number,
        Text,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Answer {
        Number(u32),
        Text(String),
        Failed,
    }

    impl Request for Ask {
        type Reply = Answer;

        fn expected_reply(&self) -> &'static str {
            match self {
                Self::Number => "Number",
                Self::Text => "Text",
            }
        }

        fn accepts(&self, reply: &Answer) -> bool {
            matches!(
                (self, reply),
                (_, Answer::Failed)
                    | (Self::Number, Answer::Number(_))
                    | (Self::Text, Answer::Text(_))
            )
        }
    }

    #[derive(Clone, Debug, PartialEq, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Protocol(ProtocolError),
        #[error("host failed")]
        HostFailed,
    }

    impl From<ProtocolError> for TestError {
        fn from(err: ProtocolError) -> Self {
            Self::Protocol(err)
        }
    }

    type TestWorkflow<T> = CoroutineWorkflow<Ask, T, TestError>;

    #[test]
    fn wake_driven_collections_finish_only_when_advanced() {
        for unordered in [false, true] {
            for count in [3, 100] {
                let polls = Arc::new(AtomicUsize::new(0));
                let observed = Arc::clone(&polls);
                let mut workflow: TestWorkflow<usize> = CoroutineWorkflow::new(|host| async move {
                    let replies: Vec<_> = (0..count).map(|_| host.request(Ask::Number)).collect();
                    observed.fetch_add(1, Ordering::SeqCst);
                    if unordered {
                        let mut replies: FuturesUnordered<_> = replies.into_iter().collect();
                        while replies.next().await.is_some() {}
                    } else {
                        join_all(replies).await;
                    }
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(count)
                });
                assert_eq!(workflow.advance().unwrap(), Step::AwaitHost);
                assert_eq!(workflow.outstanding_requests().len(), count);
                let ids: Vec<_> = workflow
                    .outstanding_requests()
                    .iter()
                    .map(|r| r.id)
                    .collect();
                for id in ids.into_iter().rev() {
                    workflow.fulfill(id, Answer::Number(1)).unwrap();
                }
                assert_eq!(polls.load(Ordering::SeqCst), 1, "fulfill must not poll");
                assert_eq!(workflow.advance().unwrap(), Step::Complete);
                assert_eq!(polls.load(Ordering::SeqCst), 2);
                assert_eq!(workflow.finish().unwrap(), count);
            }
        }
    }

    struct MailboxWake {
        shared: Shared<Ask>,
        wakes: AtomicUsize,
    }

    impl Wake for MailboxWake {
        fn wake(self: Arc<Self>) {
            // This would deadlock if fulfillment notified under the mailbox lock.
            let _mailbox = lock(&self.shared);
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn reply_wakers_are_replaced_notified_outside_the_lock_and_removed_on_drop() {
        let mut workflow: TestWorkflow<()> = CoroutineWorkflow::new(|_| async { Ok(()) });
        let host = Host {
            shared: Arc::clone(&workflow.shared),
        };
        let mut reply = host.request(Ask::Number);
        workflow.collect_issued();
        let first = Arc::new(MailboxWake {
            shared: Arc::clone(&workflow.shared),
            wakes: AtomicUsize::new(0),
        });
        let second = Arc::new(MailboxWake {
            shared: Arc::clone(&workflow.shared),
            wakes: AtomicUsize::new(0),
        });
        let first_waker = Waker::from(Arc::clone(&first));
        let second_waker = Waker::from(Arc::clone(&second));
        assert!(Pin::new(&mut reply)
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending());
        assert!(Pin::new(&mut reply)
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending());
        assert_eq!(lock(&workflow.shared).wakers.len(), 1);
        workflow.fulfill(reply.id(), Answer::Number(7)).unwrap();
        assert_eq!(first.wakes.load(Ordering::SeqCst), 0);
        assert_eq!(second.wakes.load(Ordering::SeqCst), 1);
        assert!(lock(&workflow.shared).wakers.is_empty());
        assert_eq!(
            Pin::new(&mut reply).poll(&mut Context::from_waker(&second_waker)),
            Poll::Ready(Answer::Number(7))
        );
        let mut abandoned = host.request(Ask::Text);
        assert!(Pin::new(&mut abandoned)
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending());
        drop(abandoned);
        assert!(lock(&workflow.shared).wakers.is_empty());
    }

    #[test]
    fn terminal_transitions_discard_unawaited_requests_replies_and_wakers() {
        for fail in [false, true] {
            let mut workflow: TestWorkflow<()> = CoroutineWorkflow::new(|host| async move {
                let unused = host.request(Ask::Text);
                let answer = number(&host).await?;
                drop(unused);
                drop(host.request(Ask::Number));
                if fail || answer == 0 {
                    Err(TestError::HostFailed)
                } else {
                    Ok(())
                }
            });
            workflow.advance().unwrap();
            let ids: Vec<_> = workflow
                .outstanding_requests()
                .iter()
                .map(|r| r.id)
                .collect();
            workflow.fulfill(ids[1], Answer::Number(1)).unwrap();
            if fail {
                workflow
                    .fulfill(ids[0], Answer::Text("stored but never consumed".into()))
                    .unwrap();
            }
            assert_eq!(workflow.advance().is_err(), fail);
            assert!(workflow.outstanding_requests().is_empty());
            let mailbox = lock(&workflow.shared);
            assert!(mailbox.issued.is_empty());
            assert!(mailbox.replies.is_empty());
            assert!(mailbox.wakers.is_empty());
            drop(mailbox);
            assert!(workflow
                .fulfill(ids[0], Answer::Text("late".into()))
                .is_err());
            assert_eq!(workflow.finish().is_err(), fail);
        }
    }

    #[test]
    fn early_failure_cancels_pending_collection_children_and_releases_the_mailbox() {
        let mut workflow: TestWorkflow<()> = CoroutineWorkflow::new(|host| async move {
            let mut replies: FuturesUnordered<_> =
                (0..3).map(|_| host.request(Ask::Number)).collect();
            match replies.next().await {
                Some(Answer::Failed) => Err(TestError::HostFailed),
                _ => Ok(()),
            }
        });
        workflow.advance().unwrap();
        let shared = Arc::downgrade(&workflow.shared);
        let id = workflow.outstanding_requests()[1].id;
        workflow.fulfill(id, Answer::Failed).unwrap();
        assert_eq!(workflow.advance(), Err(TestError::HostFailed));
        assert!(workflow.outstanding_requests().is_empty());
        assert!(lock(&workflow.shared).wakers.is_empty());
        drop(workflow);
        assert!(shared.upgrade().is_none());
    }

    #[test]
    fn dropping_a_running_collection_does_not_leak_the_mailbox() {
        let mut workflow: TestWorkflow<Vec<Answer>> = CoroutineWorkflow::new(|host| async move {
            Ok(join_all((0..100).map(|_| host.request(Ask::Number))).await)
        });
        workflow.advance().unwrap();
        let shared = Arc::downgrade(&workflow.shared);
        drop(workflow);
        assert!(shared.upgrade().is_none());
    }

    #[test]
    fn unchecked_delivery_also_notifies_collection_children() {
        let mut workflow: TestWorkflow<Vec<Answer>> = CoroutineWorkflow::new(|host| async move {
            Ok(join_all((0..100).map(|_| host.request(Ask::Number))).await)
        });
        workflow.advance().unwrap();
        let ids: Vec<_> = workflow
            .outstanding_requests()
            .iter()
            .map(|r| r.id)
            .collect();
        for id in ids {
            workflow.fulfill_unchecked(id, Answer::Text("bypassed".into()));
        }
        assert_eq!(workflow.advance().unwrap(), Step::Complete);
        assert_eq!(
            workflow.finish().unwrap(),
            vec![Answer::Text("bypassed".into()); 100]
        );
    }

    #[test]
    fn requests_issued_and_abandoned_on_the_first_poll_are_cancelled() {
        let mut workflow: TestWorkflow<()> = CoroutineWorkflow::new(|host| async move {
            drop(host.request(Ask::Text));
            Ok(())
        });
        assert_eq!(workflow.advance().unwrap(), Step::Complete);
        assert!(workflow.outstanding_requests().is_empty());
        assert!(matches!(
            workflow.fulfill(RequestId(0), Answer::Text("late".into())),
            Err(TestError::Protocol(ProtocolError::WorkflowComplete))
        ));
        workflow.finish().unwrap();
    }

    #[test]
    fn ask_decodes_with_the_request_id_and_defers_issuing_until_polled() {
        let mut workflow: TestWorkflow<u32> = CoroutineWorkflow::new(|host| async move {
            let answer = host.ask(Ask::Number, |id, reply| {
                assert_eq!(id, RequestId(0));
                match reply {
                    Answer::Number(n) => Ok(n * 2),
                    _ => Err(TestError::HostFailed),
                }
            });
            assert!(lock(&host.shared).issued.is_empty());
            answer.await
        });
        workflow.advance().unwrap();
        workflow.respond(|_| Answer::Number(4)).unwrap();
        assert_eq!(workflow.advance().unwrap(), Step::Complete);
        assert_eq!(workflow.finish().unwrap(), 8);
    }

    async fn number(host: &Host<Ask>) -> Result<u32, TestError> {
        match host.request(Ask::Number).await {
            Answer::Number(n) => Ok(n),
            _ => Err(TestError::HostFailed),
        }
    }

    /// Asks for `count` numbers one after another and sums them.
    async fn sum_sequentially(host: Host<Ask>, count: usize) -> Result<u32, TestError> {
        let mut total = 0;
        for _ in 0..count {
            total += number(&host).await?;
        }
        Ok(total)
    }

    fn only_outstanding(workflow: &TestWorkflow<impl Send>) -> RequestId {
        assert_eq!(workflow.outstanding_requests().len(), 1);
        workflow.outstanding_requests()[0].id
    }

    #[test]
    fn a_sequential_workflow_asks_one_thing_at_a_time() {
        let mut workflow: TestWorkflow<u32> =
            CoroutineWorkflow::new(|host| sum_sequentially(host, 3));
        assert!(
            workflow.outstanding_requests().is_empty(),
            "nothing runs before the first advance"
        );

        for n in [1, 2, 3] {
            assert_eq!(workflow.advance().unwrap(), Step::AwaitHost);
            let id = only_outstanding(&workflow);
            workflow.fulfill(id, Answer::Number(n)).unwrap();
        }

        assert_eq!(workflow.advance().unwrap(), Step::Complete);
        assert_eq!(workflow.advance().unwrap(), Step::Complete, "idempotently");
        assert_eq!(workflow.finish().unwrap(), 6);
    }

    #[test]
    fn advancing_without_a_reply_waits() {
        let mut workflow: TestWorkflow<u32> =
            CoroutineWorkflow::new(|host| sum_sequentially(host, 1));

        assert_eq!(workflow.advance().unwrap(), Step::AwaitHost);
        let id = only_outstanding(&workflow);
        assert_eq!(workflow.advance().unwrap(), Step::AwaitHost);
        assert_eq!(
            only_outstanding(&workflow),
            id,
            "the same request, not a new one"
        );
    }

    #[test]
    fn requests_issued_together_are_outstanding_together_and_answerable_in_any_order() {
        let mut workflow: TestWorkflow<(u32, String)> = CoroutineWorkflow::new(|host| async move {
            let number = host.request(Ask::Number);
            let text = host.request(Ask::Text);
            match (text.await, number.await) {
                (Answer::Text(t), Answer::Number(n)) => Ok((n, t)),
                _ => Err(TestError::HostFailed),
            }
        });

        assert_eq!(workflow.advance().unwrap(), Step::AwaitHost);
        let ids: Vec<_> = workflow
            .outstanding_requests()
            .iter()
            .map(|r| r.id)
            .collect();
        assert_eq!(ids.len(), 2);

        // The number first, though the workflow awaits the text first.
        workflow.fulfill(ids[0], Answer::Number(7)).unwrap();
        assert_eq!(workflow.advance().unwrap(), Step::AwaitHost);
        workflow.fulfill(ids[1], Answer::Text("x".into())).unwrap();

        assert_eq!(workflow.advance().unwrap(), Step::Complete);
        assert_eq!(workflow.finish().unwrap(), (7, "x".to_string()));
    }

    #[test]
    fn a_request_issued_by_the_start_closure_is_outstanding_at_once() {
        let workflow: TestWorkflow<Answer> = CoroutineWorkflow::new(|host| {
            let reply = host.request(Ask::Text);
            async move { Ok(reply.await) }
        });

        assert_eq!(workflow.outstanding_requests()[0].kind, Ask::Text);
    }

    #[test]
    fn a_workflow_error_is_returned_once_and_poisons_the_workflow() {
        let mut workflow: TestWorkflow<u32> =
            CoroutineWorkflow::new(|host| sum_sequentially(host, 2));

        workflow.advance().unwrap();
        let id = only_outstanding(&workflow);
        workflow.fulfill(id, Answer::Failed).unwrap();

        assert_eq!(workflow.advance(), Err(TestError::HostFailed));
        let failed = TestError::Protocol(ProtocolError::WorkflowFailed(Failure::new(
            "Start",
            &TestError::HostFailed,
        )));
        assert_eq!(workflow.advance(), Err(failed.clone()));
        assert_eq!(workflow.fulfill(id, Answer::Number(1)), Err(failed.clone()));
        assert_eq!(workflow.finish(), Err(failed));
    }

    #[test]
    fn phases_follow_stages_and_preserve_the_one_that_fails() {
        let mut workflow: TestWorkflow<u32> = CoroutineWorkflow::new(|host| async move {
            host.set_phase("Reading > First");
            let first = number(&host).await?;
            host.set_phase("Reading > Second");
            Ok(first + number(&host).await?)
        });
        assert_eq!(workflow.phase(), "Start");
        workflow.advance().unwrap();
        assert_eq!(workflow.phase(), "Reading > First");
        let first = only_outstanding(&workflow);
        workflow.fulfill(first, Answer::Number(1)).unwrap();
        workflow.advance().unwrap();
        assert_eq!(workflow.phase(), "Reading > Second");
        let second = only_outstanding(&workflow);
        workflow.fulfill(second, Answer::Failed).unwrap();
        assert_eq!(workflow.advance(), Err(TestError::HostFailed));

        let failed = TestError::Protocol(ProtocolError::WorkflowFailed(Failure::new(
            "Reading > Second",
            &TestError::HostFailed,
        )));
        assert_eq!(workflow.phase(), "Reading > Second");
        assert_eq!(workflow.advance(), Err(failed.clone()));
        assert_eq!(
            workflow.fulfill(second, Answer::Number(2)),
            Err(failed.clone())
        );
        assert_eq!(workflow.phase(), "Reading > Second");
        assert_eq!(workflow.finish(), Err(failed));
    }

    #[test]
    fn phases_can_be_set_before_polling_and_complete_after_success() {
        let mut workflow: TestWorkflow<u32> = CoroutineWorkflow::new(|host| {
            host.set_phase("Initialized");
            async move {
                host.set_phase("Calculating");
                Ok(7)
            }
        });
        assert_eq!(workflow.phase(), "Initialized");
        assert_eq!(workflow.advance().unwrap(), Step::Complete);
        assert_eq!(workflow.phase(), "Complete");
        assert_eq!(workflow.advance().unwrap(), Step::Complete);
        assert_eq!(workflow.finish().unwrap(), 7);
    }

    #[test]
    fn a_stalled_workflow_preserves_its_phase_on_every_later_call() {
        let mut workflow: TestWorkflow<u32> = CoroutineWorkflow::new(|host| async move {
            host.set_phase("InvalidWait");
            pending().await
        });
        assert_eq!(
            workflow.advance(),
            Err(TestError::Protocol(ProtocolError::WorkflowStalled))
        );
        let failed = TestError::Protocol(ProtocolError::WorkflowFailed(Failure::new(
            "InvalidWait",
            &ProtocolError::WorkflowStalled,
        )));
        assert_eq!(workflow.phase(), "InvalidWait");
        assert_eq!(workflow.advance(), Err(failed.clone()));
        assert_eq!(
            workflow.fulfill(RequestId(0), Answer::Failed),
            Err(failed.clone())
        );
        assert_eq!(workflow.finish(), Err(failed));
    }

    #[test]
    fn workflows_record_and_replay_success_and_failure_with_phases() {
        fn workflow() -> TestWorkflow<u32> {
            CoroutineWorkflow::new(|host| async move {
                host.set_phase("AwaitingNumber");
                number(&host).await
            })
        }
        for reply in [Answer::Number(7), Answer::Failed] {
            let mut recorded = crate::Recorder::new(workflow());
            assert_eq!(recorded.phase(), "Start");
            recorded.advance().unwrap();
            assert_eq!(recorded.phase(), "AwaitingNumber");
            let id = recorded.outstanding_requests()[0].id;
            recorded.fulfill(id, reply).unwrap();
            let result = recorded.advance();
            if result.is_err() {
                let _ = recorded.advance();
                let _ = recorded.fulfill(id, Answer::Number(0));
            } else {
                assert_eq!(recorded.phase(), "Complete");
            }
            let replayed = crate::replay(workflow(), recorded.transcript()).unwrap();
            assert_eq!(replayed.phase(), recorded.phase());
            assert_eq!(replayed.finish(), recorded.finish());
        }
    }

    #[test]
    fn fulfill_rejects_an_unknown_id_and_a_reply_of_the_wrong_kind() {
        let mut workflow: TestWorkflow<u32> =
            CoroutineWorkflow::new(|host| sum_sequentially(host, 1));
        workflow.advance().unwrap();
        let id = only_outstanding(&workflow);

        assert_eq!(
            workflow.fulfill(RequestId(99), Answer::Number(1)),
            Err(TestError::Protocol(ProtocolError::UnknownRequest(
                RequestId(99)
            )))
        );
        assert_eq!(
            workflow.fulfill(id, Answer::Text("no".into())),
            Err(TestError::Protocol(ProtocolError::ReplyMismatch {
                id,
                expected: "Number"
            }))
        );

        // Still outstanding, and answerable properly.
        workflow.fulfill(id, Answer::Number(4)).unwrap();
        assert_eq!(
            workflow.fulfill(id, Answer::Number(4)),
            Err(TestError::Protocol(ProtocolError::UnknownRequest(id))),
            "not twice"
        );
        assert_eq!(workflow.advance().unwrap(), Step::Complete);
    }

    #[test]
    fn a_finished_workflow_refuses_further_replies() {
        let mut workflow: TestWorkflow<u32> = CoroutineWorkflow::new(|_| async { Ok(1) });

        assert_eq!(workflow.advance().unwrap(), Step::Complete);
        assert_eq!(
            workflow.fulfill(RequestId(0), Answer::Number(1)),
            Err(TestError::Protocol(ProtocolError::WorkflowComplete))
        );
    }

    #[test]
    fn finishing_early_is_refused() {
        let workflow: TestWorkflow<u32> = CoroutineWorkflow::new(|host| sum_sequentially(host, 1));

        assert_eq!(
            workflow.finish(),
            Err(TestError::Protocol(ProtocolError::WorkflowNotComplete))
        );
    }

    #[test]
    fn waiting_on_something_the_host_cannot_answer_is_reported_not_hung() {
        let mut workflow: TestWorkflow<u32> = CoroutineWorkflow::new(|_| pending());

        assert_eq!(
            workflow.advance(),
            Err(TestError::Protocol(ProtocolError::WorkflowStalled))
        );
        assert_eq!(
            workflow.advance(),
            Err(TestError::Protocol(ProtocolError::WorkflowFailed(
                Failure::new("Start", &ProtocolError::WorkflowStalled,)
            )))
        );
    }

    #[test]
    fn fulfill_unchecked_bypasses_payload_validation() {
        let mut workflow: TestWorkflow<Answer> =
            CoroutineWorkflow::new(|host| async move { Ok(host.request(Ask::Number).await) });
        workflow.advance().unwrap();
        let id = only_outstanding(&workflow);

        workflow.fulfill_unchecked(id, Answer::Text("wrong".into()));

        assert!(workflow.outstanding_requests().is_empty());
        assert_eq!(workflow.advance().unwrap(), Step::Complete);
        assert_eq!(workflow.finish().unwrap(), Answer::Text("wrong".into()));
    }

    #[test]
    fn debug_output_names_the_state_and_what_is_outstanding() {
        let mut workflow: TestWorkflow<u32> =
            CoroutineWorkflow::new(|host| sum_sequentially(host, 1));
        workflow.advance().unwrap();

        let host = Host::<Ask> {
            shared: Arc::clone(&workflow.shared),
        };
        let pending = host.clone().request(Ask::Text);

        assert!(format!("{workflow:?}").contains("Running"));
        assert!(format!("{workflow:?}").contains("Number"));
        assert_eq!(format!("{host:?}"), "Host { .. }");
        assert_eq!(
            format!("{pending:?}"),
            format!("PendingReply {{ id: {:?}, .. }}", pending.id())
        );

        let mut done: TestWorkflow<u32> = CoroutineWorkflow::new(|_| async { Ok(0) });
        done.advance().unwrap();
        assert!(format!("{done:?}").contains("Complete"));

        let mut failed: TestWorkflow<u32> = CoroutineWorkflow::new(|_| pending_forever());
        let _ = failed.advance();
        assert!(format!("{failed:?}").contains("Failed"));
    }

    async fn pending_forever() -> Result<u32, TestError> {
        pending().await
    }
}
