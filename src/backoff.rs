//
// Copyright 2020 Joyent, Inc.
//

use std::default::Default;
use std::sync::Arc;
use std::time::{Duration, Instant};

use backoff::backoff::Backoff;
use backoff::default::{MAX_INTERVAL_MILLIS, MULTIPLIER};
use backoff::ExponentialBackoff;
use futures::channel::mpsc::{self, UnboundedSender};
use futures::lock::Mutex as AsyncMutex;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use lazy_static::lazy_static;
use slog::{debug, error, info, Logger};
use tokio::time;

use crate::session_manager::ZkConnectStringState;

const NO_DELAY: Duration = Duration::from_secs(0);

lazy_static! {
    //
    // The maximum Duration that next_backoff() can return (when using the
    // default backoff parameters). Public for use in tests.
    //
    pub static ref MAX_BACKOFF_INTERVAL: Duration = Duration::from_millis(
        (MAX_INTERVAL_MILLIS as f64 * MULTIPLIER).ceil() as u64,
    );

    //
    // The amount of time that must elapse after successful connection without
    // an error occurring in order for the resolver state to be considered
    // stable, at which point the backoff state is reset.
    //
    // We choose the threshold based on MAX_BACKOFF_INTERVAL because if the
    // threshold were smaller, a given backoff interval could be bigger than the
    // threshold, so we would prematurely reset the backoff before the operation
    // even got a chance to try again. The threshold could be bigger, but what's
    // the point in that?
    //
    // We add a little slack so the backoff doesn't get
    // reset just before a (possibly failing) reconnect attempt is made.
    //
    static ref BACKOFF_RESET_THRESHOLD: Duration =
        *MAX_BACKOFF_INTERVAL + Duration::from_secs(1);
}

//
// Encapsulates a backoff object and adds the notion of a time threshold that
// must be reached before the connection is considered stable and the backoff
// state is reset. This threshold is managed automatically using a background
// thread -- users can just call ZkBackoff::next_delay() normally.
//
pub(crate) struct ZkBackoff {
    backoff: Arc<AsyncMutex<ExponentialBackoff>>,
    //
    // If this is `true`, the backoff's max_elapsed_time has elapsed. We use
    // this field to assert the invariant that we don't attempt to reconnect
    // after the session has expired.
    //
    expired: bool,
    //
    // Internal channel used for indicating that an error has occurred to the
    // stability-tracking thread
    //
    error_tx: UnboundedSender<ResolverBackoffMsg>,
    log: Logger,
}

//
// For use by ZkBackoff object.
//
enum ResolverBackoffMsg {
    //
    // The Duration is the duration of the backoff in response to the error.
    //
    ErrorOccurred(Duration),
}

//
// For use by ZkBackoff object.
//
enum ResolverBackoffState {
    Stable,
    //
    // The Duration is the duration of the backoff in response to the error.
    //
    Unstable(Duration),
}

impl ZkBackoff {
    pub(crate) fn new(max_elapsed_time: Option<Duration>, log: Logger) -> Self {
        let mut backoff = ExponentialBackoff::default();
        //
        // We'd rather the resolver not give up trying to reconnect, so we
        // set the max_elapsed_time to `None` so next_backoff() always returns
        // a valid interval.
        //
        backoff.max_elapsed_time = max_elapsed_time;

        let (error_tx, mut error_rx) = mpsc::unbounded();
        let backoff = Arc::new(AsyncMutex::new(backoff));

        //
        // Start the stability-tracking task.
        //
        //
        // The waiting period for stability starts from the _time of successful
        // connection_. The time of successful connection, if it occurs at all,
        // will always be equal to (time of last error + duration of resulting
        // backoff). Thus, from time of last error, we must wait (duration of
        // resulting backoff + stability threshold) in order for the connection
        // to be considered stable. Thus, we pass the backoff duration from
        // next_backoff to this thread so the thread can figure out how long to
        // wait for stability.
        //
        // Note that this thread doesn't actually _know_ if the connect
        // operation succeeds, because we only touch the ZkBackoff
        // object when an error occurs. However, we can assume that it succeeds
        // when waiting for stability, because, if the connect operation fails,
        // this thread will receive another error and restart the wait period.
        //
        // If the connect operation takes longer than BACKOFF_RESET_THRESHOLD to
        // complete and then fails, we'll reset the backoff erroneously. This
        // situation is highly unlikely, so we'll cross that bridge when we come
        // to it.
        //
        let backoff_clone = Arc::clone(&backoff);
        let task_log = log.clone();
        debug!(log, "spawning stability-tracking task");
        tokio::task::spawn(async move {
            let mut state = ResolverBackoffState::Stable;
            loop {
                match state {
                    ResolverBackoffState::Stable => {
                        //
                        // Wait for an error to happen
                        //
                        debug!(task_log, "backoff stable; waiting for error");
                        match error_rx.next().await {
                            //
                            // * zero days since last accident *
                            //
                            Some(ResolverBackoffMsg::ErrorOccurred(new_backoff)) => {
                                info!(
                                    task_log,
                                    "error received; backoff transitioning to \
                                     unstable state"
                                );
                                state = ResolverBackoffState::Unstable(new_backoff);
                                continue;
                            }
                            //
                            // ZkBackoff object was dropped, so we exit
                            // the task
                            //
                            None => break,
                        }
                    }
                    ResolverBackoffState::Unstable(current_backoff) => {
                        debug!(task_log, "backoff unstable; waiting for stability");
                        //
                        // See large comment above for explanation of why we
                        // wait this long
                        //
                        match time::timeout(
                            current_backoff + *BACKOFF_RESET_THRESHOLD,
                            error_rx.next(),
                        )
                        .await
                        {
                            //
                            // Timeout waiting for an error: the stability
                            // threshold has been reached, so reset the backoff
                            //
                            Err(_) => {
                                info!(task_log, "stability threshold reached; resetting backoff");
                                backoff_clone.lock().await.reset();
                                state = ResolverBackoffState::Stable
                            }
                            Ok(item) => match item {
                                //
                                // We got another error, so restart the countdown
                                //
                                Some(ResolverBackoffMsg::ErrorOccurred(new_backoff)) => {
                                    debug!(
                                        task_log,
                                        "error received while waiting for \
                                         stability; restarting wait period"
                                    );
                                    state = ResolverBackoffState::Unstable(new_backoff);
                                    continue;
                                }
                                //
                                // ZkBackoff object was dropped, so we exit
                                // the thread
                                //
                                None => break,
                            },
                        }
                    }
                }
            }
            debug!(task_log, "stability-tracking task exiting");
        });

        ZkBackoff {
            backoff,
            expired: false,
            error_tx,
            log,
        }
    }

    pub(crate) async fn set_max_elapsed_time(&self, time: Option<Duration>) {
        self.backoff.lock().await.max_elapsed_time = time;
    }

    //
    // This function MUST be called every time the overarching operation being
    // backed-off (i.e. the zk connect request) is started anew -- NOT every
    // time we attempt the operation within a given spate of attempts.
    //
    // We must do this because the start_time field is usually only updated when
    // a backoff is _reset_, not when the operation is attempted anew, and the
    // max_elapsed_time bound is checked relative to the start_time field.
    // We want the max_elapsed_time to be checked relative to the start of the
    // attempt, not from the time of last reset, which could have happened long
    // before the start of the current attempt.
    //
    pub(crate) async fn start_operation(&self) {
        self.backoff.lock().await.start_time = Instant::now();
    }

    async fn next_backoff_inner(&mut self) -> Option<Duration> {
        let mut backoff = self.backoff.lock().await;
        let next_backoff = backoff.next_backoff();
        //
        // Notify the stability-tracking thread that an error has occurred
        //
        if let Some(next_backoff) = next_backoff {
            self.error_tx
                .unbounded_send(ResolverBackoffMsg::ErrorOccurred(next_backoff))
                .expect("Error sending over error_tx");

            debug!(self.log, "retrying with backoff {:?}", next_backoff);
        } else {
            self.expired = true;
            //
            // There's not really anything we can do if this fails. We won't be
            // using the channel anymore anyway.
            //
            if let Err(e) = self.error_tx.close().await {
                error!(self.log, "error closing internal backoff channel: {:?}", e);
            }
            debug!(self.log, "max backoff time elapsed; not retrying");
        }
        next_backoff
    }

    //
    // Returns a duration to wait, consulting the provided ZkConnectStringState.
    //
    // NOTE: also resets the ZkConnectStringState's connection attempts if it is
    // found that we should actually wait.
    //
    pub(crate) async fn next_delay(
        &mut self,
        conn_str_state: Arc<AsyncMutex<ZkConnectStringState>>,
    ) -> Option<Duration> {
        assert!(!self.expired);
        let mut conn_str_state = conn_str_state.lock().await;
        let should_wait = conn_str_state.should_wait();
        if should_wait {
            conn_str_state.reset_attempts();
            self.next_backoff_inner().await
        } else {
            Some(NO_DELAY)
        }
    }
}
