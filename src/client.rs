//
// Copyright 2020 Joyent, Inc.
//

use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use failure::{self, format_err};
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot::{self, Sender};
use futures::future::{AbortHandle, Abortable};
use futures::lock::Mutex as AsyncMutex;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use slog::{debug, error, info, Logger};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Barrier, Notify};
use tokio::time;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::error::{InternalError, ZkError};
use crate::proto::decoder::ZkDecoder;
use crate::proto::encoder::{RequestWrapper, ZkEncoder};
use crate::proto::request::{OpCode, Request};
use crate::proto::response::{Response, FIRST_XID, HEARTBEAT_XID, SHUTDOWN_XID, WATCH_XID};
use crate::session_manager::SessionManager;
use crate::types::watch::{KeeperState, Watch, WatchOption, WatchType, WatchedEvent};
use crate::types::ZkConnectString;

pub(crate) type ReplyRecord = (OpCode, Sender<Result<Response, ZkError>>);
pub(crate) type RequestTuple = (Request, Sender<Result<Response, ZkError>>);

//
// This struct exists so the enqueuer can communicate with the client's
// background tasks. When the enqueuer is dropped, this struct is dropped, and
// its drop() method notifies the client to close the session and exit.
//
// Note that, while all of this struct's members are Clone, this struct
// **is not Clone**. The user may clone Enqueuers, and we want all of them to
// maintain a single reference to this struct, so we avoid cloning this struct
// and put it in an Arc instead.
//
#[derive(Debug)]
struct TaskTracker {
    notify: Arc<Notify>,
    log: Logger,
}

impl Drop for TaskTracker {
    fn drop(&mut self) {
        //
        // We have to do this before aborting the encoder/decoder so the run()
        // loop notices that we've exited instead of starting the encoder
        // and decoder again.
        //
        info!(self.log, "Enqueuer dropped");
        self.notify.notify();
    }
}

//
// TODO eliminate all uses of non-async mutex
// non-async mutexes should not be used from async contexts, because they
// block the whole thread and don't allow tasks to progress. This isn't a huge
// deal here because there are only two tasks and they intimately share state.
// anyway. We should still get rid of the regular mutexes, though.
//
// The master struct modeling the zookeeper client. Clonable so it can be passed
// to worker tasks.
//
#[derive(Clone)]
pub(crate) struct SharedState {
    // The channel over which requests come from the user
    rx: Arc<AsyncMutex<UnboundedReceiver<RequestTuple>>>,

    // Clone of client-facing tx to let us send server requests internally
    req_tx: UnboundedSender<(Request, Sender<Result<Response, ZkError>>)>,

    // Global map of operations awaiting response, indexed by xid
    replies: Arc<Mutex<HashMap<i32, ReplyRecord>>>,

    // Next xid to issue
    xid: Arc<AsyncMutex<i32>>,

    // Session state
    sess_mgr: SessionManager,

    //
    // Global map of pending watches.
    //
    // Watches are only registered once we have confirmed that the operation
    // that initiated the watch did not fail. Thus, we must stage watches here
    // first. The map is indexed by xid.
    //
    // The one exception: a watch can still be added if a call to exists()
    // fails because the node does not exist yet.
    //
    pending_watches: Arc<Mutex<HashMap<i32, (String, Watch)>>>,

    // Global map of watches registered, indexed by path
    watches: Arc<Mutex<HashMap<String, Vec<Watch>>>>,

    // Default watcher for state change events and non-custom watch events
    default_watcher: UnboundedSender<WatchedEvent>,

    log: slog::Logger,
}

impl SharedState {
    //
    // Runs the client. This method is called from `ZooKeeper::connect()`.
    //
    pub(crate) async fn start(
        conn_str: ZkConnectString,
        default_watcher: UnboundedSender<WatchedEvent>,
        session_timeout: Duration,
        read_only: bool,
        log: slog::Logger,
    ) -> Enqueuer {
        //
        // This task will wait at the barrier until the background task
        // connects to the ZooKeeper server.
        //
        let barrier = Arc::new(Barrier::new(2));
        let bg_barrier = Arc::clone(&barrier);

        let xid = Arc::new(AsyncMutex::new(FIRST_XID));

        // Client-facing request channel
        let (tx, rx) = mpsc::unbounded();

        let sess_mgr = SessionManager::new(
            conn_str,
            Arc::clone(&xid),
            tx.clone(),
            session_timeout,
            read_only,
            log.clone(),
        );

        let abort_handles: Arc<AsyncMutex<Option<(AbortHandle, AbortHandle)>>> =
            Arc::new(AsyncMutex::new(None));

        let notify = Arc::new(Notify::new());
        let enqueuer = Enqueuer {
            tx: tx.clone(),
            task_tracker: Arc::new(TaskTracker {
                notify: Arc::clone(&notify),
                log: log.clone(),
            }),
        };

        //
        // This task idles for the duration of the client's existence. It waits
        // until the client is dropped and then performs cleanup from the
        // async context.
        //
        let cleanup_sess_mgr = sess_mgr.clone();
        let cleanup_abort_handles = abort_handles.clone();
        let cleanup_log = log.clone();
        let cleanup_task = tokio::task::spawn(async move {
            notify.notified().await;
            //
            // If this fails, there's really nothing we can do. We were going to
            // exit anyway.
            //
            if let Err(e) = cleanup_sess_mgr.close_session().await {
                error!(cleanup_log, "Error closing session; ignoring: {:?}", e);
            }
            if let Some((h1, h2)) = &*cleanup_abort_handles.lock().await {
                info!(cleanup_log, "aborting encoder/decoder tasks");
                h1.abort();
                h2.abort();
            }
        });

        let rx = Arc::new(AsyncMutex::new(rx));
        let rx_clone = Arc::clone(&rx);
        let replies = Arc::new(Mutex::new(HashMap::new()));
        let replies_clone = Arc::clone(&replies);
        let watches = Arc::new(Mutex::new(HashMap::new()));
        let watches_clone = Arc::clone(&watches);

        // Spawn the background task that communicates with the server
        tokio::task::spawn(async move {
            let mut s = SharedState {
                xid,
                req_tx: tx,
                rx,
                log: log.clone(),
                sess_mgr,
                replies,
                pending_watches: Arc::new(Mutex::new(HashMap::new())),
                watches,
                default_watcher,
            };
            match s.run(bg_barrier, abort_handles).await {
                Err(e) => {
                    if let InternalError::SessionExpired = e {
                        s.notify_state_change(KeeperState::Expired)
                    }
                    error!(log, "Client exiting with error: {:?}", e)
                }
                Ok(_) => info!(log, "Client exiting"),
            }
            //
            // We need to clear the waiting-reply map and close the request
            // channel before we exit so any inflight requests sent using req_tx
            // (e.g. reregister_watches, close_session) get canceled and any new
            // requests sent using req_tx fail. We need to make sure this
            // happens so all associated threads exit and any SharedState
            // references get dropped, ensuring proper shutdown.
            //
            replies_clone.lock().unwrap().clear();
            rx_clone.lock().await.close();
            //
            // We clear this too just in case we've issued a request internally
            // that has set a watch and is waiting on it, so that watch
            // resolves and any SharedState references get dropped. We don't
            // issue such requests in the code right now, but we might in the
            // future.
            //
            watches_clone.lock().unwrap().clear();
            //
            // This will probably have run by now but we wait as a formality.
            //
            cleanup_task.await.expect("cleanup task panicked");
            info!(log, "Client exited");
        });

        // Wait for the client to connect for the first time
        barrier.wait().await;

        enqueuer
    }

    async fn run(
        &mut self,
        barrier: Arc<Barrier>,
        abort_handles: Arc<AsyncMutex<Option<(AbortHandle, AbortHandle)>>>,
    ) -> Result<(), InternalError> {
        let mut first = true;
        loop {
            if self.sess_mgr.is_exited().await {
                return Ok(());
            }

            // If we really can't reconnect, we have no choice but to exit.
            let (tx, rx) = self.sess_mgr.reconnect().await?;
            self.notify_state_change(KeeperState::SyncConnected);

            // Allow the initial call to start() to resolve
            if first {
                first = false;
                barrier.wait().await;
            } else {
                self.replies.lock().unwrap().clear();
                self.pending_watches.lock().unwrap().clear();
                let mut reregister_state = self.clone();
                tokio::task::spawn(async move {
                    if let Err(e) = reregister_state.reregister_watches().await {
                        error!(
                            reregister_state.log,
                            "Error re-registering watches: {:?}", e
                        );
                    }
                });
            }

            let mut enc_state = self.clone();
            let mut dec_state = self.clone();

            let (enc_abort_handle, enc_abort_registration) = AbortHandle::new_pair();
            let (dec_abort_handle, dec_abort_registration) = AbortHandle::new_pair();

            //
            // Store handles to the encoder and decoder so the cleanup task can
            // abort them
            //
            {
                let mut abort_handles = abort_handles.lock().await;
                *abort_handles = Some((enc_abort_handle.clone(), dec_abort_handle.clone()));
            }

            // Spawn the encoder and decoder tasks
            let enc_task_handle = tokio::task::spawn(Abortable::new(
                async move { enc_state.run_encoder(tx, dec_abort_handle).await },
                enc_abort_registration,
            ));
            let dec_task_handle = tokio::task::spawn(Abortable::new(
                async move { dec_state.run_decoder(rx, enc_abort_handle).await },
                dec_abort_registration,
            ));

            //
            // These futures below _never exit unless they fail_, which is why
            // they return an error as their "Ok" value.
            //
            // If these futures are aborted, we don't need to do anything other
            // than begin the reconnect loop again.
            //
            // TODO the error layering here is wrongly interpreted
            if let Ok(e) = dec_task_handle.await {
                error!(self.log, "Decoder error: {:?}", e);
            } else {
                debug!(self.log, "decoder future aborted");
            }
            if let Ok(e) = enc_task_handle.await {
                error!(self.log, "Encoder error: {:?}", e);
            } else {
                debug!(self.log, "encoder future aborted");
            }

            //
            // If we're here, we've just been disconnected and are about to
            // reconnect.
            //
            self.notify_state_change(KeeperState::Disconnected);
            //
            // This isn't strictly necessary but is good bookkeeping, since
            // the encoder/decoder tasks aren't running if we're here.
            //
            {
                let mut abort_handles = abort_handles.lock().await;
                *abort_handles = None;
            }
        }
    }

    //
    // Re-registers outstanding watches with the server. Called upon reconnect.
    //
    // TODO node-zkstream does some interesting dedup when reregistering watches.
    // Should we be doing that too?
    // TODO figure out why watching for the existence of a node that does exist,
    // then reconnecting and reregistering watches causes a "create" event to
    // get sent even though the node already existed
    // TODO handle weirdness described here: https://github.com/joyent/node-zkstream/blob/fe7dadcfd59af3632302f807fad33e51f5b41be3/lib/zk-session.js#L497-L526
    //
    async fn reregister_watches(&mut self) -> Result<(), InternalError> {
        let mut data = HashSet::new();
        let mut exists = HashSet::new();
        let mut children = HashSet::new();
        for (path, watchlist) in self.watches.lock().unwrap().iter() {
            for watch in watchlist {
                let set = match watch.wtype {
                    WatchType::Child => &mut children,
                    WatchType::Data => &mut data,
                    WatchType::Exist => &mut exists,
                };
                set.insert(path.clone());
            }
        }
        if data.is_empty() && exists.is_empty() && children.is_empty() {
            return Ok(());
        }
        let req = Request::SetWatches {
            last_zxid_seen: self.sess_mgr.get_zxid().await,
            data,
            exists,
            children,
        };
        let (tx, rx) = oneshot::channel();
        self.req_tx.unbounded_send((req, tx))?;
        rx.await??;
        Ok(())
    }

    //
    // Sends a state change event to the user.
    //
    fn notify_state_change(&self, state: KeeperState) {
        //
        // We ignore any send errors, because the user may have dropped
        // the default watcher, and that's ok.
        //
        let _ = self
            .default_watcher
            .unbounded_send(WatchedEvent::state_event(state));
    }

    //
    // Decoder task. Accepts a handle to the encoder task so it can abort the
    // encoder if the connection is lost.
    //
    async fn run_decoder(
        &mut self,
        rx: ReadHalf<TcpStream>,
        enc_abort_handle: AbortHandle,
    ) -> InternalError {
        async fn recv_msg(
            decoder: &mut FramedRead<ReadHalf<TcpStream>, ZkDecoder>,
            sess_mgr: SessionManager,
            log: Logger,
        ) -> Result<(), InternalError> {
            match decoder.next().await {
                Some(item) => {
                    match item? {
                        //
                        // The decoder encountered some server error to be
                        // handled internally, or client logic error. We can't
                        // really do anything, so we just log the error.
                        //
                        // TODO These happened in the old client, but I'm not
                        // sure that these should ever happen here --
                        // should we panic instead in decoder if they do?
                        //
                        Err(e) => error!(log, "Server Error; doing nothing: {:?}", e),
                        Ok(zxid) => sess_mgr.set_zxid(zxid).await,
                    }
                    Ok(())
                }
                None => Err(InternalError::ConnectionEnded),
            }
        }

        let mut decoder = FramedRead::new(
            rx,
            ZkDecoder::new(
                Arc::clone(&self.replies),
                Arc::clone(&self.watches),
                Arc::clone(&self.pending_watches),
                self.default_watcher.clone(),
                self.log.clone(),
            ),
        );

        loop {
            if let Err(e) = recv_msg(&mut decoder, self.sess_mgr.clone(), self.log.clone()).await {
                //
                // The stream encountered an unrecoverable error. We stop the
                // encoder and then exit ourselves.
                //
                enc_abort_handle.abort();
                return e;
            }
        }
    }

    //
    // Encoder task. Accepts a handle to the decoder task so it can abort the
    // decoder if the connection is lost.
    //
    async fn run_encoder(
        &mut self,
        tx: WriteHalf<TcpStream>,
        dec_abort_handle: AbortHandle,
    ) -> InternalError {
        let mut encoder = FramedWrite::new(tx, ZkEncoder::new());
        //
        // This value only changes upon reconnect, and this task doesn't run
        // across reconnects, so it's safe to only fetch the heartbeat interval
        // once.
        //
        let heartbeat_interval = self.sess_mgr.get_heartbeat_interval().await;
        loop {
            let timeout_result =
                time::timeout(heartbeat_interval, self.rx.lock().await.next()).await;
            let (mut request, response_tx) = match timeout_result {
                Err(_) => {
                    //
                    // Heartbeat interval elapsed without us having anything to
                    // send. Send a heartbeat!
                    //
                    debug!(self.log, "Sending heartbeat");
                    if let Err(e) = encoder
                        .send(RequestWrapper {
                            xid: HEARTBEAT_XID,
                            req: Request::Ping,
                        })
                        .await
                    {
                        dec_abort_handle.abort();
                        return InternalError::from(e);
                    }
                    continue;
                }
                Ok(tuple) => tuple.expect("internal enqueuer rx dropped"),
            };

            let new_xid = if let Request::Close = request {
                SHUTDOWN_XID
            } else {
                let mut xid_handle = self.xid.lock().await;
                // Skip special xids
                while *xid_handle == SHUTDOWN_XID
                    || *xid_handle == WATCH_XID
                    || *xid_handle == HEARTBEAT_XID
                {
                    *xid_handle += 1;
                }
                let new_xid = *xid_handle;
                *xid_handle += 1;
                new_xid
            };

            info!(self.log, "enqueueing request {:?}", request; "xid" => new_xid);

            // Register a watch, if necessary
            match request {
                Request::GetData {
                    ref path,
                    ref mut watch,
                    ..
                }
                | Request::GetChildren {
                    ref path,
                    ref mut watch,
                    ..
                }
                | Request::Exists {
                    ref path,
                    ref mut watch,
                    ..
                } => {
                    if let WatchOption::Oneshot(_) = *watch {
                        //
                        // Replace the request's watch field so we can use the
                        // Sender separately from the request below. It's safe
                        // to replace with WatchOption::Global because all watch
                        // types other than WatchOption::None are serialized
                        // identically.
                        //
                        let w = mem::replace(watch, WatchOption::Global);
                        if let WatchOption::Oneshot(tx) = w {
                            let wtype = match request {
                                Request::GetData { .. } => WatchType::Data,
                                Request::GetChildren { .. } => WatchType::Child,
                                Request::Exists { .. } => WatchType::Exist,
                                _ => unreachable!(),
                            };
                            debug!(
                                self.log,
                                "adding pending watch";
                                "xid" => new_xid,
                                "path" => path,
                                "wtype" => ?wtype
                            );
                            self.pending_watches
                                .lock()
                                .unwrap()
                                .insert(new_xid, (path.to_string(), Watch { wtype, tx }));
                        } else {
                            unreachable!();
                        }
                    }
                }
                _ => {}
            }

            // Store response info
            if let Request::Connect { .. } = request {
                unreachable!("connect request sent over client-facing channel");
            } else {
                let mut replies = self.replies.lock().unwrap();
                let old = replies.insert(new_xid, (request.opcode(), response_tx));
                assert!(old.is_none());
            };

            //
            // XXX we should really send this in the background but then we have
            // to handle encoder lifetime/ownership. Blah!
            //
            if let Err(e) = encoder
                .send(RequestWrapper {
                    xid: new_xid,
                    req: request,
                })
                .await
            {
                dec_abort_handle.abort();
                return InternalError::from(e);
            }
        }
    }
}

//
// This struct is passed back to the user and allows them to communicate with
// the client by sending requests over the `tx` field. All enqueuers linked to
// a given client contain a reference to the same `TaskTracker`. When the last
// enqueuer is dropped, the `TaskTracker` is dropped and its `drop()` method
// initiates cleanup of the client.
//
#[derive(Clone, Debug)]
pub(crate) struct Enqueuer {
    tx: UnboundedSender<(Request, Sender<Result<Response, ZkError>>)>,
    task_tracker: Arc<TaskTracker>,
}

impl Enqueuer {
    pub(crate) async fn enqueue(
        &self,
        request: Request,
    ) -> Result<Result<Response, ZkError>, failure::Error> {
        let (tx, rx) = oneshot::channel();
        match self.tx.unbounded_send((request, tx)) {
            Ok(()) => rx
                .await
                .map_err(|e| format_err!("Error processing request: {:?}", e)),
            Err(e) => Err(format_err!("failed to enqueue new request: {:?}", e)),
        }
    }
}
