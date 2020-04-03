use byteorder::{BigEndian, WriteBytesExt};
use failure;
use failure::format_err;

use futures::{
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot::{self, Sender},
    },
    future::{Either, TryFutureExt},
    lock::Mutex as AsyncMutex,
    ready, select,
    sink::SinkExt,
    stream::StreamExt,
    task::Poll,
    Future,
};
use slog;
use slog::{debug, error, trace};
use std::collections::HashMap;
use std::mem;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio;
use tokio::io::{self, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::time;
use tokio_util::codec::{FramedRead, FramedWrite};

// TODO clean up imports
use crate::error::ZkError;
use crate::proto::decoder::ZkDecoder;
use crate::proto::encoder::{RequestWrapper, ZkEncoder};
use crate::proto::session_manager::SessionManager;
use crate::proto::{
    request::{OpCode, Request},
    response::Response,
};
use crate::types::watch::{WatchType, WatchedEvent};
use crate::Watch;

#[derive(Clone)]
pub(crate) struct SharedState {
    rx: Arc<AsyncMutex<UnboundedReceiver<(Request, Sender<Result<Response, ZkError>>)>>>,
    ///
    /// Clone of client-facing tx to let us send server requests internally
    ///
    req_tx: UnboundedSender<(Request, Sender<Result<Response, ZkError>>)>,
    ///
    /// Global map of operations awaiting response, indexed by xid
    ///
    replies: Arc<Mutex<HashMap<i32, (OpCode, Sender<Result<Response, ZkError>>)>>>,
    ///
    /// Next xid to issue
    ///
    xid: Arc<AsyncMutex<i32>>,

    log: slog::Logger,

    sess_mgr: SessionManager,
    ///
    /// Global map of pending watches.
    ///
    /// Watches are only registered once we have confirmed that the operation
    /// that initiated the watch did not fail. Thust, we must stage watches here
    /// first. The map is indexed by xid.
    ///
    /// The one exception: a watch can still be added if a call to exists()
    /// fails because the node does not exist yet.
    ///
    pending_watches: Arc<Mutex<HashMap<i32, (String, Sender<WatchedEvent>, WatchType)>>>,
    ///
    /// Global map of watches registered, indexed by path
    ///
    watches: Arc<Mutex<HashMap<String, Vec<(Sender<WatchedEvent>, WatchType)>>>>,
    ///
    /// Default watcher for state change events and non-custom watch events
    ///
    default_watcher: UnboundedSender<WatchedEvent>,
    addr: SocketAddr,
}

// struct Writer {
//     /// Incoming requests
//     rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Result<Response, ZkError>>)>,
//     /// Next xid to issue
//     next_xid: i32,
//     sess_mgr: Arc<Mutex<SessionManager>>, // /// Current state
// }

// struct Reader {
//     sess_mgr: Arc<Mutex<SessionManager>>, // /// Current state
// }

impl SharedState {
    pub(crate) async fn start(
        addr: SocketAddr,
        default_watcher: UnboundedSender<WatchedEvent>,
        log: slog::Logger,
    ) -> Enqueuer {
        let (tx, rx) = mpsc::unbounded();

        let exitlogger = log.clone();
        // TODO turns out the zk protocol mandates a default watcher -- so we
        // need one!
        let req_tx = tx.clone();
        let xid = Arc::new(AsyncMutex::new(0));
        tokio::task::spawn(async move {
            let mut s = SharedState {
                // state: PacketizerState::Connected(ActivePacketizer::new(stream)),
                xid: Arc::clone(&xid),
                req_tx,
                rx: Arc::new(AsyncMutex::new(rx)),
                log: log.clone(),
                sess_mgr: SessionManager::new(addr, xid, log),
                replies: Arc::new(Mutex::new(HashMap::new())),
                pending_watches: Arc::new(Mutex::new(HashMap::new())),
                watches: Arc::new(Mutex::new(HashMap::new())),
                default_watcher,
                addr,
            };
            s.run().await
            // .map_err(move |e| {
            //     error!(exitlogger, "packetizer exiting: {:?}", e);
            //     drop(e);
        });
        tokio::task::yield_now().await;
        // TODO should this function ever return an error?
        // TODO make sure there's some way for the bg task to know to exit if the user drops the enqueuer
        // (maybe impl drop() and abort the bg task?)

        //
        // Block until we make the initial connection. The background task might
        // try to connect first, but that's ok. Either way, we'll block until
        // we're connected
        //
        // TODO handle error instead of unwrapping
        Enqueuer(tx)
    }

    // #[allow(dead_code)]
    // enum PacketizerState {
    //     Connected(ActivePacketizer),
    //     Reconnecting(Box<Future<Output = Result<ActivePacketizer, failure::Error>> + Send + 'static>),
    // }

    // impl<S> PacketizerState<S>
    // where
    //     S: AsyncRead + AsyncWrite,
    // {
    //     fn poll(
    //         &mut self,
    //         exiting: bool,
    //         logger: &mut slog::Logger,
    //         default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
    //     ) -> Result<Poll<()>, failure::Error> {
    //         let ap = match *self {
    //             PacketizerState::Connected(ref mut ap) => {
    //                 return ap.poll(exiting, logger, default_watcher);
    //             }
    //             PacketizerState::Reconnecting(ref mut c) => ready!(c.poll()),
    //         };

    //         // we are now connected!
    //         mem::replace(self, PacketizerState::Connected(ap));
    //         self.poll(exiting, logger, default_watcher)
    //     }
    // }

    async fn run(&mut self) {
        loop {
            // TODO handle error or get rid of it
            let (tx, rx) = self.sess_mgr.reconnect().await.unwrap();
            // TODO spawn abortable tasks
            // if one task fails, abort the other and loop again
            // TODO I will have to think about a lot of edge cases with interrupted loop state probably

            // TODO handle error here
            let mut enc_state = self.clone();
            let enc_handle = tokio::task::spawn(async move { enc_state.run_encoder(tx).await });
            // TODO handle error here
            let mut dec_state = self.clone();
            let dec_handle = tokio::task::spawn(async move { dec_state.run_decoder(rx).await });
            enc_handle.await;
            dec_handle.await;

            // TODO handle error instead of unwrapping
        }
    }

    async fn run_decoder(&mut self, rx: ReadHalf<TcpStream>) {
        let mut decoder = FramedRead::new(
            rx,
            ZkDecoder::new(
                Arc::clone(&self.replies),
                Arc::clone(&self.watches),
                Arc::clone(&self.pending_watches),
                self.default_watcher.clone(),
                Arc::clone(&self.sess_mgr.session_info),
                self.log.clone(),
            ),
        );
        loop {
            // TODO handle disconnect and error
            decoder.next().await;
        }
    }

    async fn run_encoder(&mut self, tx: WriteHalf<TcpStream>) {
        let mut encoder = FramedWrite::new(tx, ZkEncoder::new());
        //
        // This only changes upon reconnect, and this function doesn't run
        // across reconnects, so it's safe to only fetch the heartbeat interval
        // once.
        //
        let heartbeat_interval = self.sess_mgr.get_heartbeat_interval();
        // TODO look into SinkExt::with() and send_all() instead of looping manually?
        loop {
            let timeout_result =
                time::timeout(heartbeat_interval, self.rx.lock().await.next()).await;

            //
            // Heartbeat interval elapsed without us having anything to send.
            // Send a heartbeat!
            //
            if timeout_result.is_err() {
                println!("heartbeating");
                encoder
                    .send(RequestWrapper {
                        xid: -2,
                        req: Request::Ping,
                    })
                    .await;
                continue;
            }

            // TODO this unwrap is always safe but it looks sketchy -- refactor?
            let (mut request, response_tx) = match timeout_result.unwrap() {
                Some(tuple) => tuple,
                None => {
                    //
                    // The user dropped the handle to zk.
                    // TODO mark client for shutdown once there are no watches
                    // left. Or, just shut it down immediately -- who is
                    // going to drop the zk handle but keep a watch handle
                    // around?
                    // TODO log this
                    //
                    return;
                }
            };
            let mut xid_handle = self.xid.lock().await;
            let new_xid = *xid_handle;
            *xid_handle += 1;
            debug!(self.log, "enqueueing request {:?}", request; "xid" => new_xid);

            //
            // Register a watch, if necessary
            //
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
                    // TODO refactor this probably
                    if let Watch::Oneshot(_) = *watch {
                        //
                        // set to Global so that watch will be sent as 1u8
                        // TODO this is a dirty hack -- elucidate it
                        //
                        let w = mem::replace(watch, Watch::Global);
                        if let Watch::Oneshot(w) = w {
                            let wtype = match request {
                                Request::GetData { .. } => WatchType::Data,
                                Request::GetChildren { .. } => WatchType::Child,
                                Request::Exists { .. } => WatchType::Exist,
                                _ => unreachable!(),
                            };
                            debug!(
                                self.log,
                                "adding pending watcher";
                                "xid" => new_xid,
                                "path" => path,
                                "wtype" => ?wtype
                            );
                            self.pending_watches
                                .lock()
                                .unwrap()
                                .insert(new_xid, (path.to_string(), w, wtype));
                        } else {
                            unreachable!();
                        }
                    }
                }
                _ => {}
            }

            //
            // Store response info
            //
            if let Request::Connect { .. } = request {
                unreachable!("connect request sent over client-facing channel");
            } else {
                let mut replies = self.replies.lock().unwrap();
                let old = replies.insert(new_xid, (request.opcode(), response_tx));
                assert!(old.is_none());
            };
            // TODO send heartbeats
            // TODO we should really send this in the background but then we have
            // to handle encoder lifetime/ownership. Blah!
            // TODO trigger reconnect upon error
            encoder
                .send(RequestWrapper {
                    xid: new_xid,
                    req: request,
                })
                .await;
        }
    }
}
//     fn poll_enqueue(&mut self) -> Result<Poll<()>, ()> {
//         while let PacketizerState::Connected(ref mut ap) = self.state {
//             let (mut item, tx) = match ready!(self.rx.poll()) {
//                 Some((request, response)) => (request, response),
//                 None => return Err(()),
//             };
//             debug!(self.logger, "enqueueing request {:?}", item; "xid" => self.xid);

//             match item {
//                 Request::GetData {
//                     ref path,
//                     ref mut watch,
//                     ..
//                 }
//                 | Request::GetChildren {
//                     ref path,
//                     ref mut watch,
//                     ..
//                 }
//                 | Request::Exists {
//                     ref path,
//                     ref mut watch,
//                     ..
//                 } => {
//                     if let Watch::Custom(_) = *watch {
//                         // set to Global so that watch will be sent as 1u8
//                         let w = mem::replace(watch, Watch::Global);
//                         if let Watch::Custom(w) = w {
//                             let wtype = match item {
//                                 Request::GetData { .. } => WatchType::Data,
//                                 Request::GetChildren { .. } => WatchType::Child,
//                                 Request::Exists { .. } => WatchType::Exist,
//                                 _ => unreachable!(),
//                             };
//                             trace!(
//                                 self.logger,
//                                 "adding pending watcher";
//                                 "xid" => self.xid,
//                                 "path" => path,
//                                 "wtype" => ?wtype
//                             );
//                             ap.pending_watchers
//                                 .insert(self.xid, (path.to_string(), w, wtype));
//                         } else {
//                             unreachable!();
//                         }
//                     }
//                 }
//                 _ => {}
//             }

//             ap.enqueue(self.xid, item, tx);
//             self.xid += 1;
//         }
//         Ok(Poll::Pending)
//     }

// impl<S> Future for Packetizer<S>
// where
//     S: ZooKeeperTransport,
// {
//     type Output = Result<(), failure::Error>;

//     fn poll(&mut self) -> Poll<Self::Output> {
//         trace!(self.logger, "packetizer polled");
//         if !self.exiting {
//             trace!(self.logger, "poll_enqueue");
//             match self.poll_enqueue() {
//                 Ok(_) => {}
//                 Err(()) => {
//                     // no more requests will be enqueued
//                     self.exiting = true;

//                     if let PacketizerState::Connected(ref mut ap) = self.state {
//                         // send CloseSession
//                         // length is fixed
//                         ap.outbox
//                             .write_i32::<BigEndian>(8)
//                             .expect("Vec::write should never fail");
//                         // xid
//                         ap.outbox
//                             .write_i32::<BigEndian>(0)
//                             .expect("Vec::write should never fail");
//                         // opcode
//                         ap.outbox
//                             .write_i32::<BigEndian>(request::OpCode::CloseSession as i32)
//                             .expect("Vec::write should never fail");
//                     } else {
//                         unreachable!("poll_enqueue will never return Err() if not connected");
//                     }
//                 }
//             }
//         }

//         self.state
//             .poll(self.exiting, &mut self.logger, &mut self.default_watcher)
//     }
// }

#[derive(Clone, Debug)]
pub(crate) struct Enqueuer(UnboundedSender<(Request, Sender<Result<Response, ZkError>>)>);

impl Enqueuer {
    pub(crate) async fn enqueue(
        &self,
        request: Request,
    ) -> Result<Result<Response, ZkError>, failure::Error> {
        let (tx, rx) = oneshot::channel();
        match self.0.unbounded_send((request, tx)) {
            Ok(()) => rx
                .await
                .map_err(|e| format_err!("Error processing request: {:?}", e)),
            Err(e) => Err(format_err!("failed to enqueue new request: {:?}", e)),
        }

        // TODO Map error to something more informative -- i.e. if rx channel got
        // closed, indicate that the zk state is failed/expired as necessary
    }
}
