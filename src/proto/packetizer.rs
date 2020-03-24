use byteorder::{BigEndian, WriteBytesExt};
use failure;
use failure::format_err;

use futures::{
    channel::{mpsc, oneshot},
    future::{Either, TryFutureExt},
    lock::Mutex,
    ready, select,
    stream::StreamExt,
    task::Poll,
    Future,
};
use slog;
use slog::{debug, error, trace};
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio;
use tokio::prelude::*;

use super::{request, Request, Response, ZooKeeperTransport};
use crate::error::ZkError;
use crate::proto::active_packetizer::ActivePacketizer;
use crate::proto::session_manager::SessionManager;
use crate::types::watch::{WatchType, WatchedEvent};
use crate::Watch;

pub(crate) struct Packetizer {
    /// Incoming requests
    rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Result<Response, ZkError>>)>,

    /// Next xid to issue
    xid: i32,

    logger: slog::Logger,

    exiting: bool,
    sess_mgr: Arc<Mutex<SessionManager>>, // /// Current state
    // state: PacketizerState<S>
    active_packetizer: ActivePacketizer,
}

// TODO use tokio_util::codec for packetization

struct Writer {
    /// Incoming requests
    rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Result<Response, ZkError>>)>,
    /// Next xid to issue
    next_xid: i32,
    sess_mgr: Arc<Mutex<SessionManager>>, // /// Current state
}

struct Reader {
    sess_mgr: Arc<Mutex<SessionManager>>, // /// Current state
}

impl Packetizer {
    pub(crate) fn start(addr: SocketAddr, log: slog::Logger) -> Enqueuer {
        let (tx, rx) = mpsc::unbounded();

        let exitlogger = log.clone();

        // TODO turns out the zk protocol mandates a default watcher -- so we
        // need one!

        tokio::task::spawn({
            let p = Packetizer {
                // state: PacketizerState::Connected(ActivePacketizer::new(stream)),
                xid: 0,
                rx,
                logger: log,
                exiting: false,
                sess_mgr: Arc::new(Mutex::new(SessionManager::new(addr))),
                active_packetizer: ActivePacketizer::new(),
            };
            p.run()
            // .map_err(move |e| {
            //     error!(exitlogger, "packetizer exiting: {:?}", e);
            //     drop(e);
            // }),
        });

        Enqueuer(tx)
    }

    async fn run(mut self) {
        loop {
            // TODO handle error getting conn here instead of unwrapping
            // let mut handle = self.sess_mgr.lock().await;
            // let conn = handle.get_rx().await.unwrap();
            // drop(handle);
            // println!("{:?}", conn);
            // select! {
            //     //
            //     // Incoming bytes
            //     //
            //     conn.peek
            // }
        }
    }
}

#[allow(dead_code)]
enum PacketizerState {
    Connected(ActivePacketizer),
    Reconnecting(Box<Future<Output = Result<ActivePacketizer, failure::Error>> + Send + 'static>),
}

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

// impl<S> Packetizer<S>
// where
//     S: ZooKeeperTransport,
// {
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
// }

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
pub(crate) struct Enqueuer(
    mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response, ZkError>>)>,
);

impl Enqueuer {
    // fn new()
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

        // Map error to something more informative -- i.e. if rx channel got
        // closed, indicate that the zk state is failed/expired as necessary
    }
}
