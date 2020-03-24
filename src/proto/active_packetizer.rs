// use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
// use failure;
// use failure::bail;
// use futures::channel::{mpsc, oneshot};
// use futures::future::{Fuse, FutureExt};
// use futures::ready;
// use futures::task::Poll;
// use slog;
// use slog::{debug, info, trace};
// use std::collections::HashMap;
// use std::mem;
// use tokio;
// use tokio::net::TcpStream;
// use tokio::prelude::*;
// use tokio::time::{self, Delay, Duration};

// use super::{request, Request, Response};
// use crate::error::ZkError;
// use crate::types::watch::{WatchType, WatchedEvent, WatchedEventType};

// pub(super) struct ActivePacketizer {
//     /// Heartbeat timer
//     timer: Fuse<Delay>,
//     timeout: Duration,

//     /// Bytes we have not yet sent
//     pub(super) outbox: Vec<u8>,

//     /// Index of first byte not yet sent
//     outstart: usize,

//     /// Bytes we have not yet deserialized
//     /// TODO I'd like this to be a fixed size, but we can receive arbitrary-length
//     /// messages, so I guess we have to keep it a Vec?
//     inbox: Vec<u8>,

//     /// Index of first byte not yet deserialized
//     instart: usize,

//     /// Set of operations awaiting response, indexed by xid
//     reply: HashMap<i32, (request::OpCode, oneshot::Sender<Result<Response, ZkError>>)>,

//     /// Set of watches registered, indexed by path
//     watchers: HashMap<String, Vec<(oneshot::Sender<WatchedEvent>, WatchType)>>,

//     /// Watches are only registered once we have confirmed that the operation
//     /// that initiated the watch did not fail. Thust, we must stage watches here
//     /// first. The map is indexed by xid.
//     ///
//     /// The one exception: a watch can still be added if a call to exists()
//     /// fails because the node does not exist yet.
//     pub(super) pending_watchers: HashMap<i32, (String, oneshot::Sender<WatchedEvent>, WatchType)>,

//     // TODO what is this for?
//     first: bool,

//     /// Fields for re-connection
//     pub(super) last_zxid_seen: i64,
//     pub(super) session_id: i64,
//     pub(super) password: Vec<u8>,
// }

// impl ActivePacketizer {
//     pub(super) fn new() -> Self {
//         ActivePacketizer {
//             timer: time::delay_for(Duration::from_secs(86_400)).fuse(),
//             // TODO propagate this to be the actual passed-in session timeout
//             timeout: Duration::from_secs(86_400),
//             outbox: Vec::new(),
//             outstart: 0,
//             inbox: Vec::new(),
//             instart: 0,
//             reply: Default::default(),
//             watchers: Default::default(),
//             pending_watchers: Default::default(),
//             first: true,

//             last_zxid_seen: 0,
//             session_id: 0,
//             password: Vec::new(),
//         }
//     }

// fn outlen(&self) -> usize {
//     self.outbox.len() - self.outstart
// }

// fn inlen(&self) -> usize {
//     self.inbox.len() - self.instart
// }

// pub async fn write(&self, conn: &TcpStream) {
//     // select! {}
// }
/*
    async fn write_out() {
        select! {
            heartbeat timer -> send heartbeat
            queued request -> serialize and send request, reset heartbeat
        }
    }
*/
/*
    async fn read_in() {
        read from the stream
        parse that babby up and handle accordingly
    }
*/

//     pub(super) fn enqueue(
//         &mut self,
//         xid: i32,
//         item: Request,
//         tx: oneshot::Sender<Result<Response, ZkError>>,
//     ) {
//         let lengthi = self.outbox.len();
//         // dummy length
//         self.outbox.push(0);
//         self.outbox.push(0);
//         self.outbox.push(0);
//         self.outbox.push(0);

//         let old = self.reply.insert(xid, (item.opcode(), tx));
//         assert!(old.is_none());

//         if let Request::Connect { .. } = item {
//         } else {
//             // xid
//             self.outbox
//                 .write_i32::<BigEndian>(xid)
//                 .expect("Vec::write should never fail");
//             // opcode
//             self.outbox
//                 .write_i32::<BigEndian>(item.opcode() as i32)
//                 .expect("Vec::write should never fail");
//         }

//         // type and payload
//         item.serialize_into(&mut self.outbox)
//             .expect("Vec::write should never fail");
//         // set true length
//         let written = self.outbox.len() - lengthi - 4;
//         let mut length = &mut self.outbox[lengthi..lengthi + 4];
//         length
//             .write_i32::<BigEndian>(written as i32)
//             .expect("Vec::write should never fail");
//     }

//     fn poll_write(
//         &mut self,
//         exiting: bool,
//         logger: &mut slog::Logger,
//     ) -> Result<Poll<()>, failure::Error>
//     where
//         S: AsyncWrite,
//     {
//         let mut wrote = false;
//         while self.outlen() != 0 {
//             let n = ready!(self.stream.poll_write(&self.outbox[self.outstart..]));
//             wrote = true;
//             self.outstart += n;
//             if self.outstart == self.outbox.len() {
//                 self.outbox.clear();
//                 self.outstart = 0;
//             }
//         }

//         if wrote {
//             // heartbeat timer restarts upon a non-heartbeat write
//             trace!(logger, "resetting heartbeat timer");
//             self.timer.reset(time::Instant::now() + self.timeout);
//         }

//         self.stream.poll_flush().map_err(failure::Error::from)?;

//         if exiting {
//             debug!(logger, "shutting down writer");
//             ready!(self.stream.shutdown());
//         }

//         Ok(Poll::Ready(()))
//     }

//     fn poll_read(
//         &mut self,
//         default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
//         logger: &mut slog::Logger,
//     ) -> Result<Poll<()>, failure::Error>
//     where
//         S: AsyncRead,
//     {
//         loop {
//             let mut need = if self.inlen() >= 4 {
//                 let length = (&mut &self.inbox[self.instart..]).read_i32::<BigEndian>()? as usize;
//                 length + 4
//             } else {
//                 4
//             };
//             trace!(logger, "need {} bytes, have {}", need, self.inlen());

//             while self.inlen() < need {
//                 let read_from = self.inbox.len();
//                 self.inbox.resize(self.instart + need, 0);
//                 match self.stream.poll_read(&mut self.inbox[read_from..])? {
//                     Poll::Ready(n) => {
//                         self.inbox.truncate(read_from + n);
//                         if n == 0 {
//                             if self.inlen() != 0 {
//                                 bail!(
//                                     "connection closed with {} bytes left in buffer: {:x?}",
//                                     self.inlen(),
//                                     &self.inbox[self.instart..]
//                                 );
//                             } else {
//                                 // Server closed session with no bytes left in buffer
//                                 debug!(logger, "server closed connection");
//                                 return Ok(Poll::Ready(()));
//                             }
//                         }

//                         if self.inlen() >= 4 && need == 4 {
//                             let length = (&mut &self.inbox[self.instart..])
//                                 .read_i32::<BigEndian>()?
//                                 as usize;
//                             need += length;
//                         }
//                     }
//                     Poll::Pending => {
//                         self.inbox.truncate(read_from);
//                         return Ok(Poll::Pending);
//                     }
//                 }
//             }

//             {
//                 let mut err = None;
//                 let mut buf = &self.inbox[self.instart + 4..self.instart + need];
//                 self.instart += need;

//                 let xid = if self.first {
//                     0
//                 } else {
//                     let xid = buf.read_i32::<BigEndian>()?;
//                     let zxid = buf.read_i64::<BigEndian>()?;
//                     if zxid > 0 {
//                         trace!(
//                             logger,
//                             "updated zxid from {} to {}",
//                             self.last_zxid_seen,
//                             zxid
//                         );

//                         assert!(zxid >= self.last_zxid_seen);
//                         self.last_zxid_seen = zxid;
//                     }
//                     let zk_err: ZkError = buf.read_i32::<BigEndian>()?.into();
//                     if zk_err != ZkError::Ok {
//                         err = Some(zk_err);
//                     }
//                     xid
//                 };

//                 if xid == 0 && !self.first {
//                     // response to shutdown -- empty response
//                     // XXX: in theory, server should now shut down receive end
//                     trace!(logger, "got response to CloseSession");
//                     if let Some(e) = err {
//                         bail!("failed to close session: {:?}", e);
//                     }
//                 } else if xid == -1 {
//                     // watch event
//                     use super::response::ReadFrom;
//                     let e = WatchedEvent::read_from(&mut buf)?;
//                     trace!(logger, "got watcher event {:?}", e);

//                     let mut remove = false;
//                     if let Some(watchers) = self.watchers.get_mut(&e.path) {
//                         // custom watchers were set by the user -- notify them
//                         let mut i = (watchers.len() - 1) as isize;
//                         trace!(logger,
//                                "found potentially waiting custom watchers";
//                                "n" => watchers.len()
//                         );

//                         while i >= 0 {
//                             let triggers = match (&watchers[i as usize].1, e.event_type) {
//                                 (WatchType::Child, WatchedEventType::NodeDeleted)
//                                 | (WatchType::Child, WatchedEventType::NodeChildrenChanged) => true,
//                                 (WatchType::Child, _) => false,
//                                 (WatchType::Data, WatchedEventType::NodeDeleted)
//                                 | (WatchType::Data, WatchedEventType::NodeDataChanged) => true,
//                                 (WatchType::Data, _) => false,
//                                 (WatchType::Exist, WatchedEventType::NodeChildrenChanged) => false,
//                                 (WatchType::Exist, _) => true,
//                             };

//                             if triggers {
//                                 // this watcher is no longer active
//                                 let w = watchers.swap_remove(i as usize);
//                                 // NOTE: ignore the case where the receiver has been dropped
//                                 let _ = w.0.send(e.clone());
//                             }
//                             i -= 1;
//                         }

//                         if watchers.is_empty() {
//                             remove = true;
//                         }
//                     }

//                     if remove {
//                         self.watchers
//                             .remove(&e.path)
//                             .expect("tried to remove watcher that didn't exist");
//                     }

//                     // NOTE: ignoring error, because the user may not care about events
//                     let _ = default_watcher.unbounded_send(e);
//                 } else if xid == -2 {
//                     // response to ping -- empty response
//                     trace!(logger, "got response to heartbeat");
//                     if let Some(e) = err {
//                         bail!("bad response to ping: {:?}", e);
//                     }
//                 } else {
//                     // response to user request
//                     self.first = false;

//                     // find the waiting request future
//                     let (opcode, tx) = match self.reply.remove(&xid) {
//                         Some(tuple) => tuple,
//                         None => bail!("No waiting request future found for xid {:?}", xid),
//                     };

//                     if let Some(w) = self.pending_watchers.remove(&xid) {
//                         // normally, watches are *only* added for successful operations
//                         // the exception to this is if an exists call fails with NoNode
//                         if err.is_none()
//                             || (opcode == request::OpCode::Exists && err == Some(ZkError::NoNode))
//                         {
//                             trace!(logger, "pending watcher turned into real watcher"; "xid" => xid);
//                             self.watchers
//                                 .entry(w.0)
//                                 .or_insert_with(Vec::new)
//                                 .push((w.1, w.2));
//                         } else {
//                             trace!(logger,
//                                    "pending watcher not turned into real watcher: {:?}",
//                                    err;
//                                    "xid" => xid
//                             );
//                         }
//                     }

//                     if let Some(e) = err {
//                         info!(logger,
//                                "handling server error response: {:?}", e;
//                                "xid" => xid, "opcode" => ?opcode);

//                         tx.send(Err(e)).is_ok();
//                     } else {
//                         let mut r = Response::parse(opcode, &mut buf)?;

//                         debug!(logger,
//                                "handling server response: {:?}", r;
//                                "xid" => xid, "opcode" => ?opcode);

//                         if let Response::Connect {
//                             timeout,
//                             session_id,
//                             ref mut password,
//                             ..
//                         } = r
//                         {
//                             assert!(timeout >= 0);
//                             trace!(logger, "negotiated session timeout: {}ms", timeout);

//                             self.timeout = time::Duration::from_millis(2 * timeout as u64 / 3);
//                             self.timer.reset(time::Instant::now() + self.timeout);

//                             // keep track of these for consistent re-connect
//                             self.session_id = session_id;
//                             mem::swap(&mut self.password, password);
//                         }

//                         tx.send(Ok(r)).is_ok(); // if receiver doesn't care, we don't either
//                     }
//                 }
//             }

//             if self.instart == self.inbox.len() {
//                 self.inbox.clear();
//                 self.instart = 0;
//             }
//         }
//     }

//     pub(super) fn poll(
//         &mut self,
//         exiting: bool,
//         logger: &mut slog::Logger,
//         default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
//     ) -> Result<Poll<()>, failure::Error> {
//         trace!(logger, "poll_read");
//         let r = self.poll_read(default_watcher, logger)?;

//         if let Poll::Ready(()) = self.timer.poll()? {
//             if self.outbox.is_empty() {
//                 // send a ping!
//                 // length is known for pings
//                 self.outbox
//                     .write_i32::<BigEndian>(8)
//                     .expect("Vec::write should never fail");
//                 // xid
//                 self.outbox
//                     .write_i32::<BigEndian>(-2)
//                     .expect("Vec::write should never fail");
//                 // opcode
//                 self.outbox
//                     .write_i32::<BigEndian>(request::OpCode::Ping as i32)
//                     .expect("Vec::write should never fail");
//                 trace!(logger, "sending heartbeat");
//             } else {
//                 // already request in flight, so no need to also send heartbeat
//             }

//             self.timer.reset(time::Instant::now() + self.timeout);
//         }

//         trace!(logger, "poll_write");
//         let w = self.poll_write(exiting, logger)?;

//         match (r, w) {
//             (Poll::Ready(()), Poll::Ready(())) if exiting => {
//                 debug!(logger, "packetizer done");
//                 Ok(Poll::Ready(()))
//             }
//             (Poll::Ready(()), Poll::Ready(())) => {
//                 bail!("Not exiting, but server closed connection")
//             }
//             (Poll::Ready(()), _) => bail!("outstanding requests, but response channel closed"),
//             _ => Ok(Poll::Pending),
//         }
//     }
// }
