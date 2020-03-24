use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::BytesMut;
use futures::channel::oneshot::Sender;
use slog::{debug, info, trace, Logger};
use std::collections::HashMap;
use std::io::Error as IoError;
use std::sync::{Arc, Mutex};
use tokio_util::codec::Decoder;

use crate::error::ZkError;
use crate::proto::request::{self, OpCode, Request};
use crate::proto::response::{ReadFrom, Response};
use crate::proto::session_manager::ReconnectInfo;
use crate::types::watch::{WatchType, WatchedEvent};

const HEADER_SIZE: usize = 4;

pub(crate) struct ResponseFrame {
    xid: i32, // TODO do we end up using this?
    zxid: i64,
    error_code: u32, // TODO do we end up using this?
    inner: Response,
    reply_tx: Sender<Result<Response, ZkError>>,
}

struct ZkDecoder {
    ///
    /// Global map of operations awaiting response, indexed by xid
    ///
    replies: Arc<Mutex<HashMap<i32, (OpCode, Sender<Result<Response, ZkError>>)>>>,
    ///
    /// Global map of watches registered, indexed by path
    ///
    watches: Arc<Mutex<HashMap<String, Vec<(Sender<WatchedEvent>, WatchType)>>>>,
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
    /// `true` if the decoder hasn't decoded a message yet; false otherwise
    ///
    first: bool,
    ///
    /// A reference to the SessionManager's ReconnectInfo
    ///
    reconnect_info: Arc<Mutex<ReconnectInfo>>,
    log: Logger,
}

impl ZkDecoder {
    fn new(
        replies: Arc<Mutex<HashMap<i32, (OpCode, Sender<Result<Response, ZkError>>)>>>,
        watches: Arc<Mutex<HashMap<String, Vec<(Sender<WatchedEvent>, WatchType)>>>>,
        pending_watches: Arc<Mutex<HashMap<i32, (String, Sender<WatchedEvent>, WatchType)>>>,
        log: Logger,
    ) -> Self {
        ZkDecoder {
            replies,
            watches,
            pending_watches,
            // TODO make sure "first" gets set and reset appropriately
            first: true,
            log,
        }
    }

    fn handle_new_zxid(zxid: i64) {
        if zxid > 0 {
            let mut reconnect_info = self.reconnect_info.lock().unwrap();
            trace!(
                log,
                "updated zxid from {} to {}",
                reconnect_info.last_zxid_seen,
                zxid
            );

            assert!(zxid >= self.last_zxid_seen);
            reconnect_info.last_zxid_seen = zxid;
        }
    }
}

impl Decoder for ZkDecoder {
    type Item = Response;
    type Error = IoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        //
        // We can read from the slice and take advantage of its cursor-tracking
        // without modifying the actual BytesMut buffer
        //
        let mut buf = &src[0..HEADER_SIZE];

        // See if we've received the message length yet
        if buf.len() < HEADER_SIZE {
            // TODO figure out the minimum message length and optimize the reservation more?
            src.reserve(HEADER_SIZE);
            return Ok(None);
        }

        //
        // Parse out the message length and return if we don't have enough data
        // yet
        //
        let msg_len = buf.read_i32::<BigEndian>()? as usize;
        if buf.len() < msg_len {
            src.reserve(msg_len);
            return Ok(None);
        }

        let mut err = None;

        //
        // The response will be a connect response if and only if this is the
        // first response we've received. If this is the first response we've
        // received, the xid we've stored it with will be 0.
        //
        let xid = if self.first {
            0
        } else {
            // Deserialize xid and zxid
            let xid = buf.read_i32::<BigEndian>()?;
            let zxid = buf.read_i64::<BigEndian>()?;

            self.handle_new_zxid(zxid);

            // Deserialize error
            let zk_err: ZkError = buf.read_i32::<BigEndian>()?.into();
            if zk_err != ZkError::Ok {
                err = Some(zk_err);
            }
            xid
        };

        //
        // Response to shutdown. The response is empty beyond the xid/zxid/err.
        // In theory, the server should now shut down its end of the connection.
        //
        if xid == 0 && !self.first {
            trace!(self.log, "got response to CloseSession");
            if let Some(e) = err {
                bail!("failed to close session: {:?}", e);
            }
        //
        // A watch has triggered.
        //
        } else if xid == -1 {
            // Deserialize the WatchedEvent
            let e = WatchedEvent::read_from(&mut buf)?;
            trace!(self.log, "got watch event {:?}", e);

            let mut remove = false;

            // Check for any custom watches set by the user and notify them
            if let Some(watches) = self.watches.get_mut(&e.path) {
                trace!(self.log,
                       "found potentially waiting custom watchers";
                       "n" => watches.len()
                );

                //
                //
                let mut i = (watches.len() - 1) as isize;
                while i >= 0 {
                    let watch_matched = match (&watches[i as usize].1, e.event_type) {
                        (WatchType::Child, WatchedEventType::NodeDeleted)
                        | (WatchType::Child, WatchedEventType::NodeChildrenChanged) => true,
                        (WatchType::Child, _) => false,
                        (WatchType::Data, WatchedEventType::NodeDeleted)
                        | (WatchType::Data, WatchedEventType::NodeDataChanged) => true,
                        (WatchType::Data, _) => false,
                        (WatchType::Exist, WatchedEventType::NodeChildrenChanged) => false,
                        (WatchType::Exist, _) => true,
                    };

                    if watch_matched {
                        // this watcher is no longer active
                        let w = watchers.swap_remove(i as usize);
                        // NOTE: ignore the case where the receiver has been dropped
                        let _ = w.0.send(e.clone());
                    }
                    i -= 1;
                }

                if watchers.is_empty() {
                    remove = true;
                }
            }

            if remove {
                self.watchers
                    .remove(&e.path)
                    .expect("tried to remove watcher that didn't exist");
            }

            // NOTE: ignoring error, because the user may not care about events
            let _ = default_watcher.unbounded_send(e);
        } else if xid == -2 {
            // response to ping -- empty response
            trace!(logger, "got response to heartbeat");
            if let Some(e) = err {
                bail!("bad response to ping: {:?}", e);
            }
        } else {
            // response to user request
            self.first = false;

            // find the waiting request future
            let (opcode, tx) = match self.reply.remove(&xid) {
                Some(tuple) => tuple,
                None => bail!("No waiting request future found for xid {:?}", xid),
            };

            if let Some(w) = self.pending_watchers.remove(&xid) {
                // normally, watches are *only* added for successful operations
                // the exception to this is if an exists call fails with NoNode
                if err.is_none()
                    || (opcode == request::OpCode::Exists && err == Some(ZkError::NoNode))
                {
                    trace!(logger, "pending watcher turned into real watcher"; "xid" => xid);
                    self.watchers
                        .entry(w.0)
                        .or_insert_with(Vec::new)
                        .push((w.1, w.2));
                } else {
                    trace!(logger,
                           "pending watcher not turned into real watcher: {:?}",
                           err;
                           "xid" => xid
                    );
                }
            }

            if let Some(e) = err {
                info!(logger,
                       "handling server error response: {:?}", e;
                       "xid" => xid, "opcode" => ?opcode);

                tx.send(Err(e)).is_ok();
            } else {
                let mut r = Response::parse(opcode, &mut buf)?;

                debug!(logger,
                       "handling server response: {:?}", r;
                       "xid" => xid, "opcode" => ?opcode);

                if let Response::Connect {
                    timeout,
                    session_id,
                    ref mut password,
                    ..
                } = r
                {
                    assert!(timeout >= 0);
                    trace!(logger, "negotiated session timeout: {}ms", timeout);

                    self.timeout = time::Duration::from_millis(2 * timeout as u64 / 3);
                    self.timer.reset(time::Instant::now() + self.timeout);

                    // keep track of these for consistent re-connect
                    self.session_id = session_id;
                    mem::swap(&mut self.password, password);
                }

                tx.send(Ok(r)).is_ok(); // if receiver doesn't care, we don't either
            }
        }

        if self.instart == self.inbox.len() {
            self.inbox.clear();
            self.instart = 0;
        }

        // TODO some activepacketizer logic ends up here; some ends up in other places --
        // make sure we've accounted for all of it!

        // TODO remember to truncate the src buffer ourselves upon success (using src.advance() probably)

        // TODO remember, later, that when we search for the waiting future for a connect response in the
        // reply map, it will have an artificial xid of 0

        // TODO make sure to check later that the response type matches the
        // expected opcode from the map
        Ok(None)

        // TODO add logging everywhere; uncomment existing log lines
    }
}
