use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BytesMut};
use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::Sender;
use slog::{debug, error, info, trace, Logger};
use std::collections::HashMap;
use std::io::Error as IoError;
use std::sync::{Arc, Mutex};
use tokio_util::codec::Decoder;

use crate::error::{InternalError, ZkError};
use crate::proto::request::{self, OpCode, Request};
use crate::proto::response::{ReadFrom, Response};
use crate::proto::session_manager::ReconnectInfo;
use crate::types::watch::{WatchType, WatchedEvent, WatchedEventType};

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
    /// Sending end of default watch stream
    ///
    default_watcher: UnboundedSender<WatchedEvent>,
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
        default_watcher: UnboundedSender<WatchedEvent>,
        reconnect_info: Arc<Mutex<ReconnectInfo>>,
        log: Logger,
    ) -> Self {
        ZkDecoder {
            replies,
            watches,
            pending_watches,
            default_watcher,
            // TODO make sure "first" gets set and reset appropriately
            first: true,
            reconnect_info,
            log,
        }
    }

    fn handle_new_zxid(&self, zxid: i64) {
        if zxid > 0 {
            let mut reconnect_info = self.reconnect_info.lock().unwrap();
            trace!(
                self.log,
                "updated zxid from {} to {}",
                reconnect_info.last_zxid_seen,
                zxid
            );

            assert!(zxid >= reconnect_info.last_zxid_seen);
            reconnect_info.last_zxid_seen = zxid;
        }
    }
}

impl Decoder for ZkDecoder {
    type Item = Result<(), InternalError>;
    // This is for unrecoverable errors. We don't actually return this.
    // TODO change the type to () or something?
    type Error = IoError;

    // TODO refactor this big boy function
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        fn wrap<T, E>(item: T) -> Result<Option<T>, E> {
            Ok(Some(item))
        }

        //
        // We can read from the slice and take advantage of its cursor-tracking
        // without modifying the actual BytesMut buffer
        //
        let mut buf = &src[0..src.len()];

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
            self.first = false;
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
                src.advance(msg_len + HEADER_SIZE);
                return wrap(Err(InternalError::ServerError(e)));
            }
        //
        // A watch has triggered.
        //
        } else if xid == -1 {
            // Deserialize the WatchedEvent
            let e = WatchedEvent::read_from(&mut buf)?;
            trace!(self.log, "got watch event {:?}", e);

            let mut watches = self.watches.lock().unwrap();
            let mut remove_watch_list = false;
            let mut global_watch = false;

            // Check for any custom watches set by the user and notify them
            if let Some(watch_list) = watches.get_mut(&e.path) {
                trace!(self.log,
                       "found potentially waiting custom watchers";
                       "n" => watch_list.len()
                );

                //
                // Iterate throught the watches and find any that the event
                // matches. If we find a match, remove the watch from the Vec.
                //
                // We iterate in reverse so we can safely use swap_remove() for
                // removal from the Vec, knowing that we've already iterated
                // over the last element in the Vec. This is pretty optimize-y.
                // Is it worth doing? (TODO)
                //
                for i in (0..watch_list.len()).rev() {
                    let watch_matched = match (watch_list[i].1, e.event_type) {
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
                        let w = watch_list.swap_remove(i);
                        //
                        // We send without worrying about the result, because
                        // the user may have dropped the receiver, and that's
                        // ok.
                        //
                        let _ = w.0.send(e.clone());
                    }
                }
                if watch_list.is_empty() {
                    remove_watch_list = true;
                }
            } else {
                global_watch = true;
            }

            if remove_watch_list {
                watches.remove(&e.path).expect(
                    "tried to remove a watch list for a path with no \
                     registered watches",
                );
            }

            if global_watch {
                //
                // TODO as far as I can tell, the original client send watch
                // events over the default watcher _even if_ there was a custom
                // watcher sent. Do we want to do this? Probably not.
                //
                // We ignore any send errors, because the user may have dropped
                // the default watcher, and that's ok.
                //
                let _ = self.default_watcher.unbounded_send(e);
            }
        //
        // TODO remove magic numbers like -2 here
        // Response to ping. The response is empty beyond the xid/zxid/err.
        //
        } else if xid == -2 {
            trace!(self.log, "got response to heartbeat");
            if let Some(e) = err {
                src.advance(msg_len + HEADER_SIZE);
                return wrap(Err(InternalError::ServerError(e)));
            }
        //
        // Response to user request
        //
        } else {
            // TODO is it ok that I moved this to the if statement above?
            // self.first = false;

            // Find the waiting request
            let (opcode, tx) = match self.replies.lock().unwrap().remove(&xid) {
                Some(tuple) => tuple,
                None => {
                    // TODO what to do here? Should this ever happen?
                    src.advance(msg_len + HEADER_SIZE);
                    return wrap(Err(InternalError::DanglingXid(xid)));
                }
            };

            //
            // If the request tried to set a watch, we confirm that the request
            // has succeeded and register the watch if so.
            //
            if let Some(w) = self.pending_watches.lock().unwrap().remove(&xid) {
                //
                // Normally, a requested watch is only added once the initial
                // operation is successful. The one exception to this is if an
                // exists call fails with NoNode.
                //
                if err.is_none()
                    || (opcode == request::OpCode::Exists && err == Some(ZkError::NoNode))
                {
                    debug!(self.log, "pending watch turned into real watch"; "xid" => xid);
                    self.watches
                        .lock()
                        .unwrap()
                        .entry(w.0)
                        .or_insert_with(Vec::new)
                        .push((w.1, w.2));
                } else {
                    debug!(self.log,
                           "pending watch not turned into real watch: {:?}",
                           err;
                           "xid" => xid
                    );
                }
            }

            //
            // We send the ZkError or successful response data to the user.
            //
            if let Some(e) = err {
                info!(self.log,
                       "handling server error response: {:?}", e;
                       "xid" => xid, "opcode" => ?opcode);
                tx.send(Err(e)).expect("Internal rx for response dropped");
            } else {
                match Response::parse(opcode, &mut buf) {
                    Ok(mut r) => {
                        debug!(self.log,
                               "handling server response: {:?}", r;
                               "xid" => xid, "opcode" => ?opcode);
                        if let Response::Connect {
                            timeout,
                            session_id,
                            ref mut password,
                            ..
                        } = r
                        {
                            // TODO handle session mgr stuff
                            // assert!(timeout >= 0);
                            // trace!(logger, "negotiated session timeout: {}ms", timeout);

                            // self.timeout = time::Duration::from_millis(2 * timeout as u64 / 3);
                            // self.timer.reset(time::Instant::now() + self.timeout);

                            // // keep track of these for consistent re-connect
                            // self.session_id = session_id;
                            // mem::swap(&mut self.password, password);
                        }
                        tx.send(Ok(r)).expect("Internal rx for response dropped");
                    }
                    Err(e) => {
                        src.advance(msg_len + HEADER_SIZE);
                        // TODO impl From<IoError> for InternalError instead?
                        return wrap(Err(InternalError::MalformedResponse(e)));
                    }
                }
            }
        }
        src.advance(msg_len + HEADER_SIZE);
        wrap(Ok(()))

        // TODO some activepacketizer logic ends up here; some ends up in other places --
        // make sure we've accounted for all of it!

        // TODO remember to truncate the src buffer ourselves upon success (using src.advance() probably)

        // TODO remember, later, that when we search for the waiting future for a connect response in the
        // reply map, it will have an artificial xid of 0

        // TODO add logging everywhere; uncomment existing log lines

        // TODO implement decode_eof?

        // TODO write a comment explaining all the various nested errors and control flow here
    }
}
