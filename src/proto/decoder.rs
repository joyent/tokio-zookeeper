use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, BytesMut};
use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::Sender;
use slog::{debug, info, trace, Logger};
use std::collections::HashMap;
use std::io::Error as IoError;
use std::mem;
use std::sync::{Arc, Mutex};
use tokio_util::codec::Decoder;

use crate::error::{InternalError, ZkError};
use crate::proto::request::{self, OpCode};
use crate::proto::response::{ReadFrom, Response};
use crate::session_manager::SessionInfo;
use crate::types::watch::{WatchType, WatchedEvent, WatchedEventType};

//
// The byte length of the outermost request/response packet header. The value
// contained in this header indicates the byte length of the rest of the
// message.
//
pub const HEADER_SIZE: usize = 4;

// TODO pick between std mutex and futures mutex and only use one

pub(crate) struct ZkDecoder {
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
    /// A reference to the SessionManager's SessionInfo
    ///
    session_info: Arc<Mutex<SessionInfo>>,
    log: Logger,
}

impl ZkDecoder {
    pub fn new(
        replies: Arc<Mutex<HashMap<i32, (OpCode, Sender<Result<Response, ZkError>>)>>>,
        watches: Arc<Mutex<HashMap<String, Vec<(Sender<WatchedEvent>, WatchType)>>>>,
        pending_watches: Arc<Mutex<HashMap<i32, (String, Sender<WatchedEvent>, WatchType)>>>,
        default_watcher: UnboundedSender<WatchedEvent>,
        session_info: Arc<Mutex<SessionInfo>>,
        log: Logger,
    ) -> Self {
        ZkDecoder {
            replies,
            watches,
            pending_watches,
            default_watcher,
            session_info,
            log,
        }
    }

    fn handle_new_zxid(&self, zxid: i64) {
        if zxid > 0 {
            let mut session_info = self.session_info.lock().unwrap();
            trace!(
                self.log,
                "updated zxid from {} to {}",
                session_info.last_zxid_seen,
                zxid
            );

            assert!(zxid >= session_info.last_zxid_seen);
            session_info.last_zxid_seen = zxid;
        }
    }
}

impl Decoder for ZkDecoder {
    type Item = Result<(), InternalError>;
    //
    // This is for unrecoverable errors.
    // The Decoder trait requires this type to be convertible to an IoError, so
    // we just use the IoError itself.
    //
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

        // Deserialize xid and zxid
        let xid = buf.read_i32::<BigEndian>()?;
        let zxid = buf.read_i64::<BigEndian>()?;

        self.handle_new_zxid(zxid);

        // Deserialize error
        let zk_err: ZkError = buf.read_i32::<BigEndian>()?.into();
        if zk_err != ZkError::Ok {
            err = Some(zk_err);
        }

        //
        // Response to shutdown. The response is empty beyond the xid/zxid/err.
        // In theory, the server should now shut down its end of the connection.
        //
        if xid == 0 {
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
            println!("heartbeat responded to");
            trace!(self.log, "got response to heartbeat");
            if let Some(e) = err {
                src.advance(msg_len + HEADER_SIZE);
                return wrap(Err(InternalError::ServerError(e)));
            }
        //
        // Response to user request
        //
        } else {
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
            // TODO do we leak pending watches somehow if a request gets interrupted?
            // I think we don't, assuming we clear all pending watches upon reconnect,
            // because an interrupted request will always trigger a reconnect. I'm not sure.
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
                    Ok(r) => {
                        debug!(self.log,
                               "handling server response: {:?}", r;
                               "xid" => xid, "opcode" => ?opcode);
                        if let Response::Connect { .. } = r {
                            panic!("Regular ZkDecoder received connect response");
                        }
                        tx.send(Ok(r)).expect("Internal rx for response dropped");
                    }
                    Err(e) => {
                        src.advance(msg_len + HEADER_SIZE);
                        return Err(e);
                    }
                }
            }
        }
        src.advance(msg_len + HEADER_SIZE);
        // Set aside space for the next frame (TODO optimize for minimum actual frame size?)
        src.reserve(HEADER_SIZE);
        wrap(Ok(()))

        // TODO some activepacketizer logic ends up here; some ends up in other places --
        // make sure we've accounted for all of it!

        // TODO add logging everywhere; uncomment existing log lines

        // TODO implement decode_eof?

        // TODO write a comment explaining all the various nested errors and control flow here
        // TODO handle keeperstate somehow? Not sure tbh
        // TODO send state changes over default watcher? The existing impl seems
        // to only return state in response to API requests (as either errors or KeeperState enum), which makes
        // sense I guess, as these requests are the only time the user interacts with the server.
    }
}

pub(crate) struct ZkConnDecoder {
    ///
    /// tx over which to send response
    ///
    response_tx: Option<Sender<Result<Response, ZkError>>>,
    log: Logger,
}

impl ZkConnDecoder {
    pub fn new(response_tx: Sender<Result<Response, ZkError>>, log: Logger) -> Self {
        ZkConnDecoder {
            response_tx: Some(response_tx),
            log,
        }
    }
}

impl Decoder for ZkConnDecoder {
    type Item = Result<(), InternalError>;
    //
    // This is for unrecoverable errors. We never actually return this.
    // The Decoder trait requires this type to be convertible to an IoError, so
    // we just use the IoError itself.
    //
    type Error = IoError;

    // TODO refactor this big boy function
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
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

        //
        // The ZkConnDecoder is a one-shot deal. It shouldn't be used to decode
        // more than one message. TODO if this changes, make sure we advance() and reserve() the buffer.
        //
        let response_tx = mem::replace(&mut self.response_tx, None)
            .expect("ZkConnDecoder has received more than one message");

        let response = Response::parse(OpCode::CreateSession, &mut buf).map(|r| {
            debug!(self.log, "handling server connect response: {:?}", r);
            response_tx
                .send(Ok(r))
                .expect("Internal rx for response dropped");
            Some(Ok(()))
        });
        response
        // TODO implement decode_eof?
    }
}
