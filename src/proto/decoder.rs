//
// Copyright 2020 Joyent, Inc.
//

use std::collections::HashMap;
use std::io::Error as IoError;
use std::sync::{Arc, Mutex};

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, BytesMut};
use futures::channel::mpsc::UnboundedSender;
use slog::{debug, info, trace, Logger};
use tokio_util::codec::Decoder;

use crate::client::ReplyRecord;
use crate::error::{InternalError, ZkError};
use crate::proto::request::{self, OpCode};
use crate::proto::response::{ReadFrom, Response, HEARTBEAT_XID, WATCH_XID};
use crate::types::watch::{Watch, WatchType, WatchedEvent, WatchedEventType};

//
// The byte length of the outermost request/response packet header. The value
// contained in this header indicates the byte length of the rest of the
// message.
//
pub(crate) const HEADER_SIZE: usize = 4;
//
// All (non-connect) responses will have at least an xid (i32), zxid (i64), and
// error (i32).
//
const MINIMUM_RESPONSE_SIZE: usize = HEADER_SIZE + 4 + 8 + 4;

pub(crate) struct ZkDecoder {
    // Global map of operations awaiting response, indexed by xid
    replies: Arc<Mutex<HashMap<i32, ReplyRecord>>>,

    // Global map of watches registered, indexed by path
    watches: Arc<Mutex<HashMap<String, Vec<Watch>>>>,

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

    // Sending end of default watch stream
    default_watcher: UnboundedSender<WatchedEvent>,
    log: Logger,
}

//
// A decoder for everything _except_ connect responses, which have their own
// semantics.
//
// Non-connect responses come with an xid, but not an opcode. We must use the
// xid to look up the opcode in our map of submitted requests, and then use that
// opcode to parse the response.
//
impl ZkDecoder {
    pub fn new(
        replies: Arc<Mutex<HashMap<i32, ReplyRecord>>>,
        watches: Arc<Mutex<HashMap<String, Vec<Watch>>>>,
        pending_watches: Arc<Mutex<HashMap<i32, (String, Watch)>>>,
        default_watcher: UnboundedSender<WatchedEvent>,
        log: Logger,
    ) -> Self {
        ZkDecoder {
            replies,
            watches,
            pending_watches,
            default_watcher,
            log,
        }
    }

    fn handle_watch_event(&mut self, mut buf: &[u8]) {
        // Deserialize the WatchedEvent
        let e = WatchedEvent::read_from(&mut buf).unwrap();
        debug!(self.log, "got watch event {:?}", e);

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
            // over the last element in the Vec.
            //
            for i in (0..watch_list.len()).rev() {
                let watch_matched = match (watch_list[i].wtype, e.event_type) {
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
                    let _ = w.tx.send(e.clone());
                } else {
                    //
                    // There were other non-global watches set on this path,
                    // but none of them matched this event.
                    //
                    global_watch = true;
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
    }

    //
    // A complicated response type to mirror the response type of the
    // ZkDecoder's decode() function. The outer result signifies whether or not
    // the actual decoding of the response failed. The inner result, which is
    // returned if the decoding succeeded, indicates whether or not the client
    // encountered some logic error.
    //
    fn handle_response(
        &mut self,
        mut buf: &[u8],
        xid: i32,
        err: Option<ZkError>,
    ) -> Result<Result<(), InternalError>, IoError> {
        // Find the waiting request
        let (opcode, tx) = match self.replies.lock().unwrap().remove(&xid) {
            Some(tuple) => tuple,
            None => {
                // TODO Should this ever happen?
                return Ok(Err(InternalError::DanglingXid(xid)));
            }
        };

        //
        // If the request tried to set a watch, we confirm that the request
        // has succeeded and register the watch if so.
        //
        if let Some((path, watch)) = self.pending_watches.lock().unwrap().remove(&xid) {
            //
            // Normally, a requested watch is only added once the initial
            // operation is successful. The one exception to this is if an
            // exists call fails with NoNode.
            //
            if err.is_none() || (opcode == request::OpCode::Exists && err == Some(ZkError::NoNode))
            {
                debug!(self.log, "pending watch turned into real watch"; "xid" => xid);
                self.watches
                    .lock()
                    .unwrap()
                    .entry(path)
                    .or_insert_with(Vec::new)
                    .push(watch);
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
                    return Err(e);
                }
            }
        }
        Ok(Ok(()))
    }
}

impl Decoder for ZkDecoder {
    //
    // The result is the zxid of the decoded message for use by the
    // SessionManager.
    //
    type Item = Result<i64, InternalError>;
    //
    // This is for unrecoverable errors.
    // The Decoder trait requires this type to be convertible to an IoError, so
    // we just use the IoError itself.
    //
    type Error = IoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        //
        // We can read from the slice and take advantage of its cursor-tracking
        // without modifying the actual BytesMut buffer
        //
        let mut buf = &src[0..src.len()];

        // See if we've received the message length yet
        if buf.len() < HEADER_SIZE {
            src.reserve(MINIMUM_RESPONSE_SIZE);
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

        // Deserialize error
        let zk_err: ZkError = buf.read_i32::<BigEndian>()?.into();
        if zk_err != ZkError::Ok {
            err = Some(zk_err);
        }

        //
        // A watch has triggered.
        //
        let result = if xid == WATCH_XID {
            assert!(err == None);
            self.handle_watch_event(buf);
            let result = Ok(zxid);
            Ok(Some(result))
        //
        // Response to ping. The response is empty beyond the xid/zxid/err.
        //
        } else if xid == HEARTBEAT_XID {
            trace!(self.log, "got response to heartbeat");
            let result = if let Some(e) = err {
                Err(InternalError::ServerError(e))
            } else {
                Ok(zxid)
            };
            Ok(Some(result))
        //
        // Response to request
        //
        } else {
            //
            // Handle the response, then convert a successful result to an
            // option and stick the zxid in it.
            //
            self.handle_response(buf, xid, err)
                .map(|item| Some(item.map(|_| zxid)))
        };

        src.advance(msg_len + HEADER_SIZE);
        src.reserve(MINIMUM_RESPONSE_SIZE);
        result
    }
}

//
// A decoder for connect responses only, which have their own unique semantics.
//
// Connect responses do not come with an xid or opcode. We must receive them
// from a context in which we _know_ that the response will be a connect
// response and not something else. Thus, it makes sense to give them their own
// decoder.
//
pub(crate) struct ZkConnDecoder {}

impl Decoder for ZkConnDecoder {
    type Item = Response;
    //
    // This is for unrecoverable errors.
    //
    type Error = InternalError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        //
        // We can read from the slice and take advantage of its cursor-tracking
        // without modifying the actual BytesMut buffer
        //
        let mut buf = &src[0..src.len()];

        // See if we've received the message length yet
        if buf.len() < HEADER_SIZE {
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

        let resp = Response::parse(OpCode::CreateSession, &mut buf)
            .map(Some)
            .map_err(InternalError::from);
        src.advance(msg_len + HEADER_SIZE);
        src.reserve(MINIMUM_RESPONSE_SIZE);
        resp
    }
}
