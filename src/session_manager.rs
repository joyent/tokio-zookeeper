use futures::channel::oneshot;
use futures::lock::Mutex as AsyncMutex;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use slog::{info, Logger};
use std::io::Error as IoError;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{cmp, i32, mem, u64};
use tokio::io::{self, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::proto::decoder::ZkConnDecoder;
use crate::proto::encoder::{RequestWrapper, ZkEncoder};
use crate::proto::request::Request;
use crate::proto::response::Response;

//
// See: https://genius.com/Built-to-spill-randy-described-eternity-lyrics
//
pub(crate) const LONG_TIMEOUT: Duration = Duration::from_secs(u64::MAX);

///
/// Fields for re-connection
///
#[derive(Debug, Clone)]
pub(crate) struct SessionInfo {
    pub(crate) protocol_version: i32,
    pub(crate) last_zxid_seen: i64,
    pub(crate) session_id: i64,
    pub(crate) password: Vec<u8>,
    pub(crate) timeout: i32,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) read_only: bool,
}

impl SessionInfo {
    fn new(session_timeout: Duration, read_only: bool) -> Self {
        //
        // Squash the user-specified timeout to an i32, which is what is
        // expected by the zk protocol.
        //
        // We have to do an explicit conversion rather than just a cast because
        // casting to a smaller data length _truncates_ the data rather than
        // rounding it down.
        //
        // For example, if we were casting from 5 bits to 4, we'd want 16
        // (10000) to be rounded to 15 (1111) rather than truncated to 0 (0000).
        //
        // Note that it doesn't matter if the user-specified timeout gets
        // squashed, because the maximum timeout negotiable with the server will
        // fit in an i32 anyway.
        //
        let timeout = cmp::min(i32::MAX as u128, session_timeout.as_millis()) as i32;

        //
        // The starting values are immediately overwritten by the response to
        // the first connect request, so they don't really matter.
        //
        // There is one exception: we set the heartbeat interval to something
        // very long so we don't try to send a heartbeat before the initial
        // connect request even gets sent. The heartbeat interval will then get
        // overwritten in response to the server's connect response.
        //
        SessionInfo {
            protocol_version: 0,
            last_zxid_seen: 0,
            session_id: 0,
            password: Vec::new(),
            timeout,
            heartbeat_interval: LONG_TIMEOUT,
            read_only,
        }
    }
}

// #[derive(Debug)]
// enum SessionState {
//     Connecting,
//     Connected,
//     Closed,
//     AuthFailed,
// }

// #[derive(Debug)]
// enum SessionEvent {
//     Connected,
//     Disconnected,
//     CloseCalled,
//     SessionExpired,
//     AuthFailed,
// }

// struct StateMachine {
//     state: SessionState,
// }

// impl StateMachine {
//     fn set_next_state(&mut self, event: SessionEvent) {
//         let next = match (&self.state, event) {
//             (SessionState::Connecting, SessionEvent::Connected) => SessionState::Connected,
//             (SessionState::Connecting, SessionEvent::SessionExpired) => SessionState::Closed,
//             (SessionState::Connecting, SessionEvent::CloseCalled) => SessionState::Closed,
//             (SessionState::Connecting, SessionEvent::AuthFailed) => SessionState::AuthFailed,
//             (SessionState::Connected, SessionEvent::Disconnected) => SessionState::Connecting,
//             (SessionState::Connected, SessionEvent::CloseCalled) => SessionState::Closed,
//             (s, e) => panic!("Invalid SessionState/SessionEvent pair: {:?}, {:?}", s, e),
//         };
//         self.state = next;
//     }
// }

// TODO shutdown stream on cleanup

#[derive(Clone, Debug)]
pub(crate) struct SessionManager {
    addr: SocketAddr,
    ///
    /// Next xid to issue
    ///
    xid: Arc<AsyncMutex<i32>>,
    // TODO having this be public is janky
    pub(crate) session_info: Arc<Mutex<SessionInfo>>,
    exited: Arc<Mutex<bool>>,
    log: Logger,
}

impl SessionManager {
    pub(crate) fn new(
        addr: SocketAddr,
        xid: Arc<AsyncMutex<i32>>,
        session_timeout: Duration,
        read_only: bool,
        log: Logger,
    ) -> Self {
        SessionManager {
            addr,
            xid,
            session_info: Arc::new(Mutex::new(SessionInfo::new(session_timeout, read_only))),
            exited: Arc::new(Mutex::new(false)),
            log,
        }
    }

    // TODO encapsulate the IoError
    // TODO implement an intelligent reconnect policy
    // TODO reset the reconnect interval to something very large
    pub(crate) async fn reconnect(
        &self,
    ) -> Result<(WriteHalf<TcpStream>, ReadHalf<TcpStream>), IoError> {
        let conn = TcpStream::connect(self.addr).await?;

        let (mut conn_rx, mut conn_tx) = io::split(conn);
        //
        // Manually send a connect request, and block until it completes
        //
        let (resp_tx, resp_rx) = oneshot::channel();

        let mut xid_handle = self.xid.lock().await;
        let new_xid = *xid_handle;
        *xid_handle += 1;

        let request = {
            let session_info = self.session_info.lock().unwrap();
            Request::Connect {
                protocol_version: session_info.protocol_version,
                last_zxid_seen: session_info.last_zxid_seen,
                timeout: session_info.timeout,
                session_id: session_info.session_id,
                passwd: session_info.password.clone(),
                read_only: session_info.read_only,
            }
        };

        // TODO cross reference the request xid with the response xid
        // the channel isn't necessary -- just return the response from the
        // decoder -- I think that should work?
        let mut encoder = FramedWrite::new(&mut conn_tx, ZkEncoder::new());
        let mut decoder =
            FramedRead::new(&mut conn_rx, ZkConnDecoder::new(resp_tx, self.log.clone()));
        // TODO handle errors from all awaits below
        encoder
            .send(RequestWrapper {
                xid: new_xid,
                req: request,
            })
            .await;
        decoder.next().await;
        let resp = resp_rx.await.unwrap().unwrap();
        if let Response::Connect {
            protocol_version,
            timeout,
            session_id,
            ref password,
            read_only,
        } = resp
        {
            assert!(timeout >= 0);
            info!(self.log, "negotiated session timeout: {}ms", timeout);

            let mut session_info = self.session_info.lock().unwrap();
            session_info.protocol_version = protocol_version;
            session_info.session_id = session_id;
            mem::replace(&mut session_info.password, password.to_vec());
            session_info.timeout = timeout;
            //
            // The client must send heartbeats at an interval less than the
            // session timeout. Two-thirds the session timeout seems reasonable.
            //
            session_info.heartbeat_interval = Duration::from_millis(2 * timeout as u64 / 3);
            session_info.read_only = read_only;
        } else {
            //
            // The decoder should have caught this already
            //
            unreachable!("Parsed response is not a Response::Connect");
        }

        Ok((conn_tx, conn_rx))
    }

    pub(crate) fn get_heartbeat_interval(&self) -> Duration {
        (*self.session_info.lock().unwrap()).heartbeat_interval
    }

    pub(crate) async fn close_session(&self) {
        // TODO implement this
        let mut exited = self.exited.lock().unwrap();
        *exited = true;
    }

    pub(crate) fn is_exited(&self) -> bool {
        *self.exited.lock().unwrap()
    }
}

// TODO go through and replace all asyncmutexes with mutexes where possible
