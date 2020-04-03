use futures::channel::oneshot::{self, Sender};
use futures::lock::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use slog::{info, Logger};
use std::default::Default;
use std::io::Error as IoError;
use std::mem;
use std::net::SocketAddr;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{self, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::error::ZkError;
use crate::proto::decoder::ZkConnDecoder;
use crate::proto::encoder::{RequestWrapper, ZkEncoder};
use crate::proto::request::Request;
use crate::proto::response::Response;

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

impl Default for SessionInfo {
    fn default() -> Self {
        //
        // The default values never get used, so they don't really matter. We
        // set the heartbeat interval to something very long (one day!) so we
        // don't try to send a heartbeat before the initial connect request even
        // gets sent.
        //
        SessionInfo {
            protocol_version: 0,
            last_zxid_seen: 0,
            session_id: 0,
            password: Vec::new(),
            timeout: 86_400_000,
            heartbeat_interval: Duration::from_secs(86_400),
            read_only: false,
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

#[derive(Clone)]
pub(crate) struct SessionManager {
    addr: SocketAddr,
    ///
    /// Next xid to issue
    ///
    xid: Arc<AsyncMutex<i32>>,
    // TODO having this be public is janky
    pub(crate) session_info: Arc<Mutex<SessionInfo>>,
    log: Logger,
}

impl SessionManager {
    pub fn new(addr: SocketAddr, xid: Arc<AsyncMutex<i32>>, log: Logger) -> Self {
        SessionManager {
            addr,
            xid,
            session_info: Arc::new(Mutex::new(SessionInfo::default())),
            log,
        }
    }

    // TODO encapsulate the IoError
    // TODO implement an intelligent reconnect policy
    // TODO actually send the reconnect message
    // TODO reset the reconnect interval to something very large
    pub async fn reconnect(&self) -> Result<(WriteHalf<TcpStream>, ReadHalf<TcpStream>), IoError> {
        let mut conn = TcpStream::connect(self.addr).await?;

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
            // TODO handle session mgr stuff. Is there more?
            assert!(timeout >= 0);
            info!(self.log, "negotiated session timeout: {}ms", timeout);

            let mut session_info = self.session_info.lock().unwrap();
            session_info.protocol_version = protocol_version;
            session_info.session_id = session_id;
            mem::replace(&mut session_info.password, password.to_vec());
            session_info.timeout = timeout;
            // TODO add explanatory comment for this math
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

    pub fn get_heartbeat_interval(&self) -> Duration {
        (*self.session_info.lock().unwrap()).heartbeat_interval
    }
}

// TODO go through and replace all asyncmutexes with mutexes where possible
