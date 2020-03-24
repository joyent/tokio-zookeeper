//
// Copyright 2020 Joyent, Inc.
//

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, i32, mem, u64};

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::{self, Sender};
use futures::lock::Mutex as AsyncMutex;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use slog::{debug, error, info, trace, Logger};
use tokio::io::{self, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::time;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::error::{InternalError, ZkError};
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
struct SessionInfo {
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

#[derive(Clone, Debug)]
pub(crate) struct SessionManager {
    // Address of server
    addr: SocketAddr,

    // Next xid to issue
    xid: Arc<AsyncMutex<i32>>,

    // Clone of client-facing tx to let us send server requests internally
    req_tx: UnboundedSender<(Request, Sender<Result<Response, ZkError>>)>,

    // Shared reference to session info parameters
    session_info: Arc<AsyncMutex<SessionInfo>>,

    // Whether or not this is our first time connecting to the server
    first: Arc<AsyncMutex<bool>>,

    // Whether or not the client has exited
    exited: Arc<AsyncMutex<bool>>,

    //
    // A timestamp of the last time we contacted the server. Currently not used.
    // Could be used to more closely determine at what point session expiry is
    // guaranteed when trying to reconnect.
    //
    last_contact: Arc<AsyncMutex<Instant>>,
    log: Logger,
}

impl SessionManager {
    pub(crate) fn new(
        addr: SocketAddr,
        xid: Arc<AsyncMutex<i32>>,
        req_tx: UnboundedSender<(Request, Sender<Result<Response, ZkError>>)>,
        session_timeout: Duration,
        read_only: bool,
        log: Logger,
    ) -> Self {
        SessionManager {
            addr,
            xid,
            req_tx,
            session_info: Arc::new(AsyncMutex::new(SessionInfo::new(
                session_timeout,
                read_only,
            ))),
            first: Arc::new(AsyncMutex::new(true)),
            exited: Arc::new(AsyncMutex::new(false)),
            last_contact: Arc::new(AsyncMutex::new(Instant::now())),
            log,
        }
    }

    //
    // Reconnect to the server, with retry and exponential backoff.
    //
    pub(crate) async fn reconnect(
        &self,
    ) -> Result<(WriteHalf<TcpStream>, ReadHalf<TcpStream>), InternalError> {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = {
            if *self.first.lock().await {
                //
                // If we're establishing a new session, there is no session
                // timeout to serve as an upper bound for the time to connect.
                //
                None
            } else {
                //
                // If the session timeout has elapsed, we can't possibly
                // reconnect.
                //
                // TODO implement stability threshold for backoff
                // TODO implement zkconnectstring
                //
                let session_info = self.session_info.lock().await;
                Some(Duration::from_millis(session_info.timeout as u64))
            }
        };
        info!(self.log, "Connecting");
        let mut delay = Duration::from_millis(0);
        loop {
            debug!(
                self.log,
                "Attempting to connect";
                "delay_ms" => delay.as_millis()
            );
            time::delay_for(delay).await;
            match self.reconnect_inner().await {
                Err(e) => {
                    error!(self.log, "Error connecting to ZooKeeper: {:?}", e);
                    if let InternalError::SessionExpired = e {
                        return Err(InternalError::SessionExpired);
                    }
                    match backoff.next_backoff() {
                        Some(interval) => {
                            delay = interval;
                            continue;
                        }
                        //
                        // The max time before session expiry elapsed
                        //
                        None => return Err(InternalError::SessionExpired),
                    }
                }
                Ok(result) => {
                    *self.first.lock().await = false;
                    return Ok(result);
                }
            }
        }
    }

    //
    // Perform one reconnect attempt, without retry or exponential backoff
    //
    async fn reconnect_inner(
        &self,
    ) -> Result<(WriteHalf<TcpStream>, ReadHalf<TcpStream>), InternalError> {
        async fn recv_msg(
            decoder: &mut FramedRead<&mut ReadHalf<TcpStream>, ZkConnDecoder>,
        ) -> Result<Response, InternalError> {
            match decoder.next().await {
                Some(item) => item,
                None => Err(InternalError::ConnectionEnded),
            }
        }

        let request = {
            let session_info = self.session_info.lock().await;
            Request::Connect {
                protocol_version: session_info.protocol_version,
                last_zxid_seen: session_info.last_zxid_seen,
                timeout: session_info.timeout,
                session_id: session_info.session_id,
                passwd: session_info.password.clone(),
                read_only: session_info.read_only,
            }
        };
        let conn = TcpStream::connect(self.addr).await?;

        let (mut conn_rx, mut conn_tx) = io::split(conn);

        let mut encoder = FramedWrite::new(&mut conn_tx, ZkEncoder::new());
        let mut decoder = FramedRead::new(&mut conn_rx, ZkConnDecoder {});

        encoder
            .send(RequestWrapper {
                //
                // Connect requests don't have xids, so it doesn't matter
                // what we put here.
                //
                xid: 0,
                req: request,
            })
            .await?;
        let resp = recv_msg(&mut decoder).await?;
        if let Response::Connect {
            protocol_version,
            timeout,
            session_id,
            ref password,
            read_only,
        } = resp
        {
            info!(self.log, "handling server connect response: {:?}", resp);
            assert!(timeout >= 0);
            //
            // XXX This means we supplied invalid connect info from the server's
            // point of view, implying the session is expired. This isn't
            // necessarily a stable interface, and shouldn't be relied on.
            //
            if timeout == 0 {
                return Err(InternalError::SessionExpired);
            }
            info!(self.log, "negotiated session timeout: {}ms", timeout);

            let mut session_info = self.session_info.lock().await;
            session_info.protocol_version = protocol_version;
            session_info.session_id = session_id;
            mem::replace(&mut session_info.password, password.to_vec());
            session_info.timeout = timeout;
            //
            // The client must send heartbeats at an interval less than the
            // session timeout. Two-thirds the session timeout seems
            // reasonable.
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

    pub(crate) async fn get_heartbeat_interval(&self) -> Duration {
        (*self.session_info.lock().await).heartbeat_interval
    }

    //
    // Send a `Close` request to the server
    //
    pub(crate) async fn close_session(&self) -> Result<(), InternalError> {
        *self.exited.lock().await = true;
        let req = Request::Close;
        let (tx, rx) = oneshot::channel();
        self.req_tx.unbounded_send((req, tx))?;
        rx.await??;
        Ok(())
    }

    //
    // Update the last seen zxid
    //
    pub(crate) async fn set_zxid(&self, zxid: i64) {
        if zxid > 0 {
            let mut session_info = self.session_info.lock().await;
            assert!(zxid >= session_info.last_zxid_seen);
            trace!(
                self.log,
                "updated zxid from {} to {}",
                session_info.last_zxid_seen,
                zxid
            );
            session_info.last_zxid_seen = zxid;
        }
    }

    pub(crate) async fn get_zxid(&self) -> i64 {
        self.session_info.lock().await.last_zxid_seen
    }

    pub(crate) async fn is_exited(&self) -> bool {
        *self.exited.lock().await
    }

    //
    // TODO make use of this for anticipating session expiry instead of using
    // XXX section above.
    //
    #[allow(dead_code)]
    pub(crate) async fn register_contact(&self) {
        *self.last_contact.lock().await = Instant::now();
    }
}
