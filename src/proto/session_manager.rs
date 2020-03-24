use super::ZooKeeperTransport;
use crate::error::ZkError;
use std::io::Error as IoError;
use std::net::SocketAddr;
use std::sync::mpsc::Receiver;
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;

///
/// Fields for re-connection
///
#[derive(Debug, Clone)]
pub(super) struct ReconnectInfo {
    pub(super) last_zxid_seen: i64,
    pub(super) session_id: i64,
    pub(super) password: Vec<u8>,
}

#[derive(Debug)]
enum SessionState {
    Connecting,
    Connected,
    Closed,
    AuthFailed,
}

#[derive(Debug)]
enum SessionEvent {
    Connected,
    Disconnected,
    CloseCalled,
    SessionExpired,
    AuthFailed,
}

struct StateMachine {
    state: SessionState,
}

impl StateMachine {
    fn set_next_state(&mut self, event: SessionEvent) {
        let next = match (&self.state, event) {
            (SessionState::Connecting, SessionEvent::Connected) => SessionState::Connected,
            (SessionState::Connecting, SessionEvent::SessionExpired) => SessionState::Closed,
            (SessionState::Connecting, SessionEvent::CloseCalled) => SessionState::Closed,
            (SessionState::Connecting, SessionEvent::AuthFailed) => SessionState::AuthFailed,
            (SessionState::Connected, SessionEvent::Disconnected) => SessionState::Connecting,
            (SessionState::Connected, SessionEvent::CloseCalled) => SessionState::Closed,
            (s, e) => panic!("Invalid SessionState/SessionEvent pair: {:?}, {:?}", s, e),
        };
        self.state = next;
    }
}

// TODO shutdown stream on cleanup

pub struct SessionManager {
    addr: SocketAddr,
    conn: Option<TcpStream>,
}

impl SessionManager {
    pub fn new(addr: SocketAddr) -> Self {
        SessionManager { addr, conn: None }
    }

    async fn get_conn(&mut self) -> Result<&mut TcpStream, IoError> {
        if self.conn.is_none() {
            self.conn = Some(self.reconnect().await?);
        }
        Ok(self.conn.as_mut().unwrap())
    }

    pub async fn get_rx(&mut self) -> Result<ReadHalf<'_>, IoError> {
        self.get_conn().await.map(|mut conn| conn.split().0)
    }

    pub async fn get_tx(&mut self) -> Result<WriteHalf<'_>, IoError> {
        self.get_conn().await.map(|mut conn| conn.split().1)
    }

    // TODO encapsulate the IoError
    // TODO implement an intelligent reconnect policy
    async fn reconnect(&mut self) -> Result<TcpStream, IoError> {
        tokio::net::TcpStream::connect(self.addr).await
    }
}
