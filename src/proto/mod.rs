use async_trait::async_trait;
use failure;
use futures::Future;
use std::net::SocketAddr;
use tokio;
use tokio::prelude::*;

pub(crate) mod active_packetizer;
pub(crate) mod decoder;
pub(crate) mod packetizer;
pub(crate) mod request;
pub(crate) mod response;
pub(crate) mod session_manager;

use self::request::Request;
use self::response::Response;

#[async_trait]
pub trait ZooKeeperTransport: AsyncRead + AsyncWrite + Sized + Send {
    type Addr: Send;
    type ConnectError: Into<failure::Error>;
    async fn connect(addr: &Self::Addr) -> Result<Self, Self::ConnectError>;
}

#[async_trait]
impl ZooKeeperTransport for tokio::net::TcpStream {
    type Addr = SocketAddr;
    type ConnectError = tokio::io::Error;
    async fn connect(addr: &Self::Addr) -> tokio::io::Result<Self> {
        tokio::net::TcpStream::connect(addr).await
    }
}
