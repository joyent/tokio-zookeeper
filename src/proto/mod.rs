use async_trait::async_trait;
use failure;
use std::net::SocketAddr;
use tokio;
use tokio::prelude::*;

pub(crate) mod decoder;
pub(crate) mod encoder;
pub(crate) mod request;
pub(crate) mod response;

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
