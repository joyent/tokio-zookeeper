use failure;
use std::net::SocketAddr;
use tokio;
use tokio::prelude::*;

pub(crate) mod active_packetizer;
pub(crate) mod packetizer;
pub(crate) mod request;
pub(crate) mod response;

use self::request::Request;
use self::response::Response;

pub trait ZooKeeperTransport: AsyncRead + AsyncWrite + Sized + Send {
    type Addr: Send;
    type ConnectError: Into<failure::Error>;
    type ConnectFut: Future<Item = Self, Error = Self::ConnectError> + Send + 'static;
    fn connect(addr: &Self::Addr) -> Self::ConnectFut;
}

impl ZooKeeperTransport for tokio::net::TcpStream {
    type Addr = SocketAddr;
    type ConnectError = tokio::io::Error;
    type ConnectFut = tokio::net::tcp::ConnectFuture;
    fn connect(addr: &Self::Addr) -> Self::ConnectFut {
        tokio::net::TcpStream::connect(addr)
    }
}
