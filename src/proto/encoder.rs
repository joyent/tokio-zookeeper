use bytes::{BufMut, BytesMut};
use std::convert::TryInto;
use std::io::Error as IoError;
use tokio_util::codec::Encoder;

use crate::proto::decoder::HEADER_SIZE;
use crate::proto::request::Request;

pub(crate) struct RequestWrapper {
    pub(crate) req: Request,
    pub(crate) xid: i32,
}

pub(crate) struct ZkEncoder {}

impl ZkEncoder {
    pub fn new() -> Self {
        ZkEncoder {}
    }
}

impl Encoder<RequestWrapper> for ZkEncoder {
    type Error = IoError;

    fn encode(&mut self, item: RequestWrapper, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Set aside bytes at beginning of buffer for message length
        let mut buf = dst.split_off(HEADER_SIZE);
        if let Request::Connect { .. } = item.req {
        } else {
            // xid
            buf.put_i32(item.xid);
            // opcode
            buf.put_i32(item.req.opcode() as i32);
        }

        // payload
        item.req.serialize_into(&mut buf);
        // write payload length into the part of the buffer we set aside
        let written = buf.len();
        dst.put_u32(written.try_into().unwrap());

        // Join length and payload
        dst.unsplit(buf);
        Ok(())
    }
}
