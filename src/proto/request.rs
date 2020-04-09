use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

use crate::error::ZkError;
use crate::types::acl::Acl;
use crate::types::watch::Watch;
use crate::types::CreateMode;

#[derive(Debug)]
pub(crate) enum Request {
    Ping,
    Connect {
        // TODO make sure (here and elsewhere) that the signedness of the fields
        // matches the signedness expected by the server
        protocol_version: i32,
        last_zxid_seen: i64,
        timeout: i32,
        session_id: i64,
        passwd: Vec<u8>,
        read_only: bool,
    },
    Exists {
        path: String,
        watch: Watch,
    },
    Delete {
        path: String,
        version: i32,
    },
    SetData {
        path: String,
        data: Cow<'static, [u8]>,
        version: i32,
    },
    Create {
        path: String,
        data: Cow<'static, [u8]>,
        acl: Cow<'static, [Acl]>,
        mode: CreateMode,
    },
    GetChildren {
        path: String,
        watch: Watch,
    },
    GetData {
        path: String,
        watch: Watch,
    },
    GetAcl {
        path: String,
    },
    SetAcl {
        path: String,
        acl: Cow<'static, [Acl]>,
        version: i32,
    },
    Check {
        path: String,
        version: i32,
    },
    Multi(Vec<Request>),
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
#[repr(i32)]
#[allow(dead_code)]
pub(crate) enum OpCode {
    Notification = 0,
    Create = 1,
    Delete = 2,
    Exists = 3,
    GetData = 4,
    SetData = 5,
    GetACL = 6,
    SetACL = 7,
    GetChildren = 8,
    Synchronize = 9,
    Ping = 11,
    GetChildren2 = 12,
    Check = 13,
    Multi = 14,
    Auth = 100,
    SetWatches = 101,
    Sasl = 102,
    CreateSession = -10,
    CloseSession = -11,
    Error = -1,
}

impl From<i32> for OpCode {
    fn from(code: i32) -> Self {
        match code {
            0 => OpCode::Notification,
            1 => OpCode::Create,
            2 => OpCode::Delete,
            3 => OpCode::Exists,
            4 => OpCode::GetData,
            5 => OpCode::SetData,
            6 => OpCode::GetACL,
            7 => OpCode::SetACL,
            8 => OpCode::GetChildren,
            9 => OpCode::Synchronize,
            11 => OpCode::Ping,
            12 => OpCode::GetChildren2,
            13 => OpCode::Check,
            14 => OpCode::Multi,
            100 => OpCode::Auth,
            101 => OpCode::SetWatches,
            102 => OpCode::Sasl,
            -10 => OpCode::CreateSession,
            -11 => OpCode::CloseSession,
            -1 => OpCode::Error,
            _ => unimplemented!(),
        }
    }
}

pub(super) enum MultiHeader {
    NextOk(OpCode),
    NextErr(ZkError),
    Done,
}

pub trait WriteTo {
    fn write_to<B: BufMut>(&self, buf: &mut B);
}

impl WriteTo for Acl {
    fn write_to<B: BufMut>(&self, buf: &mut B) {
        buf.put_u32(self.perms.code());
        self.scheme.write_to(buf);
        self.id.write_to(buf)
    }
}

impl WriteTo for MultiHeader {
    fn write_to<B: BufMut>(&self, buf: &mut B) {
        match *self {
            MultiHeader::NextOk(opcode) => {
                buf.put_i32(opcode as i32);
                buf.put_u8(false as u8);
                buf.put_i32(-1)
            }
            MultiHeader::NextErr(_) => {
                panic!("client should not serialize MultiHeader::NextErr");
            }
            MultiHeader::Done => {
                buf.put_i32(-1);
                buf.put_u8(true as u8);
                buf.put_i32(-1)
            }
        }
    }
}

impl WriteTo for u8 {
    fn write_to<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(*self);
    }
}

impl WriteTo for str {
    fn write_to<B: BufMut>(&self, buf: &mut B) {
        buf.put_i32(self.len() as i32);
        buf.put(self.as_ref());
    }
}

impl WriteTo for [u8] {
    fn write_to<B: BufMut>(&self, buf: &mut B) {
        buf.put_i32(self.len() as i32);
        buf.put(self);
    }
}

fn write_list<B, T>(buf: &mut B, ts: &[T])
where
    T: WriteTo,
    B: BufMut,
{
    buf.put_i32(ts.len() as i32);
    for elem in ts {
        elem.write_to(buf);
    }
}

impl Request {
    pub(super) fn serialize_into(&self, buffer: &mut BytesMut) {
        match *self {
            Request::Ping => {}
            Request::Connect {
                protocol_version,
                last_zxid_seen,
                timeout,
                session_id,
                ref passwd,
                read_only,
            } => {
                buffer.put_i32(protocol_version);
                buffer.put_i64(last_zxid_seen);
                buffer.put_i32(timeout);
                buffer.put_i64(session_id);
                buffer.put_i32(passwd.len() as i32);
                buffer.put(&passwd[..]);
                buffer.put_u8(read_only as u8);
            }
            Request::GetData {
                ref path,
                ref watch,
            }
            | Request::GetChildren {
                ref path,
                ref watch,
            }
            | Request::Exists {
                ref path,
                ref watch,
            } => {
                path.write_to(buffer);
                buffer.put_u8(watch.to_u8());
            }
            Request::Delete { ref path, version } => {
                path.write_to(buffer);
                buffer.put_i32(version);
            }
            Request::SetData {
                ref path,
                ref data,
                version,
            } => {
                path.write_to(buffer);
                data.write_to(buffer);
                buffer.put_i32(version);
            }
            Request::Create {
                ref path,
                ref data,
                mode,
                ref acl,
            } => {
                path.write_to(buffer);
                data.write_to(buffer);
                write_list(buffer, acl);
                buffer.put_i32(mode as i32);
            }
            Request::GetAcl { ref path } => {
                path.write_to(buffer);
            }
            Request::SetAcl {
                ref path,
                ref acl,
                version,
            } => {
                path.write_to(buffer);
                write_list(buffer, acl);
                buffer.put_i32(version);
            }
            Request::Check { ref path, version } => {
                path.write_to(buffer);
                buffer.put_i32(version);
            }
            Request::Multi(ref requests) => {
                for r in requests {
                    MultiHeader::NextOk(r.opcode()).write_to(buffer);
                    r.serialize_into(buffer);
                }
                MultiHeader::Done.write_to(buffer);
            }
        }
    }

    pub(crate) fn opcode(&self) -> OpCode {
        match *self {
            Request::Ping => OpCode::Ping,
            Request::Connect { .. } => OpCode::CreateSession,
            Request::Exists { .. } => OpCode::Exists,
            Request::Delete { .. } => OpCode::Delete,
            Request::Create { .. } => OpCode::Create,
            Request::GetChildren { .. } => OpCode::GetChildren,
            Request::SetData { .. } => OpCode::SetData,
            Request::GetData { .. } => OpCode::GetData,
            Request::GetAcl { .. } => OpCode::GetACL,
            Request::SetAcl { .. } => OpCode::SetACL,
            Request::Multi { .. } => OpCode::Multi,
            Request::Check { .. } => OpCode::Check,
        }
    }
}
