//
// Copyright 2020 Joyent, Inc.
//

use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult};

use byteorder::{BigEndian, ReadBytesExt};

use crate::error::ZkError;
use crate::proto::request::{MultiHeader, OpCode};
use crate::types::acl::Acl;
use crate::types::watch::{WatchedEvent, WatchedEventType};
use crate::types::{KeeperState, Permission, Stat};

pub(crate) const SHUTDOWN_XID: i32 = 0;
pub(crate) const WATCH_XID: i32 = -1;
pub(crate) const HEARTBEAT_XID: i32 = -2;
pub(crate) const FIRST_XID: i32 = 1;

#[derive(Debug)]
pub(crate) enum Response {
    Connect {
        protocol_version: i32,
        timeout: i32,
        session_id: i64,
        password: Vec<u8>,
        read_only: bool,
    },
    Stat(Stat),
    GetData {
        bytes: Vec<u8>,
        stat: Stat,
    },
    GetAcl {
        acl: Vec<Acl>,
        stat: Stat,
    },
    Empty,
    Strings(Vec<String>),
    String(String),
    Multi(Vec<Result<Response, ZkError>>),
}

pub trait ReadFrom: Sized {
    fn read_from<R: Read>(read: &mut R) -> IoResult<Self>;
}

impl ReadFrom for Vec<String> {
    fn read_from<R: Read>(read: &mut R) -> IoResult<Self> {
        let len = read.read_i32::<BigEndian>()?;
        let mut items = Vec::with_capacity(len as usize);
        for _ in 0..len {
            items.push(read.read_string()?);
        }
        Ok(items)
    }
}

impl ReadFrom for Stat {
    fn read_from<R: Read>(read: &mut R) -> IoResult<Stat> {
        Ok(Stat {
            czxid: read.read_i64::<BigEndian>()?,
            mzxid: read.read_i64::<BigEndian>()?,
            ctime: read.read_i64::<BigEndian>()?,
            mtime: read.read_i64::<BigEndian>()?,
            version: read.read_i32::<BigEndian>()?,
            cversion: read.read_i32::<BigEndian>()?,
            aversion: read.read_i32::<BigEndian>()?,
            ephemeral_owner: read.read_i64::<BigEndian>()?,
            data_length: read.read_i32::<BigEndian>()?,
            num_children: read.read_i32::<BigEndian>()?,
            pzxid: read.read_i64::<BigEndian>()?,
        })
    }
}

impl ReadFrom for WatchedEvent {
    fn read_from<R: Read>(read: &mut R) -> IoResult<WatchedEvent> {
        let wtype = read.read_i32::<BigEndian>()?;
        let state = read.read_i32::<BigEndian>()?;
        let path = read.read_string()?;
        Ok(WatchedEvent {
            event_type: WatchedEventType::from(wtype),
            keeper_state: KeeperState::from(state),
            path,
        })
    }
}

impl ReadFrom for Vec<Acl> {
    fn read_from<R: Read>(read: &mut R) -> IoResult<Self> {
        let len = read.read_i32::<BigEndian>()?;
        let mut items = Vec::with_capacity(len as usize);
        for _ in 0..len {
            items.push(Acl::read_from(read)?);
        }
        Ok(items)
    }
}

impl ReadFrom for Acl {
    fn read_from<R: Read>(read: &mut R) -> IoResult<Self> {
        let perms = Permission::read_from(read)?;
        let scheme = read.read_string()?;
        let id = read.read_string()?;
        Ok(Acl { perms, scheme, id })
    }
}

impl ReadFrom for Permission {
    fn read_from<R: Read>(read: &mut R) -> IoResult<Self> {
        Ok(Permission::from_raw(read.read_u32::<BigEndian>()?))
    }
}

impl ReadFrom for MultiHeader {
    fn read_from<R: Read>(read: &mut R) -> IoResult<Self> {
        let opcode = read.read_i32::<BigEndian>()?;
        let done = read.read_u8()? != 0;
        let err = read.read_i32::<BigEndian>()?;
        if done {
            Ok(MultiHeader::Done)
        } else if opcode == -1 {
            Ok(MultiHeader::NextErr(err.into()))
        } else {
            Ok(MultiHeader::NextOk(opcode.into()))
        }
    }
}

pub trait BufferReader: Read {
    fn read_buffer(&mut self) -> IoResult<Vec<u8>>;
}

impl<R: Read> BufferReader for R {
    fn read_buffer(&mut self) -> IoResult<Vec<u8>> {
        let len = self.read_i32::<BigEndian>()?;
        let len = if len < 0 { 0 } else { len as usize };
        let mut buf = vec![0; len];
        let read = self.read(&mut buf)?;
        if read == len {
            Ok(buf)
        } else {
            Err(IoError::new(ErrorKind::WouldBlock, "read_buffer failed"))
        }
    }
}

trait StringReader: Read {
    fn read_string(&mut self) -> IoResult<String>;
}

impl<R: Read> StringReader for R {
    fn read_string(&mut self) -> IoResult<String> {
        let raw = self.read_buffer()?;
        Ok(String::from_utf8(raw).expect("Bytes are not utf8"))
    }
}

impl Response {
    pub(super) fn parse(opcode: OpCode, reader: &mut &[u8]) -> Result<Self, IoError> {
        match opcode {
            OpCode::CreateSession => Ok(Response::Connect {
                protocol_version: reader.read_i32::<BigEndian>()?,
                timeout: reader.read_i32::<BigEndian>()?,
                session_id: reader.read_i64::<BigEndian>()?,
                password: reader.read_buffer()?,
                read_only: reader.read_u8()? != 0,
            }),
            OpCode::Exists | OpCode::SetData | OpCode::SetACL => {
                Ok(Response::Stat(Stat::read_from(reader)?))
            }
            OpCode::GetData => Ok(Response::GetData {
                bytes: reader.read_buffer()?,
                stat: Stat::read_from(reader)?,
            }),
            OpCode::GetChildren => Ok(Response::Strings(Vec::<String>::read_from(reader)?)),
            OpCode::Create => Ok(Response::String(reader.read_string()?)),
            OpCode::GetACL => Ok(Response::GetAcl {
                acl: Vec::<Acl>::read_from(reader)?,
                stat: Stat::read_from(reader)?,
            }),
            OpCode::Multi => {
                let mut responses = Vec::new();
                loop {
                    match MultiHeader::read_from(reader)? {
                        MultiHeader::NextErr(e) => {
                            responses.push(Err(e));
                            let _ = reader.read_i32::<BigEndian>()?;
                        }
                        MultiHeader::NextOk(opcode) => {
                            responses.push(Ok(Response::parse(opcode, reader)?));
                        }
                        MultiHeader::Done => break,
                    }
                }
                Ok(Response::Multi(responses))
            }
            OpCode::SetWatches | OpCode::CloseSession | OpCode::Delete | OpCode::Check => {
                Ok(Response::Empty)
            }
            _ => panic!("got unexpected response opcode {:?}", opcode),
        }
    }
}
