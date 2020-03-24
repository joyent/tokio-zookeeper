//
// Copyright 2020 Joyent, Inc.
//

use std::io::{Error as IoError, ErrorKind as IoErrorKind};

use failure_derive::Fail;
use futures::channel::mpsc::TrySendError;
use futures::channel::oneshot::Canceled;

//
// Represents errors handled internally by the client, as opposed to being
// returned to the user
//
#[derive(Debug)]
pub(crate) enum InternalError {
    // A server error to be handled internally.
    ServerError(ZkError),

    // We received a response with an xid for which we have no record
    DanglingXid(i32),

    // The server connection failed. The underlying IoError is included
    ConnectionError(IoError),

    // The server closed the TCP stream.
    ConnectionEnded,

    // An operation failed because the session is expired.
    SessionExpired,
}

impl From<IoError> for InternalError {
    fn from(e: IoError) -> InternalError {
        InternalError::ConnectionError(e)
    }
}

impl From<Canceled> for InternalError {
    fn from(_: Canceled) -> InternalError {
        InternalError::ConnectionError(IoError::new(IoErrorKind::Other, "Request canceled"))
    }
}

impl<T> From<TrySendError<T>> for InternalError {
    fn from(_: TrySendError<T>) -> InternalError {
        InternalError::ConnectionError(IoError::new(IoErrorKind::Other, "Request canceled"))
    }
}

impl From<ZkError> for InternalError {
    fn from(e: ZkError) -> InternalError {
        InternalError::ServerError(e)
    }
}

/// Raw errors returned from the ZooKeeper server
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ZkError {
    ///
    /// This code is never returned from the server. It should not be used other
    /// than to indicate a range. Specifically, error codes greater than this
    /// value are API errors, while values less than this indicate a system
    /// error.
    ///
    APIError = -100,

    /// Client authentication failed.
    AuthFailed = -115,

    /// Invalid arguments.
    BadArguments = -8,

    ///
    /// Version conflict in `set` operation. In case of reconfiguration:
    /// reconfig requested from config version X but last seen config has a
    /// different version Y.
    ///
    BadVersion = -103,

    /// Connection to the server has been lost.
    ConnectionLoss = -4,

    /// A data inconsistency was found.
    DataInconsistency = -3,

    /// Attempt to create ephemeral node on a local session.
    EphemeralOnLocalSession = -120,

    /// Invalid `Acl` specified.
    InvalidACL = -114,

    /// Invalid callback specified.
    InvalidCallback = -113,

    /// Error while marshalling or unmarshalling data.
    MarshallingError = -5,

    /// Not authenticated.
    NoAuth = -102,

    /// Ephemeral nodes may not have children.
    NoChildrenForEphemerals = -108,

    /// Request to create node that already exists.
    NodeExists = -110,

    /// Attempted to read a node that does not exist.
    NoNode = -101,

    /// The node has children.
    NotEmpty = -111,

    /// State-changing request is passed to read-only server.
    NotReadOnly = -119,

    /// Attempt to remove a non-existing watcher.
    NoWatcher = -121,

    /// No error occurred.
    Ok = 0,

    /// Operation timeout.
    OperationTimeout = -7,

    /// A runtime inconsistency was found.
    RuntimeInconsistency = -2,

    /// The session has been expired by the server.
    SessionExpired = -112,

    /// Session moved to another server, so operation is ignored.
    SessionMoved = -118,

    ///
    /// System and server-side errors. This is never thrown by the server. It
    /// shouldn't be used other than to indicate a range. Specifically, error
    /// codes greater than this value, but lesser than `APIError`, are system
    /// errors.
    ///
    SystemError = -1,

    /// Operation is unimplemented.
    Unimplemented = -6,
}

impl From<i32> for ZkError {
    fn from(code: i32) -> Self {
        match code {
            -100 => ZkError::APIError,
            -115 => ZkError::AuthFailed,
            -8 => ZkError::BadArguments,
            -103 => ZkError::BadVersion,
            -4 => ZkError::ConnectionLoss,
            -3 => ZkError::DataInconsistency,
            -120 => ZkError::EphemeralOnLocalSession,
            -114 => ZkError::InvalidACL,
            -113 => ZkError::InvalidCallback,
            -5 => ZkError::MarshallingError,
            -102 => ZkError::NoAuth,
            -108 => ZkError::NoChildrenForEphemerals,
            -110 => ZkError::NodeExists,
            -101 => ZkError::NoNode,
            -111 => ZkError::NotEmpty,
            -119 => ZkError::NotReadOnly,
            -121 => ZkError::NoWatcher,
            0 => ZkError::Ok,
            -7 => ZkError::OperationTimeout,
            -2 => ZkError::RuntimeInconsistency,
            -112 => ZkError::SessionExpired,
            -118 => ZkError::SessionMoved,
            -1 => ZkError::SystemError,
            -6 => ZkError::Unimplemented,
            _ => panic!("unknown error code {}", code),
        }
    }
}

///
/// Errors that may cause a delete request to fail.
///
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum Delete {
    /// No node exists with the given `path`.
    #[fail(display = "target node does not exist")]
    NoNode,

    ///
    /// The target node has a different version than was specified by the call
    /// to delete.
    #[fail(
        display = "target node has different version than expected ({})",
        expected
    )]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },

    /// The target node has child nodes, and therefore cannot be deleted.
    #[fail(display = "target node has children, and cannot be deleted")]
    NotEmpty,
}

///
/// Errors that may cause a `set_data` request to fail.
///
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum SetData {
    /// No node exists with the given `path`.
    #[fail(display = "target node does not exist")]
    NoNode,

    ///
    /// The target node has a different version than was specified by the call
    /// to `set_data`.
    ///
    #[fail(
        display = "target node has different version than expected ({})",
        expected
    )]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },

    ///
    /// The target node's permission does not accept data modification or
    /// requires different authentication to be altered.
    ///
    #[fail(display = "insuficient authentication")]
    NoAuth,
}

///
/// Errors that may cause a create request to fail.
///
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum Create {
    /// A node with the given `path` already exists.
    #[fail(display = "target node already exists")]
    NodeExists,

    /// The parent node of the given `path` does not exist.
    #[fail(display = "parent node of target does not exist")]
    NoNode,

    ///
    /// The parent node of the given `path` is ephemeral, and cannot have
    /// children.
    ///
    #[fail(display = "parent node is ephemeral, and cannot have children")]
    NoChildrenForEphemerals,

    /// The given ACL is invalid.
    #[fail(display = "the given ACL is invalid")]
    InvalidAcl,
}

///
/// Errors that may cause a `get_acl` request to fail.
///
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum GetAcl {
    /// No node exists with the given `path`.
    #[fail(display = "target node does not exist")]
    NoNode,
}

///
/// Errors that may cause a `set_acl` request to fail.
///
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum SetAcl {
    /// No node exists with the given `path`.
    #[fail(display = "target node does not exist")]
    NoNode,

    ///
    /// The target node has a different version than was specified by the call
    /// to `set_acl`.
    ///
    #[fail(
        display = "target node has different version than expected ({})",
        expected
    )]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },

    /// The given ACL is invalid.
    #[fail(display = "the given ACL is invalid")]
    InvalidAcl,

    ///
    /// The target node's permission does not accept acl modification or
    /// requires different authentication to be altered.
    ///
    #[fail(display = "insufficient authentication")]
    NoAuth,
}

///
/// Errors that may cause a `check` request to fail.
///
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum Check {
    /// No node exists with the given `path`.
    #[fail(display = "target node does not exist")]
    NoNode,

    ///
    /// The target node has a different version than was specified by the call
    /// to `check`.
    ///
    #[fail(
        display = "target node has different version than expected ({})",
        expected
    )]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },
}

///
/// The result of a failed `multi` request.
///
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum Multi {
    /// A failed `delete` request.
    #[fail(display = "delete failed: {}", 0)]
    Delete(Delete),

    /// A failed `set_data` request.
    #[fail(display = "set_data failed: {}", 0)]
    SetData(SetData),

    /// A failed `create` request.
    #[fail(display = "create failed: {}", 0)]
    Create(Create),

    /// A failed `check` request.
    #[fail(display = "check failed")]
    Check(Check),

    ///
    /// The request would have succeeded, but a later request in the `multi`
    /// batch failed and caused this request to get rolled back.
    ///
    #[fail(display = "request rolled back due to later failed request")]
    RolledBack,

    ///
    /// The request was skipped because an earlier request in the `multi` batch
    /// failed. It is unknown whether this request would have succeeded.
    ///
    #[fail(display = "request failed due to earlier failed request")]
    Skipped,
}

impl From<Delete> for Multi {
    fn from(err: Delete) -> Self {
        Multi::Delete(err)
    }
}

impl From<SetData> for Multi {
    fn from(err: SetData) -> Self {
        Multi::SetData(err)
    }
}

impl From<Create> for Multi {
    fn from(err: Create) -> Self {
        Multi::Create(err)
    }
}

impl From<Check> for Multi {
    fn from(err: Check) -> Self {
        Multi::Check(err)
    }
}
