//! This crate provides a client for interacting with [Apache
//! ZooKeeper](https://zookeeper.apache.org/), a highly reliable distributed
//! service for maintaining configuration information, naming, providing
//! distributed synchronization, and providing group services.
//!
//! # About ZooKeeper
//!
//! The [ZooKeeper Overview](https://zookeeper.apache.org/doc/current/zookeeperOver.html) provides
//! a thorough introduction to ZooKeeper, but we'll repeat the most important points here. At its
//! [heart](https://zookeeper.apache.org/doc/current/zookeeperOver.html#sc_designGoals), ZooKeeper
//! is a [hierarchical key-value
//! store](https://zookeeper.apache.org/doc/current/zookeeperOver.html#sc_dataModelNameSpace) (that
//! is, keys can have "sub-keys"), which additional mechanisms that guarantee consistent operation
//! across client and server failures. Keys in ZooKeeper look like paths (e.g., `/key/subkey`), and
//! every item along a path is called a
//! "[Znode](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_zkDataModel_znodes)".
//! Each Znode (including those with children) can also have associated data, which can be queried
//! and updated like in other key-value stores. Along with its data and children, each Znode stores
//! meta-information such as [access-control
//! lists](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl),
//! [modification
//! timestamps](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_timeInZk),
//! and a version number
//! that allows clients to avoid stepping on each other's toes when accessing values (more on that
//! later).
//!
//! ## Operations
//!
//! ZooKeeper's API consists of the same basic operations you would expect to find in a
//! file-system: [`create`](struct.ZooKeeper.html#method.create) for creating new Znodes,
//! [`delete`](struct.ZooKeeper.html#method.delete) for removing them,
//! [`exists`](struct.ZooKeeper.html#method.exists) for checking if a node exists,
//! [`get_data`](struct.ZooKeeper.html#method.get_data) and
//! [`set_data`](struct.ZooKeeper.html#method.set_data) for getting and setting a node's associated
//! data respectively, and [`get_children`](struct.ZooKeeper.html#method.get_children) for
//! retrieving the children of a given node (i.e., its subkeys). For all of these operations,
//! ZooKeeper gives [strong
//! guarantees](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkGuarantees)
//! about what happens when there are multiple clients interacting with the system, or even what
//! happens in response to system and network failures.
//!
//! ## Ephemeral nodes
//!
//! When you create a Znode, you also specify a [`CreateMode`]. Nodes that are created with
//! [`CreateMode::Persistent`] are the nodes we have discussed thus far. They remain in the server
//! until you delete them. Nodes that are created with [`CreateMode::Ephemeral`] on the other hand
//! are special. These [ephemeral
//! nodes](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#Ephemeral+Nodes) are
//! automatically deleted by the server when the client that created them disconnects. This can be
//! handy for implementing lease-like mechanisms, and for detecting faults. Since they are
//! automatically deleted, and nodes with children cannot be deleted directly, ephemeral nodes are
//! not allowed to have children.
//!
//! ## Watches
//!
//! In addition to the methods above, [`ZooKeeper::exists`], [`ZooKeeper::get_data`], and
//! [`ZooKeeper::get_children`] also support setting
//! "[watches](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkWatches)" on
//! a node. A watch is one-time trigger that causes a [`WatchedEvent`] to be sent to the client
//! that set the watch when the state for which the watch was set changes. For example, for a
//! watched `get_data`, a one-time notification will be sent the first time the data of the target
//! node changes following when the response to the original `get_data` call was
//! processed. See the ["Watches" entry in the Programmer's
//! Guide](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkWatches)
//! for details.
//!
//! ## Getting started
//!
//! To get ZooKeeper up and running, follow the official [Getting Started
//! Guide](https://zookeeper.apache.org/doc/current/zookeeperStarted.html). In most Linux
//! environments, the procedure for getting a basic setup working is usually just to install the
//! `zookeeper` package and then run `systemctl start zookeeper`. ZooKeeper will then be running at
//! `127.0.0.1:2181`.
//!
//! # This implementation
//!
//! This library is analogous to the asynchronous API offered by the [official Java
//! implementation](https://zookeeper.apache.org/doc/current/api/org/apache/zookeeper/ZooKeeper.html),
//! and for most operations the Java documentation should apply to the Rust implementation. If this
//! is not the case, it is considered [a bug](https://github.com/joyent/tokio-zookeeper/issues),
//! and we'd love a bug report with as much relevant information as you can offer.
//!
//! Note that since this implementation is asynchronous, users of the client must take care to
//! not re-order operations in their own code. There is some discussion of this in the [official
//! documentation of the Java
//! bindings](https://zookeeper.apache.org/doc/r3.4.12/zookeeperProgrammers.html#Java+Binding).
//!
//! For more information on ZooKeeper, see the [ZooKeeper Programmer's
//! Guide](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html) and the [Confluence
//! ZooKeeper wiki](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Index). There is also a
//! basic tutorial (that uses the Java client)
//! [here](https://zookeeper.apache.org/doc/current/zookeeperTutorial.html).
//!
//! ## Interaction with Tokio
//!
//! The futures in this crate expect to be running under a `tokio::Runtime`.
//! Within that context, you can `await` them.
//!
//! # An example
//!
//! ```no_run
//! TODO write an example
//! ```
//!

#![allow(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(missing_copy_implementations)]

pub mod error;
pub mod proto;
pub mod types;

pub(crate) mod backoff;
pub(crate) mod client;
pub(crate) mod session_manager;
pub(crate) mod transform;

use std::borrow::Cow;
use std::time;

use clap::{crate_name, crate_version};
use failure::{bail, format_err};
use futures::channel::mpsc::{self, UnboundedReceiver};
use futures::channel::oneshot::{self, Receiver};
use slog::{o, trace, Drain, LevelFilter};
use slog_async::Async;

use crate::client::{Enqueuer, SharedState};
use crate::proto::request::Request;
use crate::proto::response::Response;
use crate::session_manager::LONG_TIMEOUT;
use crate::types::watch::WatchOption;
use crate::types::{Acl, CreateMode, MultiResponse, Stat, WatchedEvent, ZkConnectString};

// TODO Enforce path constraints?
// https://zookeeper.apache.org/doc/r3.4.12/zookeeperProgrammers.html#ch_zkDataModel

///
/// A connection to ZooKeeper.
///
/// All interactions with ZooKeeper are performed by calling the methods of a
/// `ZooKeeper` instance. All clones of a given `ZooKeeper` instance use the
/// same underlying connection. Once a connection to a server is established,
/// the server assigns a session ID to the client. The client will send
/// heartbeats to the server periodically to keep the session valid.
///
/// An application can use the client as long as the session ID of the client
/// remains valid. If for some reason, the client fails to send heartbeats to
/// the server for a period of time exceeding the session timeout value,
/// the server will expire the session, and the session ID will become invalid.
/// The `ZooKeeper` instance will then no longer be usable, and all futures will
/// resolve with a protocol-level error. To make further ZooKeeper API calls,
/// the application must create a new `ZooKeeper` instance.
///
/// If the connected server becomes unreachable, the client will automatically
/// try to connect to another server before its session ID expires. If
/// successful, the application can continue to use the client. If unsuccessful,
/// the client will attempt to reconnect to the servers at intervals determined
/// by an exponential backoff, until the session expires.
///
/// Some successful ZooKeeper API calls can leave watches on the "data nodes" in
/// the ZooKeeper server. Other successful ZooKeeper API calls can trigger those
/// watches. Once a watch is triggered, an event will be delivered to the client
/// that set the watch. Each watch can be triggered only once. Thus, up to one
/// event will be delivered to a client for every watch it leaves.
///
#[derive(Debug, Clone)]
pub struct ZooKeeper {
    connection: Enqueuer,
    logger: slog::Logger,
}

///
/// Builder that allows customizing options for ZooKeeper connections.
///
#[derive(Debug, Clone)]
pub struct ZooKeeperBuilder {
    session_timeout: time::Duration,
    read_only: bool,
    logger: slog::Logger,
}

impl Default for ZooKeeperBuilder {
    fn default() -> Self {
        let drain = Async::new(
            LevelFilter::new(
                slog_bunyan::with_name(crate_name!(), std::io::stdout()).build(),
                slog::Level::Info,
            )
            .fuse(),
        )
        .build()
        .fuse();
        let root = slog::Logger::root(drain, o!("build-id" => crate_version!()));

        ZooKeeperBuilder {
            //
            // This default value will be negotiated down to the longest
            // timeout the server supports.
            //
            session_timeout: LONG_TIMEOUT,
            read_only: false,
            logger: root,
        }
    }
}

impl ZooKeeperBuilder {
    ///
    /// Connect to a ZooKeeper server instance at the given address.
    ///
    /// This function returns a `ZooKeeper` instance, along with a default
    /// watcher that will provide notifications of server state changes and
    /// watches on ZooKeeper nodes.
    ///
    /// This function cannot fail. The client will repeatedly attempt to connect
    /// to the server (with exponential backoff) until the session is
    /// established. There is no upper bound on the time to connect, because the
    /// session hasn't been established yet, so there is no session to time out.
    ///
    /// If the servers in the cluster become unreachable, the client will
    /// attempt to reconnect until the session timeout expires. Only then should
    /// the application call connect() again.
    ///
    pub async fn connect(
        self,
        conn_str: &ZkConnectString,
    ) -> (ZooKeeper, UnboundedReceiver<WatchedEvent>) {
        let log = self.logger.clone();
        let (tx, rx) = mpsc::unbounded();
        let enqueuer = SharedState::start(
            conn_str.clone(),
            tx,
            self.session_timeout,
            self.read_only,
            log,
        )
        .await;
        (
            ZooKeeper {
                connection: enqueuer,
                logger: self.logger,
            },
            rx,
        )
    }

    ///
    /// Set the ZooKeeper [session expiry
    /// timeout](https://zookeeper.apache.org/doc/r3.4.12/zookeeperProgrammers.html#ch_zkSessions).
    ///
    /// By default, the timeout will be the longest interval the server
    /// supports.
    ///
    pub fn set_timeout(&mut self, t: time::Duration) {
        self.session_timeout = t;
    }

    ///
    /// Set the logger that should be used internally in the ZooKeeper client.
    ///
    /// By default, the log level is set to `Info` and logs are written to
    /// stdout.See also [the `slog` documentation](https://docs.rs/slog).
    ///
    pub fn set_logger(&mut self, l: slog::Logger) {
        self.logger = l;
    }
}

impl ZooKeeper {
    ///
    /// Connect to a ZooKeeper server instance at the given address with default parameters.
    ///
    /// See [`ZooKeeperBuilder::connect`].
    ///
    pub async fn connect(conn_str: &ZkConnectString) -> (Self, UnboundedReceiver<WatchedEvent>) {
        ZooKeeperBuilder::default().connect(conn_str).await
    }

    ///
    /// Create a node with the given `path` with `data` as its contents.
    ///
    /// The `mode` argument specifies additional options for the newly created node.
    ///
    /// If `mode` is set to [`CreateMode::Ephemeral`] (or [`CreateMode::EphemeralSequential`]), the
    /// node will be removed by the ZooKeeper automatically when the session associated with the
    /// creation of the node expires.
    ///
    /// If `mode` is set to [`CreateMode::PersistentSequential`] or
    /// [`CreateMode::EphemeralSequential`], the actual path name of a sequential node will be the
    /// given `path` plus a suffix `i` where `i` is the current sequential number of the node. The
    /// sequence number is always fixed length of 10 digits, 0 padded. Once such a node is created,
    /// the sequential number will be incremented by one. The newly created node's full name is
    /// returned when the future is resolved.
    ///
    /// If a node with the same actual path already exists in the ZooKeeper, the returned future
    /// resolves with an error of [`error::Create::NodeExists`]. Note that since a different actual
    /// path is used for each invocation of creating sequential nodes with the same `path`
    /// argument, calls with sequential modes will never return `NodeExists`.
    ///
    /// Ephemeral nodes cannot have children in ZooKeeper. Therefore, if the parent node of the
    /// given `path` is ephemeral, the return future resolves to
    /// [`error::Create::NoChildrenForEphemerals`].
    ///
    /// If a node is created successfully, the ZooKeeper server will trigger the watches on the
    /// `path` left by `exists` calls, and the watches on the parent of the node by `get_children`
    /// calls.
    ///
    /// The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
    ///
    pub async fn create<D, A>(
        &self,
        path: &str,
        data: D,
        acl: A,
        mode: CreateMode,
    ) -> Result<Result<String, error::Create>, failure::Error>
    where
        D: Into<Cow<'static, [u8]>>,
        A: Into<Cow<'static, [Acl]>>,
    {
        let data = data.into();
        trace!(self.logger, "create"; "path" => path, "mode" => ?mode, "dlen" => data.len());
        self.connection
            .enqueue(Request::Create {
                path: path.to_string(),
                data,
                acl: acl.into(),
                mode,
            })
            .await
            .and_then(transform::create)
    }

    ///
    /// Set the data for the node at the given `path`.
    ///
    /// The call will succeed if such a node exists, and the given `version` matches the version of
    /// the node (if the given `version` is `None`, it matches any version). On success, the
    /// updated [`Stat`] of the node is returned.
    ///
    /// This operation, if successful, will trigger all the watches on the node of the given `path`
    /// left by `get_data` calls.
    ///
    /// The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
    ///
    pub async fn set_data<D>(
        &self,
        path: &str,
        version: Option<i32>,
        data: D,
    ) -> Result<Result<Stat, error::SetData>, failure::Error>
    where
        D: Into<Cow<'static, [u8]>>,
    {
        let data = data.into();
        trace!(self.logger, "set_data"; "path" => path, "version" => ?version, "dlen" => data.len());
        let version = version.unwrap_or(-1);
        self.connection
            .enqueue(Request::SetData {
                path: path.to_string(),
                version,
                data,
            })
            .await
            .and_then(move |r| transform::set_data(version, r))
    }

    ///
    /// Delete the node at the given `path`.
    ///
    /// The call will succeed if such a node exists, and the given `version` matches the node's
    /// version (if the given `version` is `None`, it matches any versions).
    ///
    /// This operation, if successful, will trigger all the watches on the node of the given `path`
    /// left by `exists` API calls, and the watches on the parent node left by `get_children` API
    /// calls.
    ///
    pub async fn delete(
        &self,
        path: &str,
        version: Option<i32>,
    ) -> Result<Result<(), error::Delete>, failure::Error> {
        trace!(self.logger, "delete"; "path" => path, "version" => ?version);
        let version = version.unwrap_or(-1);
        self.connection
            .enqueue(Request::Delete {
                path: path.to_string(),
                version,
            })
            .await
            .and_then(move |r| transform::delete(version, r))
    }

    ///
    /// Return the [ACL](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl)
    /// and Stat of the node at the given `path`.
    ///
    /// If no node exists for the given path, the returned future resolves with an error of
    /// [`error::GetAcl::NoNode`].
    ///
    pub async fn get_acl(
        &self,
        path: &str,
    ) -> Result<Result<(Vec<Acl>, Stat), error::GetAcl>, failure::Error> {
        trace!(self.logger, "get_acl"; "path" => path);
        self.connection
            .enqueue(Request::GetAcl {
                path: path.to_string(),
            })
            .await
            .and_then(transform::get_acl)
    }

    ///
    /// Set the [ACL](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl)
    /// for the node of the given `path`.
    ///
    /// The call will succeed if such a node exists and the given `version` matches the ACL version
    /// of the node. On success, the updated [`Stat`] of the node is returned.
    ///
    /// If no node exists for the given path, the returned future resolves with an error of
    /// [`error::SetAcl::NoNode`]. If the given `version` does not match the ACL version, the
    /// returned future resolves with an error of [`error::SetAcl::BadVersion`].
    ///
    pub async fn set_acl<A>(
        &self,
        path: &str,
        acl: A,
        version: Option<i32>,
    ) -> Result<Result<Stat, error::SetAcl>, failure::Error>
    where
        A: Into<Cow<'static, [Acl]>>,
    {
        trace!(self.logger, "set_acl"; "path" => path, "version" => ?version);
        let version = version.unwrap_or(-1);
        self.connection
            .enqueue(Request::SetAcl {
                path: path.to_string(),
                acl: acl.into(),
                version,
            })
            .await
            .and_then(move |r| transform::set_acl(version, r))
    }

    async fn exists_inner(
        &self,
        path: &str,
        watch: WatchOption,
    ) -> Result<Option<Stat>, failure::Error> {
        trace!(self.logger, "exists"; "path" => path, "watch" => ?watch);
        self.connection
            .enqueue(Request::Exists {
                path: path.to_string(),
                watch,
            })
            .await
            .and_then(transform::exists)
    }

    ///
    /// Return the [`Stat`] of the node of the given `path`, or `None` if the
    /// node does not exist.
    ///
    pub async fn exists(&self, path: &str) -> Result<Option<Stat>, failure::Error> {
        self.exists_inner(path, WatchOption::None).await
    }

    ///
    /// Return the [`Stat`] of the node of the given `path`, or `None` if the
    /// node does not exist.
    ///
    /// If no errors occur, a watch will be left on the node at the given
    /// `path`. The watch is triggered by any successful operation that creates
    /// or deletes the node, or sets the data on the node, and in turn causes
    /// an event to be sent over the default watcher.
    ///
    pub async fn exists_watch(&self, path: &str) -> Result<Option<Stat>, failure::Error> {
        self.exists_inner(path, WatchOption::Global).await
    }

    ///
    /// Return the [`Stat`] of the node of the given `path`, or `None` if the
    /// node does not exist.
    ///
    /// If no errors occur, a watch will be left on the node at the given
    /// `path`. The watch is triggered by any successful operation that creates
    /// or deletes the node, or sets the data on the node, and in turn causes
    /// the returned `oneshot::Receiver` to resolve.
    ///
    pub async fn exists_watch_oneshot(
        &self,
        path: &str,
    ) -> Result<(Option<Stat>, Receiver<WatchedEvent>), failure::Error> {
        let (tx, rx) = oneshot::channel();
        self.exists_inner(path, WatchOption::Oneshot(tx))
            .await
            .map(|res| (res, rx))
    }

    async fn get_children_inner(
        &self,
        path: &str,
        watch: WatchOption,
    ) -> Result<Option<Vec<String>>, failure::Error> {
        trace!(self.logger, "get_children"; "path" => path, "watch" => ?watch);
        self.connection
            .enqueue(Request::GetChildren {
                path: path.to_string(),
                watch,
            })
            .await
            .and_then(transform::get_children)
    }

    ///
    /// Return the names of the children of the node at the given `path`, or
    /// `None` if the node does not exist.
    ///
    /// The returned list of children is not sorted and no guarantee is provided
    /// as to its natural or lexical order.
    ///
    pub async fn get_children(&self, path: &str) -> Result<Option<Vec<String>>, failure::Error> {
        self.get_children_inner(path, WatchOption::None).await
    }

    ///
    /// Return the names of the children of the node at the given `path`, or
    /// `None` if the node does not exist.
    ///
    /// The returned list of children is not sorted and no guarantee is provided
    /// as to its natural or lexical order.
    ///
    /// If no errors occur, a watch is left on the node at the given `path`. The
    /// watch is triggered by any successful operation that deletes the node at
    /// the given `path`, or creates or deletes a child of that node, and in
    /// turn causes an event to be sent over the default watcher.
    ///
    pub async fn get_children_watch(
        &self,
        path: &str,
    ) -> Result<Option<Vec<String>>, failure::Error> {
        self.get_children_inner(path, WatchOption::Global).await
    }

    ///
    /// Return the names of the children of the node at the given `path`, or
    /// `None` if the node does not exist.
    ///
    /// The returned list of children is not sorted and no guarantee is provided
    /// as to its natural or lexical order.
    ///
    /// If no errors occur, a watch is left on the node at the given `path`. The
    /// watch is triggered by any successful operation that deletes the node at
    /// the given `path`, or creates or deletes a child of that node, and in
    /// turn causes the returned `oneshot::Receiver` to resolve.
    ///
    pub async fn get_children_watch_oneshot(
        &self,
        path: &str,
    ) -> Result<(Option<Vec<String>>, Receiver<WatchedEvent>), failure::Error> {
        let (tx, rx) = oneshot::channel();
        self.get_children_inner(path, WatchOption::Oneshot(tx))
            .await
            .map(|res| (res, rx))
    }

    async fn get_data_inner(
        &self,
        path: &str,
        watch: WatchOption,
    ) -> Result<Option<(Vec<u8>, Stat)>, failure::Error> {
        trace!(self.logger, "get_data"; "path" => path, "watch" => ?watch);
        self.connection
            .enqueue(Request::GetData {
                path: path.to_string(),
                watch,
            })
            .await
            .and_then(transform::get_data)
    }

    ///
    /// Return the data and the [`Stat`] of the node at the given `path`, or
    /// `None` if it does not exist.
    ///
    pub async fn get_data(&self, path: &str) -> Result<Option<(Vec<u8>, Stat)>, failure::Error> {
        self.get_data_inner(path, WatchOption::None).await
    }

    ///
    /// Return the data and the [`Stat`] of the node at the given `path`, or
    /// `None` if it does not exist.
    ///
    /// If no errors occur, a watch is left on the node at the given `path`. The
    /// watch is triggered by any successful operation that sets the node's
    /// data, or deletes the node, and in turn causes an event to be sent over
    /// the default watcher.
    ///
    pub async fn get_data_watch(
        &self,
        path: &str,
    ) -> Result<Option<(Vec<u8>, Stat)>, failure::Error> {
        self.get_data_inner(path, WatchOption::Global).await
    }

    ///
    /// Return the data and the [`Stat`] of the node at the given `path`, or
    /// `None` if it does not exist.
    ///
    /// If no errors occur, a watch is left on the node at the given `path`. The
    /// watch is triggered by any successful operation that sets the node's
    /// data, or deletes the node, and in turn causes the returned
    /// `oneshot::Receiver` to resolve.
    ///
    pub async fn get_data_watch_oneshot(
        &self,
        path: &str,
    ) -> Result<(Option<(Vec<u8>, Stat)>, Receiver<WatchedEvent>), failure::Error> {
        let (tx, rx) = oneshot::channel();
        self.get_data_inner(path, WatchOption::Oneshot(tx))
            .await
            .map(|res| (res, rx))
    }

    ///
    /// Run the provided multi request.
    ///
    pub async fn run_multi(
        &self,
        request: MultiRequest,
    ) -> Result<Vec<Result<MultiResponse, error::Multi>>, failure::Error> {
        let reqs_lite: Vec<transform::RequestMarker> = request.0.iter().map(|r| r.into()).collect();
        self.connection
            .enqueue(Request::Multi(request.0))
            .await
            .and_then(move |r| match r {
                Ok(Response::Multi(responses)) => reqs_lite
                    .iter()
                    .zip(responses)
                    .map(|(req, res)| transform::multi(req, res))
                    .collect(),
                Ok(r) => bail!("got non-multi response to multi: {:?}", r),
                Err(e) => Err(format_err!("multi call failed: {:?}", e)),
            })
    }
}

///
/// Batches ZooKeeper operations into an atomic "multi" request.
///
#[derive(Debug, Default)]
pub struct MultiRequest(Vec<Request>);

impl MultiRequest {
    ///
    /// Create an empty multi request. Multi requests batch several operations
    /// into one atomic unit. The caller must then add operations to the
    /// multi request using the provided methods and run the multi request by
    /// calling `run_multi` on a `ZooKeeper` object.
    ///
    pub fn new() -> Self {
        MultiRequest(Vec::new())
    }

    ///
    /// Attach a create operation to this multi request.
    ///
    /// See [`ZooKeeper::create`] for details.
    ///
    pub fn create<D, A>(mut self, path: &str, data: D, acl: A, mode: CreateMode) -> Self
    where
        D: Into<Cow<'static, [u8]>>,
        A: Into<Cow<'static, [Acl]>>,
    {
        self.0.push(Request::Create {
            path: path.to_string(),
            data: data.into(),
            acl: acl.into(),
            mode,
        });
        self
    }

    ///
    /// Attach a set data operation to this multi request.
    ///
    /// See [`ZooKeeper::set_data`] for details.
    ///
    pub fn set_data<D>(mut self, path: &str, version: Option<i32>, data: D) -> Self
    where
        D: Into<Cow<'static, [u8]>>,
    {
        self.0.push(Request::SetData {
            path: path.to_string(),
            version: version.unwrap_or(-1),
            data: data.into(),
        });
        self
    }

    ///
    /// Attach a delete operation to this multi request.
    ///
    /// See [`ZooKeeper::delete`] for details.
    ///
    pub fn delete(mut self, path: &str, version: Option<i32>) -> Self {
        self.0.push(Request::Delete {
            path: path.to_string(),
            version: version.unwrap_or(-1),
        });
        self
    }

    ///
    /// Attach a check operation to this multi request.
    ///
    /// There is no equivalent to the check operation outside of a multi
    /// request.
    ///
    pub fn check(mut self, path: &str, version: i32) -> Self {
        self.0.push(Request::Check {
            path: path.to_string(),
            version,
        });
        self
    }
}

// TODO make these runnable and expand them
//
// #[cfg(test)]
// mod tests {
//     use super::*;

//     use slog::Drain;
//     use types::watchOption::{KeeperState, WatchedEventType};

//     #[test]
//     fn it_works() {
//         let mut rt = tokio::runtime::Runtime::new().unwrap();
//         let mut builder = ZooKeeperBuilder::default();
//         let decorator = slog_term::TermDecorator::new().build();
//         let drain = slog_term::FullFormat::new(decorator).build().fuse();
//         let drain = slog_async::Async::new(drain).build().fuse();
//         builder.set_logger(slog::Logger::root(drain, o!()));

//         let (zk, w): (ZooKeeper, _) = rt
//             .block_on(
//                 builder
//                     .connect(&"127.0.0.1:2181".parse().unwrap())
//                     .and_then(|(zk, w)| {
//                         zk.with_watcher()
//                             .exists("/foo")
//                             .inspect(|(_, _, stat)| assert_eq!(stat, &None))
//                             .and_then(|(zk, exists_w, _)| {
//                                 zk.watch()
//                                     .exists("/foo")
//                                     .map(move |(zk, x)| (zk, x, exists_w))
//                             })
//                             .inspect(|(_, stat, _)| assert_eq!(stat, &None))
//                             .and_then(|(zk, _, exists_w)| {
//                                 zk.create(
//                                     "/foo",
//                                     &b"Hello world"[..],
//                                     Acl::open_unsafe(),
//                                     CreateMode::Persistent,
//                                 )
//                                 .map(move |(zk, x)| (zk, x, exists_w))
//                             })
//                             .inspect(|(_, ref path, _)| {
//                                 assert_eq!(path.as_ref().map(String::as_str), Ok("/foo"))
//                             })
//                             .and_then(move |(zk, _, exists_w)| {
//                                 exists_w
//                                     .map(move |w| (zk, w))
//                                     .map_err(|e| format_err!("exists_w failed: {:?}", e))
//                             })
//                             .inspect(|(_, event)| {
//                                 assert_eq!(
//                                     event,
//                                     &WatchedEvent {
//                                         event_type: WatchedEventType::NodeCreated,
//                                         keeper_state: KeeperState::SyncConnected,
//                                         path: String::from("/foo"),
//                                     }
//                                 );
//                             })
//                             .and_then(|(zk, _)| zk.watch().exists("/foo"))
//                             .inspect(|(_, stat)| {
//                                 assert_eq!(stat.unwrap().data_length as usize, b"Hello world".len())
//                             })
//                             .and_then(|(zk, _)| zk.get_acl("/foo"))
//                             .inspect(|(_, res)| {
//                                 let res = res.as_ref().unwrap();
//                                 assert_eq!(res.0, Acl::open_unsafe())
//                             })
//                             .and_then(|(zk, _)| zk.get_data("/foo"))
//                             .inspect(|(_, res)| {
//                                 let data = b"Hello world";
//                                 let res = res.as_ref().unwrap();
//                                 assert_eq!(res.0, data);
//                                 assert_eq!(res.1.data_length as usize, data.len());
//                             })
//                             .and_then(|(zk, res)| {
//                                 zk.set_data("/foo", Some(res.unwrap().1.version), &b"Bye world"[..])
//                             })
//                             .inspect(|(_, stat)| {
//                                 assert_eq!(stat.unwrap().data_length as usize, "Bye world".len());
//                             })
//                             .and_then(|(zk, _)| zk.get_data("/foo"))
//                             .inspect(|(_, res)| {
//                                 let data = b"Bye world";
//                                 let res = res.as_ref().unwrap();
//                                 assert_eq!(res.0, data);
//                                 assert_eq!(res.1.data_length as usize, data.len());
//                             })
//                             .and_then(|(zk, _)| {
//                                 zk.create(
//                                     "/foo/bar",
//                                     &b"Hello bar"[..],
//                                     Acl::open_unsafe(),
//                                     CreateMode::Persistent,
//                                 )
//                             })
//                             .inspect(|(_, ref path)| {
//                                 assert_eq!(path.as_ref().map(String::as_str), Ok("/foo/bar"))
//                             })
//                             .and_then(|(zk, _)| zk.get_children("/foo"))
//                             .inspect(|(_, children)| {
//                                 assert_eq!(children, &Some(vec!["bar".to_string()]));
//                             })
//                             .and_then(|(zk, _)| zk.get_data("/foo/bar"))
//                             .inspect(|(_, res)| {
//                                 let data = b"Hello bar";
//                                 let res = res.as_ref().unwrap();
//                                 assert_eq!(res.0, data);
//                                 assert_eq!(res.1.data_length as usize, data.len());
//                             })
//                             .and_then(|(zk, _)| {
//                                 // add a new exists watch so we'll get notified of delete
//                                 zk.watch().exists("/foo")
//                             })
//                             .and_then(|(zk, _)| zk.delete("/foo", None))
//                             .inspect(|(_, res)| assert_eq!(res, &Err(error::Delete::NotEmpty)))
//                             .and_then(|(zk, _)| zk.delete("/foo/bar", None))
//                             .inspect(|(_, res)| assert_eq!(res, &Ok(())))
//                             .and_then(|(zk, _)| zk.delete("/foo", None))
//                             .inspect(|(_, res)| assert_eq!(res, &Ok(())))
//                             .and_then(|(zk, _)| zk.watch().exists("/foo"))
//                             .inspect(|(_, stat)| assert_eq!(stat, &None))
//                             .and_then(move |(zk, _)| {
//                                 w.into_future()
//                                     .map(move |x| (zk, x))
//                                     .map_err(|e| format_err!("stream error: {:?}", e.0))
//                             })
//                             .inspect(|(_, (event, _))| {
//                                 assert_eq!(
//                                     event,
//                                     &Some(WatchedEvent {
//                                         event_type: WatchedEventType::NodeCreated,
//                                         keeper_state: KeeperState::SyncConnected,
//                                         path: String::from("/foo"),
//                                     })
//                                 );
//                             })
//                             .and_then(|(zk, (_, w))| {
//                                 w.into_future()
//                                     .map(move |x| (zk, x))
//                                     .map_err(|e| format_err!("stream error: {:?}", e.0))
//                             })
//                             .and_then(|(zk, (event, w))| {
//                                 assert_eq!(
//                                     event,
//                                     Some(WatchedEvent {
//                                         event_type: WatchedEventType::NodeDataChanged,
//                                         keeper_state: KeeperState::SyncConnected,
//                                         path: String::from("/foo"),
//                                     })
//                                 );

//                                 w.into_future()
//                                     .map(move |x| (zk, x))
//                                     .map_err(|e| format_err!("stream error: {:?}", e.0))
//                             })
//                             .inspect(|(_, (event, _))| {
//                                 assert_eq!(
//                                     event,
//                                     &Some(WatchedEvent {
//                                         event_type: WatchedEventType::NodeDeleted,
//                                         keeper_state: KeeperState::SyncConnected,
//                                         path: String::from("/foo"),
//                                     })
//                                 );
//                             })
//                             .map(|(zk, (_, w))| (zk, w))
//                     }),
//             )
//             .unwrap();

//         drop(zk); // make Packetizer idle
//         rt.shutdown_on_idle().wait().unwrap();
//         assert_eq!(w.wait().count(), 0);
//     }

//     #[test]
//     fn example() {
//         tokio::run(
//             ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
//                 .and_then(|(zk, default_watcher)| {
//                     // let's first check if /example exists. the .watch() causes us to be notified
//                     // the next time the "exists" status of /example changes after the call.
//                     zk.watch()
//                         .exists("/example")
//                         .inspect(|(_, stat)| {
//                             // initially, /example does not exist
//                             assert_eq!(stat, &None)
//                         })
//                         .and_then(|(zk, _)| {
//                             // so let's make it!
//                             zk.create(
//                                 "/example",
//                                 &b"Hello world"[..],
//                                 Acl::open_unsafe(),
//                                 CreateMode::Persistent,
//                             )
//                         })
//                         .inspect(|(_, ref path)| {
//                             assert_eq!(path.as_ref().map(String::as_str), Ok("/example"))
//                         })
//                         .and_then(|(zk, _)| {
//                             // does it exist now?
//                             zk.watch().exists("/example")
//                         })
//                         .inspect(|(_, stat)| {
//                             // looks like it!
//                             // note that the creation above also triggered our "exists" watch!
//                             assert_eq!(stat.unwrap().data_length as usize, b"Hello world".len())
//                         })
//                         .and_then(|(zk, _)| {
//                             // did the data get set correctly?
//                             zk.get_data("/example")
//                         })
//                         .inspect(|(_, res)| {
//                             let data = b"Hello world";
//                             let res = res.as_ref().unwrap();
//                             assert_eq!(res.0, data);
//                             assert_eq!(res.1.data_length as usize, data.len());
//                         })
//                         .and_then(|(zk, res)| {
//                             // let's update the data.
//                             zk.set_data("/example", Some(res.unwrap().1.version), &b"Bye world"[..])
//                         })
//                         .inspect(|(_, stat)| {
//                             assert_eq!(stat.unwrap().data_length as usize, "Bye world".len());
//                         })
//                         .and_then(|(zk, _)| {
//                             // create a child of /example
//                             zk.create(
//                                 "/example/more",
//                                 &b"Hello more"[..],
//                                 Acl::open_unsafe(),
//                                 CreateMode::Persistent,
//                             )
//                         })
//                         .inspect(|(_, ref path)| {
//                             assert_eq!(path.as_ref().map(String::as_str), Ok("/example/more"))
//                         })
//                         .and_then(|(zk, _)| {
//                             // it should be visible as a child of /example
//                             zk.get_children("/example")
//                         })
//                         .inspect(|(_, children)| {
//                             assert_eq!(children, &Some(vec!["more".to_string()]));
//                         })
//                         .and_then(|(zk, _)| {
//                             // it is not legal to delete a node that has children directly
//                             zk.delete("/example", None)
//                         })
//                         .inspect(|(_, res)| assert_eq!(res, &Err(error::Delete::NotEmpty)))
//                         .and_then(|(zk, _)| {
//                             // instead we must delete the children first
//                             zk.delete("/example/more", None)
//                         })
//                         .inspect(|(_, res)| assert_eq!(res, &Ok(())))
//                         .and_then(|(zk, _)| zk.delete("/example", None))
//                         .inspect(|(_, res)| assert_eq!(res, &Ok(())))
//                         .and_then(|(zk, _)| {
//                             // no /example should no longer exist!
//                             zk.exists("/example")
//                         })
//                         .inspect(|(_, stat)| assert_eq!(stat, &None))
//                         .and_then(move |(zk, _)| {
//                             // now let's check that the .watch().exists we did in the very
//                             // beginning actually triggered!
//                             default_watcher
//                                 .into_future()
//                                 .map(move |x| (zk, x))
//                                 .map_err(|e| format_err!("stream error: {:?}", e.0))
//                         })
//                         .inspect(|(_, (event, _))| {
//                             assert_eq!(
//                                 event,
//                                 &Some(WatchedEvent {
//                                     event_type: WatchedEventType::NodeCreated,
//                                     keeper_state: KeeperState::SyncConnected,
//                                     path: String::from("/example"),
//                                 })
//                             );
//                         })
//                 })
//                 .map(|_| ())
//                 .map_err(|e| panic!("{:?}", e)),
//         );
//     }

//     #[test]
//     fn acl_test() {
//         let mut rt = tokio::runtime::Runtime::new().unwrap();
//         let mut builder = ZooKeeperBuilder::default();
//         let decorator = slog_term::TermDecorator::new().build();
//         let drain = slog_term::FullFormat::new(decorator).build().fuse();
//         let drain = slog_async::Async::new(drain).build().fuse();
//         builder.set_logger(slog::Logger::root(drain, o!()));

//         let (zk, _): (ZooKeeper, _) = rt
//             .block_on(
//                 builder
//                     .connect(&"127.0.0.1:2181".parse().unwrap())
//                     .and_then(|(zk, _)| {
//                         zk.create(
//                             "/acl_test",
//                             &b"foo"[..],
//                             Acl::open_unsafe(),
//                             CreateMode::Ephemeral,
//                         )
//                         .and_then(|(zk, _)| zk.get_acl("/acl_test"))
//                         .inspect(|(_, res)| {
//                             let res = res.as_ref().unwrap();
//                             assert_eq!(res.0, Acl::open_unsafe())
//                         })
//                         .and_then(|(zk, res)| {
//                             zk.set_acl(
//                                 "/acl_test",
//                                 Acl::creator_all(),
//                                 Some(res.unwrap().1.version),
//                             )
//                         })
//                         .inspect(|(_, res)| {
//                             // a not authenticated user is not able to set `auth` scheme acls.
//                             assert_eq!(res, &Err(error::SetAcl::InvalidAcl))
//                         })
//                         .and_then(|(zk, _)| zk.set_acl("/acl_test", Acl::read_unsafe(), None))
//                         .inspect(|(_, stat)| {
//                             // successfully change node acl to `read_unsafe`
//                             assert_eq!(stat.unwrap().data_length as usize, b"foo".len())
//                         })
//                         .and_then(|(zk, _)| zk.get_acl("/acl_test"))
//                         .inspect(|(_, res)| {
//                             let res = res.as_ref().unwrap();
//                             assert_eq!(res.0, Acl::read_unsafe())
//                         })
//                         .and_then(|(zk, _)| zk.set_data("/acl_test", None, &b"bar"[..]))
//                         .inspect(|(_, res)| {
//                             // cannot set data on a read only node
//                             assert_eq!(res, &Err(error::SetData::NoAuth))
//                         })
//                         .and_then(|(zk, _)| zk.set_acl("/acl_test", Acl::open_unsafe(), None))
//                         .inspect(|(_, res)| {
//                             // cannot change a read only node's acl
//                             assert_eq!(res, &Err(error::SetAcl::NoAuth))
//                         })
//                     }),
//             )
//             .unwrap();

//         drop(zk); // make Packetizer idle
//         rt.shutdown_on_idle().wait().unwrap();
//     }

//     #[test]
//     fn multi_test() {
//         let mut rt = tokio::runtime::Runtime::new().unwrap();
//         let mut builder = ZooKeeperBuilder::default();
//         let decorator = slog_term::TermDecorator::new().build();
//         let drain = slog_term::FullFormat::new(decorator).build().fuse();
//         let drain = slog_async::Async::new(drain).build().fuse();
//         builder.set_logger(slog::Logger::root(drain, o!()));

//         let check_exists = |zk: ZooKeeper, paths: &'static [&'static str]| {
//             let mut fut: Box<
//                 futures::Future<Output = Result<(ZooKeeper, Vec<bool>), failure::Error>> + Send,
//             > = Box::new(futures::future::ok((zk, Vec::new())));
//             for p in paths {
//                 fut = Box::new(fut.and_then(move |(zk, mut v)| {
//                     zk.exists(p).map(|(zk, stat)| {
//                         v.push(stat.is_some());
//                         (zk, v)
//                     })
//                 }))
//             }
//             fut
//         };

//         let (zk, _): (ZooKeeper, _) = rt
//             .block_on(
//                 builder
//                     .connect(&"127.0.0.1:2181".parse().unwrap())
//                     .and_then(|(zk, _)| {
//                         zk.multi()
//                             .create("/b", &b"a"[..], Acl::open_unsafe(), CreateMode::Persistent)
//                             .create("/c", &b"b"[..], Acl::open_unsafe(), CreateMode::Persistent)
//                             .run()
//                     })
//                     .inspect(|(_, res)| {
//                         assert_eq!(
//                             res,
//                             &[
//                                 Ok(MultiResponse::Create("/b".into())),
//                                 Ok(MultiResponse::Create("/c".into()))
//                             ]
//                         )
//                     })
//                     .and_then(move |(zk, _)| check_exists(zk, &["/a", "/b", "/c", "/d"]))
//                     .inspect(|(_, res)| assert_eq!(res, &[false, true, true, false]))
//                     .and_then(|(zk, _)| {
//                         zk.multi()
//                             .create("/a", &b"a"[..], Acl::open_unsafe(), CreateMode::Persistent)
//                             .create("/b", &b"b"[..], Acl::open_unsafe(), CreateMode::Persistent)
//                             .create("/c", &b"b"[..], Acl::open_unsafe(), CreateMode::Persistent)
//                             .create("/d", &b"a"[..], Acl::open_unsafe(), CreateMode::Persistent)
//                             .run()
//                     })
//                     .inspect(|(_, res)| {
//                         assert_eq!(
//                             res,
//                             &[
//                                 Err(error::Multi::RolledBack),
//                                 Err(error::Multi::Create(error::Create::NodeExists)),
//                                 Err(error::Multi::Skipped),
//                                 Err(error::Multi::Skipped),
//                             ]
//                         )
//                     })
//                     .and_then(move |(zk, _)| check_exists(zk, &["/a", "/b", "/c", "/d"]))
//                     .inspect(|(_, res)| assert_eq!(res, &[false, true, true, false]))
//                     .and_then(|(zk, _)| zk.multi().set_data("/b", None, &b"garbaggio"[..]).run())
//                     .inspect(|(_, res)| match res[0] {
//                         Ok(MultiResponse::SetData(stat)) => {
//                             assert_eq!(stat.data_length as usize, "garbaggio".len())
//                         }
//                         _ => panic!("unexpected response: {:?}", res),
//                     })
//                     .and_then(|(zk, _)| zk.multi().check("/b", 0).delete("/c", None).run())
//                     .inspect(|(_, res)| {
//                         assert_eq!(
//                             res,
//                             &[
//                                 Err(error::Multi::Check(error::Check::BadVersion {
//                                     expected: 0
//                                 })),
//                                 Err(error::Multi::Skipped),
//                             ]
//                         )
//                     })
//                     .and_then(move |(zk, _)| check_exists(zk, &["/a", "/b", "/c", "/d"]))
//                     .inspect(|(_, res)| assert_eq!(res, &[false, true, true, false]))
//                     .and_then(|(zk, _)| zk.multi().check("/a", 0).run())
//                     .inspect(|(_, res)| {
//                         assert_eq!(res, &[Err(error::Multi::Check(error::Check::NoNode)),])
//                     })
//                     .and_then(|(zk, _)| {
//                         zk.multi()
//                             .check("/b", 1)
//                             .delete("/b", None)
//                             .check("/c", 0)
//                             .delete("/c", None)
//                             .run()
//                     })
//                     .inspect(|(_, res)| {
//                         assert_eq!(
//                             res,
//                             &[
//                                 Ok(MultiResponse::Check),
//                                 Ok(MultiResponse::Delete),
//                                 Ok(MultiResponse::Check),
//                                 Ok(MultiResponse::Delete),
//                             ]
//                         )
//                     })
//                     .and_then(move |(zk, _)| check_exists(zk, &["/a", "/b", "/c", "/d"]))
//                     .inspect(|(_, res)| assert_eq!(res, &[false, false, false, false])),
//             )
//             .unwrap();

//         drop(zk); // make Packetizer idle
//         rt.shutdown_on_idle().wait().unwrap();
//     }
// }
