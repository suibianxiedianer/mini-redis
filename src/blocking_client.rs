//! 所谓的阻塞客户端，即自身包括一个单线程的异步运行时，并以阻塞的方式运行异步代码。
//!
//! 为的是方便在同步环境下直接使用此客户端，所有的参数都与异步客户端相同。
//!
//! # 示例
//!
//! ```no_run
//! use mini_redis::blocking_client;
//!
//! fn main() {
//!     let client = match blocking_client::connect("localhost:6379") {
//!         Ok(client) => client,
//!         err(_) => panic!("failed to establish connection"),
//!     };
//!
//!     client.set("foo", "bar".into()).unwrap();
//!
//!     let val = client.get("foo").unwrap().unwrap();
//!     assert_eq!(val, "bar");
//! }
//! ```
use std::time::Duration;

use bytes::Bytes;
use tokio::{
    net::ToSocketAddrs,
    runtime::Runtime,
};

pub use crate::client::Message;

/// 与 Redis 服务建立连接
///
/// `BlockingClient` 提供基本的网络客户端功能，使用 [`connect`](fn@connect) 函数建立连接
///
/// 使用 `Client` 的方法来处理请求
pub struct BlockingClient {
    // 异步客户端
    inner: crate::client::Client,

    // 一个 `current_thread` 运行时，用于在客户端上以阻塞的方式运行异步操作
    rt: Runtime,
}

/// 一个处于 “订阅”/“取消订阅” 模式的客户端
///
/// 一旦客户端执行了订阅操作，它将进入订阅模式，并由客户端转变为订阅端，此模式仅支持
/// `订阅`、`取消订阅` 命令。
pub struct BlockingSubscriber {
    // 异步的订阅客户端连接
    inner: crate::client::Subscriber,

    // 一个 `current_thread` 运行时，用于在订阅客户端上以阻塞的方式运行异步操作
    rt: Runtime,
}

/// 由 `Subscriber::into_iter()` 返回的迭代器 
pub struct SubscriberIterator {
    inner: crate::client::Subscriber,
    rt: Runtime,
}

/// 与 Redis 服务建立连接并返回一个 `BlockingClient`
///
/// # 示例
///
/// ```no_run
/// use mini_redis::blocking_client;
///
/// fn main() {}
///     let client = match blocking_client::connect("localhost:6379") {
///         Ok(client) => client,
///         Err(_) => panic!("failed to establish connection"),
///     };
///
///     drop(client);
/// ```
pub fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<BlockingClient> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let inner = rt.block_on(crate::client::connect(addr))?;

    Ok(BlockingClient { inner, rt })
}

impl BlockingClient {
    pub fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        self.rt.block_on(self.inner.get(key))
    }

    pub fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.rt.block_on(self.inner.set(key, value))
    }

    pub fn set_expires(&mut self, key: &str, value: Bytes, expiration: Duration) -> crate::Result<()> {
        self.rt.block_on(self.inner.set_expires(key, value, expiration))
    }

    pub fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        self.rt.block_on(self.inner.publish(channel, message))
    }

    pub fn subscribe(self, channels: Vec<String>) -> crate::Result<BlockingSubscriber> {
        let subscriber = self.rt.block_on(self.inner.subscribe(channels))?;

        Ok(BlockingSubscriber {
            inner: subscriber,
            rt: self.rt,
        })
    }
}

impl BlockingSubscriber {
    pub fn get_subscribed(&self) -> &[String] {
        &self.inner.get_subscribed()
    }

    pub fn next_message(&mut self) -> crate::Result<Option<Message>> {
        self.rt.block_on(self.inner.next_message())
    }

    pub fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.subscribe(channels))
    }

    pub fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.unsubscribe(channels))
    }

    /// 将自身转化为 `SubscriberIterator`
    pub fn into_iter(self) -> impl Iterator<Item = crate::Result<Message>> {
        SubscriberIterator {
            inner: self.inner,
            rt: self.rt
        }
    }
}

impl Iterator for SubscriberIterator {
    type Item = crate::Result<Message>;

    fn next(&mut self) -> Option<Self::Item> {
        // transpose 将 Result 和 Option 互相转换
        self.rt.block_on(self.inner.next_message()).transpose()
    }
}
