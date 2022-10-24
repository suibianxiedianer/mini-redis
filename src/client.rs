use std::{
    io::{Error, ErrorKind},
    time::Duration,
};

use async_stream::try_stream;
use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_stream::Stream;
use tracing::{debug, instrument};

use crate::{
    cmd::{Get, Set, Publish, Subscribe, Unsubscribe, Ping},
    Connection, Frame,
};

/// 与 Redis 服务建立连接
/// 实现 `Get`/`Set`/`Publish`/`Subscribe`/`Unsubscribe`/`Ping` 命令
#[derive(Debug)]
pub struct Client {
    connection: Connection,
}

/// 一个实现了订阅/取消模式的客户端
/// 当开始订阅消息后，`Client` 将会转化为 `Subscriber`
pub struct Subscriber {
    client: Client,
    subscribed_channels: Vec<String>,
}

/// 订阅频道发送的消息
#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

/// 通过给定的地址来和服务端建立起连接
///
/// # 示例
/// ```no_run
/// use mini_redis::client;
///
/// #[tokio::main]
/// async fn main() {
///     let client = match client::connect("localhost:6379").await {
///         Ok(client) => client,
///         Err(_) => panic!("failed to establish connection"),
///     }
///
///     drop(client);
/// }
/// ```
pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
    // 尝试和服务建立连接
    let socket = TcpStream::connect(addr).await?;

    Ok(Client { connection: Connection::new(socket) })
}

impl Client {
    /// Get：查找指定键保存的值
    /// 如果此键值对不存在则返回 `None`
    ///
    /// # 示例
    /// ```no_run
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connect("localhost:6379").await?;
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     println!("Got foo = {:?}", val);
    /// }
    /// ```
    ///
    #[instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        let frame = Get::new(key).into_frame();
        debug!(?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// 将一个值保存到一个键上
    /// 此键上的值可以被重写覆盖，若值被重写，则其有效期也将重置
    ///
    /// # 示例
    /// ```no_run
    /// use mini_redis::client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connect("localhost:6379").await?;
    ///
    ///     client.set("foo", "bar".into()).await.unwrap();
    ///
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "val");
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.set_cmd(Set::new(key, value, None)).await
    }

    /// 将一个值保存到一个键上，并设置其有效期，过期后键值会被删除
    /// 此键上的值可以被重写覆盖，若值被重写，则其有效期也将重置
    ///
    /// # 示例
    /// ```no_run
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connection("localhost:6379").await?;
    ///     let expiration = Duration::from_millis(500);
    ///
    ///     client.set_expires("foo", "bar".into(), expiration).await.unwrap();
    ///
    ///     // 立即查找，有结果
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    ///
    ///     // 过期啦
    ///     time.sleep(expiration).await;
    ///     let val = client.get("foo").await.unwrap();
    ///     assert!(val.is_none());
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn set_expires(&mut self, key: &str, value: Bytes, expiration: Duration) -> crate::Result<()> {
        self.set_cmd(Set::new(key, value, Some(expiration))).await
    }

    async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        let frame = cmd.into_frame();

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }

    /// 给指定的频道发送消息，返回当前监听此频道的订阅者数量
    /// 无法确保每个监听的接收者可以收到消息，因为连接可能随时断开
    ///
    /// # 示例
    /// ```no_run
    /// use mini_redis::client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connect("localhost:6379").await?;
    ///
    ///     let num = client.publish("foo", "bar".into()).await.unwrap();
    ///     println!("Got = {:?}", num);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        let frame = Publish::new(channel, message).into_frame();
        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Integer(response) => Ok(response),
            frame => Err(frame.to_error()),
        }
    }

    /// 客户端订阅指定的频道
    /// 一旦客户端执行了订阅的命令，它将消耗掉自身并返回一个 `Subscriber`
    /// 新的 `Subscriber` 客户端会保持连接，接收订阅的频道的消息，它仅可执行订阅相关的命令
    #[instrument(skip(self))]
    pub async fn subscribe(mut self, channels: Vec<String>) -> crate::Result<Subscriber> {
        self.subscribe_cmd(&channels).await?;

        Ok(Subscriber {
            client: self,
            subscribed_channels: channels,
        })
    }

    //// subscribe 命令的核心逻辑
    async fn subscribe_cmd(&mut self, channels: &[String]) -> crate::Result<()> {
        let frame = Subscribe::new(channels).into_frame();
        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        // 在 `Subscribe` 命令的实现中，使用 `drain` 消费 channels 
        // 每订阅一个频道，就会返回一条消息，故消息顺序是与 channels 一致的
        // 格式为 ["subscrib", channel, sub_nums]
        for channel in channels {
            match self.read_response().await? {
                Frame::Array(frame) => match frame.as_slice() {
                    [subscribe, schannel, ..]
                        if *subscribe == "subscribe" && *schannel == channel => {},
                    _ => return Err(Frame::Array(frame).to_error()),
                },
                frame => return Err(frame.to_error()),
            }
        }

        Ok(())
    }

    /// Ping 服务端
    /// 未指定消息时返回 `PONG`，否则返回 `Ping` 相同的消息
    ///
    /// # 示例
    /// ```
    /// use mini_redis::client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connect("localhost:6379").await.unwrap();
    ///
    ///     let pong = client.ping(None).await.unwrap();
    ///     assert_eq!(b"PONG", &pong[..]);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn ping(&mut self, msg: Option<String>) -> crate::Result<Bytes> {
        let frame = Ping::new(msg).into_frame();

        self.connection.write_frame(&frame).await?;

        // Simple 或 Bulk 是正常返回值
        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    /// 从当前连接中读取返回消息
    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;
        debug!(?response);

        match response {
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                // 正常来讲，一定会有返回消息，若为 None 服务端断开了连接
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");
                Err(err.into())
            }
        }
    }
}

impl Subscriber {
    /// 返回已订阅的频道列表
    pub fn get_subscribed(&self) -> &[String] {
        &self.subscribed_channels
    }

    pub fn into_stream(mut self) -> impl Stream<Item = crate::Result<Message>> {
        try_stream! {
            while let Some(message) = self.next_message().await? {
                yield message;
            }
        }
    }

    /// 接收订阅的频道发送的消息
    /// 正确的消息格式为 ["message", channel, msg]
    #[instrument(skip(self))]
    pub async fn next_message(&mut self) -> crate::Result<Option<Message>> {
        match self.client.connection.read_frame().await? {
            Some(frame) => {
                debug!(?frame);

                match frame {
                    Frame::Array(frame) => match frame.as_slice() {
                        [message, channel, content] if *message == "message" => Ok(Some(Message {
                            channel: channel.to_string(),
                            content: Bytes::from(content.to_string()),
                        })),
                        _ => Err(Frame::Array(frame).to_error()),
                    },
                    frame => Err(frame.to_error()),
                }
            }
            // 连接断开
            None => Ok(None),
        }
    }

    #[instrument(skip(self))]
    pub async fn subscribe(&mut self, channels: Vec<String>) -> crate::Result<()> {
        unimplemented!()
    }

    #[instrument(skip(self))]
    pub async fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        unimplemented!()
    }
}
