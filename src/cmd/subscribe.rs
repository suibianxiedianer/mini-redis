use std::pin::Pin;

use bytes::Bytes;
use tokio::{
    select,
    sync::broadcast,
};
use tokio_stream::{Stream, StreamExt, StreamMap};

use crate::{
    Frame, Connection, Command, Db, Parse, ParseError, Shutdown,
    cmd::Unknown,
};

/// 订阅一个或多个频道
/// 一旦客户端进入订阅模式，它只能使用
/// `SUBSCRIBE`/`PSUSCRIBE`/`UNSUBSCRIBE`/`PUNSUBSCRIBE`/`PING`/`QUIT` 命令
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// 客户端取消某个或某几个频道的订阅
/// 若不指定频道，则取消所有现有订阅
#[derive(Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// 消息流
/// TODO
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// 根据指定的频道创建一个 `Subscribe` 命令
    pub(crate) fn new(channels: &[String]) -> Self {
        Subscribe {
            channels: channels.to_vec(),
        }
    }

    /// 从 `Parse` 中解析出 `Subscribe` 命令，此时 `SUBSCRIBE` 头已被读取
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        // 至少得订阅一个频道
        let mut channels = vec![parse.next_string()?];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    } 

    /// 服务端收到请求后，建立连接？
    pub(crate) async fn apply(mut self, db: &Db, dst: &mut Connection, shutdown: &mut Shutdown) -> crate::Result<()> {
        // 使用 StreamMap 保存订阅的频道
        let mut subscriptions = StreamMap::new();

        loop {
            // 消费掉 channels 条目，订阅频道，返回结果
            for channel in self.channels.drain(..) {
                subscribe_to_channel(channel, &mut subscriptions, db, dst).await?;
            }

            // select 等待下面几个事件
            select! {
                // 订阅的频道有新消息
                Some((channel, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel, msg)).await?;
                },
                // 客户端发送了新的请求，或者连接断开
                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        None => return Ok(()),
                    };

                    // 这里处理客户端发送的消息
                    handle_command(frame, &mut self.channels, &mut subscriptions, dst).await?;
                },
                _ = shutdown.recv() => {
                    return Ok(())
                }
            }
        }
    }

    /// 客户端发送请求前转换为 `Frame`，与 parse_frames 对应
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}

/// 订阅一个频道，并将接收消息的 stream 流放入 subscriptions 订阅列表里
/// 若订阅成功，向客户端返回消息
async fn subscribe_to_channel(
    channel: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection
    ) -> crate::Result<()> {
    let mut rx = db.subscribe(channel.clone());

    // 返回一个固定的，实现了 async/await 的 stream
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                Err(broadcast::error::RecvError::Lagged(_)) => {},
                Err(_) => break,
            }
        }
    });

    subscriptions.insert(channel.clone(), rx);

    let response = make_subscribe_frame(channel, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

/// 订阅后，服务端返回的消息
fn make_subscribe_frame(channel: String, sub_nums: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel));
    response.push_int(sub_nums as u64);

    response
}

/// 取消订阅后，服务端返回的消息
/// 取消订阅的频道名，和当前订阅的数量
fn make_unsubscribe_frame(channel: String, sub_nums: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel));
    response.push_int(sub_nums as u64);

    response
}

/// 从订阅频道的消息生成 frame
fn make_message_frame(channel: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel));
    response.push_bulk(msg);

    response
}

/// 处理客户端发送的命令
async fn handle_command(
    frame: Frame,
    channels: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection
    ) -> crate::Result<()> {
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            // vec.extend(append) 使用迭代器的内容扩展集合
            channels.extend(subscribe.channels.into_iter());
        },
        Command::Unsubscribe(mut unsubscribe) => {
            // 若未指定 channels 则清空所有现有订阅
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel| channel.to_string())
                    .collect();
            }

            for channel in unsubscribe.channels {
                subscriptions.remove(&channel);

                let response = make_unsubscribe_frame(channel, subscriptions.len());
                dst.write_frame(&response).await?;
            }
        },
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        },
    }
    Ok(())
}

impl Unsubscribe {
    /// 使用给定的 `channels` 创建一个 `Unsubscribe` 命令
    pub(crate) fn new(channels: &[String]) -> Self {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    /// 命令头已被读取，继续读取 channels 列表并生成 `Unsubscribe` 命令
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, ParseError> {
        use ParseError::EndOfStream;

        let mut channels = vec![];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(EndOfStream) => break,
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe { channels })
    }

    /// 客户端发送请求前将命令转换为 `Frame`
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from_static(b"unsubscribe"));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}
