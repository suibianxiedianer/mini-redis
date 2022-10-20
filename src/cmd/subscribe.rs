use std::pin::Pin;

use bytes::Bytes;
use tokio_stream::{Stream, StreamExt, StreamMap};

use crate::{Frame, Connection, Db, Parse, ParseError, Shutdown};

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

    pub(crate) async fn apply(self, db: &Db, dst: &Connection, shutdown: &mut Shutdown) -> crate::Result<()> {
        Ok(())
    }
}
