use bytes::Bytes;

use crate::{Frame, Connection, Db, Parse};

#[derive(Debug)]
pub struct Publish {
    channel: String,
    message: Bytes,
}

impl Publish {
    /// 创建一个 `Publish` 命令，包含对应的广播频道和要发送的消息
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Self {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }

    /// 服务收到命令的 `Frame::Array` ，确认何种命令后调此生成对应的命令
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        let channel = parse.next_string()?;
        let message = parse.next_bytes()?;

        Ok(Publish { channel, message })
    }

    /// 服务端接收命令后，处理并返回
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let num_subscribers = db.publish(&self.channel, self.message);
        let response = Frame::Integer(num_subscribers as u64);
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 客户端发送命令前，转换为 `Frame`
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);

        frame
    }
}
