use bytes::Bytes;
use tracing::instrument;

use crate::{Frame, Connection, Parse, ParseError};

#[derive(Debug, Default)]
pub struct Ping {
    msg: Option<String>,
}

impl Ping {
    /// 新建一条 `Ping` 命令
    pub fn new(msg: Option<String>) -> Self {
        Ping { msg }
    }

    /// 从 `Parse` 中解析出 `Ping` 命令，命令头已被读取
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_string() {
            Ok(msg) => Ok(Ping { msg: Some(msg) }),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(err) => Err(err.into()),
        }
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            Some(msg) => Frame::Bulk(Bytes::from(msg)),
            None => Frame::Simple("PONG".to_string()),
        };

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();

        frame.push_bulk(Bytes::from("ping".as_bytes()));
        if let Some(msg) = self.msg {
            frame.push_bulk(Bytes::from(msg));
        }

        frame
    }
}
