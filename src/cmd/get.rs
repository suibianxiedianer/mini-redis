use bytes::Bytes;
use tracing::{debug, instrument};

use crate::{Frame, Connection, Parse, Db};

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    /// 从一个实现了 `ToString` 特征的值得到 `Get`
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    /// 得到键的值
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 何时，为什么
    /// TODO
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        let key = parse.next_string()?;

        Ok(Get { key })
    }

    /// 从数据库中查找结果，并写入客户端的连接
    /// TODO: 下面 instrument 意义何在
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if let Some(value) = db.get(&self.key) {
            Frame::Bulk(value)
        } else {
            Frame::Null
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 客户端向服务器请求时，调用此命令将 `Get` 转换为 `Frame` 并发送
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
