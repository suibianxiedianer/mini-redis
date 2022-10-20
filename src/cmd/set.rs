use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

use crate::{Connection, Db, Frame, Parse, ParseError};

/// 设置一个键，对应保存一个数据，可以选择设置键值的有效期
/// 若数据库中已有此键保存数据，则更新其值
#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    /// 新建一条 `Set` 命令
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire
        }
    }

    /// 返回键名
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 返回值
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// 返回有效期
    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }

    /// 和 `Get` 类似
    /// Frame::Array(Vec)
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        let key = parse.next_string()?;
        let value = parse.next_bytes()?;
        let mut expire = None;

        match parse.next_string() {
            // 当前只支持设置秒或毫秒，标志为 EX/PX
            Ok(s) if s.to_uppercase() == "EX" => {
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            },
            Ok(s) if s.to_uppercase() == "PX" => {
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            },
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            // 没设置生效时间，无妨
            Err(EndOfStream) => {},
            Err(err) => return Err(err.into())
        }

        Ok( Set { key, value, expire } )
    }

    /// 服务端调用此函数，向数据库中写入，并返回结果
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.set(self.key, self.value, self.expire);

        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 客户端向服务器请求时，调用此命令将 `Set` 转换为 `Frame` 并发送
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);

        // 这里只使用毫秒，更精确的缘故？
        if let Some(expire) = self.expire {
            frame.push_bulk(Bytes::from("px".as_bytes()));
            frame.push_int(expire.as_millis() as u64);
        }

        frame
    }
}
