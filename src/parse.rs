#![cfg_attr(debug_assertions, allow(dead_code, unused_imports, unused_variables, unused_mut))]
use std::{fmt, str, vec};

use crate::Frame;

use bytes::Bytes;

/// 一个用来处理 Frame::Array 的小工具？
/// 主要提供了几个 next 方法，方便起见
#[derive(Debug)]
pub(crate) struct Parse {
    parts: vec::IntoIter<Frame>,
}

#[derive(Debug)]
pub(crate) enum ParseError {
    EndOfStream,
    Other(crate::Error)
}

impl Parse {
    /// 由 `frame` 得到一个新的 `Parse`
    /// `frame` 必须是一个数组类 `frame`，否则返回 `ParseError`
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let arr = match frame {
            Frame::Array(arr) => arr,
            frame => {
                return Err(format!("protocol error: expected array but got {:?}", frame).into())
            },
        };

        Ok(Parse { parts: arr.into_iter() })
    }

    /// 返回下一个 `frame`
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// 将下一个 frame 作为字符串返回
    /// 只支持 Simple 和 Bulk 的转换
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error: invalid string.".into()),
            frame => Err(format!("protocol error: expected Simple/Bulk frame, but got {:?}", frame).into()),
        }
    }

    /// 将下一个 frame 作为 raw bytes 串返回
    /// 只支持 Simple 和 Bulk 的转换
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!("protocol error: expected Simple/Bulk frame, but got {:?}", frame).into()),
        }
    }

    /// 读取下一个 frame 并尝试转换为整数
    /// 也可从 Simple/Bulk 解析出整数
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;

        const MSG: &str = "protocol error: invalid number";

        match self.next()? {
            Frame::Integer(i) => Ok(i),
            Frame::Simple(s) => atoi::<u64>(&s.into_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("protocol error: expected Integer/Simple/Bulk frame, but got {:?}", frame).into()),
        }
    }

    /// 确保 array 中没有更多的可读数据
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error: expected end of the frame, but there was more.".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> Self {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> Self {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error: unnexpected end of stream.".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}
