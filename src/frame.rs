#![cfg_attr(debug_assertions, allow(dead_code, unused_imports, unused_variables, unused_mut))]
//! 明确一下 Frame，是 redis 服务里传输数据时使用的数据结构
//! 并提供从字节流中解析出 frames 的函数
use std::{
    convert::TryInto,
    fmt,
    io::Cursor,
    num::TryFromIntError,
    string::FromUtf8Error,
};

use bytes::{Buf, Bytes};

/// Redis 协议里使用的 frame
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),         // b'+' + bytes + '\r\n'
    Error(String),          // b'-' + bytes + '\r\n'
    Integer(u64),           // b':' + bytes(num) + '\r\n'
    Null,                   // b"$" + b'-1' + '\r\n'
    Bulk(Bytes),            // b'$' + bytes(num) + '\r\n' + bytes(data) + '\r\n'
    Array(Vec<Frame>),      // b'*' + bytes(len) + '\r\n' + bytes(frames)
}

#[derive(Debug)]
pub enum Error {
    /// 没有足够的数据来解析出消息
    Incomplete,

    /// 其它错误
    Other(crate::Error)
}

impl Frame {
    /// 返回一个空白的 Frame::Array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// 将 `Bulk` 放入 array 中，`self` 必须为 Frame::Array
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(array) => {
                array.push(Frame::Bulk(bytes));
            },
            _ => panic!("not an array frame"),
        }
    }

    /// 将 `u64` 放入 array 中，`self` 必须为 Frame::Array
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(array) => {
                array.push(Frame::Integer(value))
            },
            _ => panic!("not an array frame"),
        }
    }

    /// 检查是否可以从 `src` 中解析出一条 Frame 消息
    pub(crate) fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        // 读取 src 中第一个字符，
        match get_u8(src)? {
            // Frame 为 Simple 或 Error
            b'+' | b'-' => {
                get_line(src)?;
                Ok(())
            },
            // Frame 为数字
            b':' => {
                get_decimal(src)?;
                Ok(())
            },
            // Frame 为 Null 或 Bulk
            b'$' => {
                // Frame::Null = b"$-1\r\n"
                if b'-' == peek_u8(src)? {
                    // 跳过 b"-1\r\n"
                    skip(src, 4)
                } else {
                    let len: usize = get_decimal(src)?.try_into()?;

                    // 跳过 bytes + b"\r\n"
                    skip(src, len + 2)
                }
            },
            //Frame 为数组
            b'*' => {
                // 数组中有多少元素
                let len = get_decimal(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            },
            // 非法数据
            actual => Err(format!("protocol error: invalid frame type {}", actual).into())
        }
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            },
            b'-' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            },
            b':' => {
                let num = get_decimal(src)?;

                Ok(Frame::Integer(num))
            },
            b'$' => {
                // Null
                if peek_u8(src)? == b'-' {
                    if get_line(src)? != b"-1" {
                        return Err("protocol error: invalid frmae format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    let len = get_decimal(src)?.try_into()?;

                    if src.remaining() < len + 2 {
                        return Err(Error::Incomplete)
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);
                    skip(src, len + 2)?;
                    Ok(Frame::Bulk(data))
                }
            },
            b'*' => {
                let len = get_decimal(src)?.try_into()?;
                let mut res = Vec::with_capacity(len);

                for _ in 0..len {
                    res.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(res))
            },
            _ => unimplemented!(),
        }
    }

    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame: {}", self).into()
    }
}

// 字符串引用比对，仅支持 `Simple`、`Bulk`或 `Error` 类型
impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) | Frame::Error(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Array(arr) => {
                for (i, item) in arr.iter().enumerate() {
                    // arr 的构成为 [元素个数，元素1，元素2, ..]
                    // 此处我们不想打印出元素个数
                    if i > 0 {
                        write!(fmt, " ")?;
                        item.fmt(fmt)?;
                    }
                }

                Ok(())
            }
        }
    }
}

// 读取第一个 byte 且将游标后移
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete)
    }

    Ok(src.get_u8())
}

// 读取一行，并设置游标
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // 获取起始位置
    let start = src.position() as usize;
    // 
    let end = src.get_ref().len() - 1;

    for i in start..end {
        // 若读取到 b"\r\n" 则将游标放置到下行首，并返回当前行数据（不包括 b"\r\n"）
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i])
        }
    }

    // 数据不足一行
    Err(Error::Incomplete)
}

// 读取一行转化 u64
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let line = get_line(src)?;
    atoi::<u64>(line).ok_or_else(|| "protocol error: invalid frame format.".into())
}

// 仅读取第一个 byte 但不移动游标
fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete)
    }

    Ok(src.chunk()[0])
}

// 将数据前移 n 个 byte，类似于删除，整体左移？
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete)
    }

    src.advance(n);
    Ok(())
}

/// 从字符串生成 Error 信息
impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error: invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error: invalid frame format".into()
    }
}

// 像 std::error::Error 一样使用 (crate)Error
impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
