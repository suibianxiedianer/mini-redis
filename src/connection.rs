#![cfg_attr(debug_assertions, allow(dead_code, unused_imports, unused_variables, unused_mut))]
use std::io::{self, Cursor};

use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

/// 通过此远程连接发送和接收 `Frame`
#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,

    // 读取 frames 的 buffer
    buffer: BytesMut,
}

impl Connection {
    /// 通过 socket 创建一个新连接
    /// buffer 大小为 4K
    pub fn new(socket: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024)
        }
    }

    /// 从当前连接中读取一条 `Frame`
    /// 这个函数会等待直到收到的数据足够解析出一条 `Frame`
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // 尝试从 buffer 中解析出 frame
            // 成功则返回 frame
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame))
            }

            // 读取不到数据时连接断开，若 buffer 不为空则异常
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None)
                } else {
                    return Err("Connection reset by peer".into())
                }
            }
        }
    }

    /// 从 self.buffer 中解析出 frame
    pub fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // 使用 Cursor 可以追踪当前数据读取的位置
        // `Cursor` 也实现了 `Buf`
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                // 检查通过则当前位置之前为一个 `Frame`
                let len = buf.position() as usize;
                buf.set_position(0);

                let frame = Frame::parse(&mut buf)?;
                // 前 len 个数据已经转换完成，
                // 将游标前移，清除了前 len 个数据
                self.buffer.advance(len);
                Ok(Some(frame))
            },
            // 没有足够的数据来解析 `Frame`，继续接收数据
            Err(Incomplete)  => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 将 frame 写入 stream
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            // Array(Vec<Frame>):
            // b'*' + bytes(len) + '\r\n' + bytes(frames)
            Frame::Array(arr) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(arr.len() as u64).await?;

                for entry in arr {
                    self.write_value(&entry).await?;
                }
            },
            _ => self.write_value(frame).await?,
        }

        self.stream.flush().await
    }

    pub async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            },
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            },
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            },
            Frame::Null => {
                self.stream.write_all(b"-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                self.stream.write_u8(b'$').await?;
                self.write_decimal(val.len() as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            },
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    pub async fn write_decimal(&mut self, value: u64) -> io::Result<()> {
        use std::io::Write;

        // 初始化一个，并将 value 写入，获得字节数
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", value)?;

        // 将 value 对应的字节写入 stream，并以 b"\r\n" 结束
        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
