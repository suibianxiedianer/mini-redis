#![cfg_attr(debug_assertions, allow(dead_code, unused_imports, unused_variables, unused_mut))]
use std::io::{self, Cursor};

use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
}

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
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame))
            }
        }
    }

    pub fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // 使用 Cursor 可以追踪当前数据读取的位置
        // `Cursor` 也实现了 `Buf`
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf)? {
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

    pub async fn write_frame(&mut self, frame: Frame) -> io::Result<()> {}

    pub async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {}

    pub async fn write_decimal(&mut self, value: u64) -> io::Result<()> {}
}
