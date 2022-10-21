pub mod frame;
pub use frame::Frame;

pub mod connection;
pub use connection::Connection;

mod parse;
use parse::{Parse, ParseError};

mod db;
use db::{Db, DbDropGuard};

mod shutdown;
use shutdown::Shutdown;

pub mod cmd;
pub use cmd::Command;

/// 定义 crate::Error
/// 大部分函数返回的错误
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// 定义 crate::Result
pub type Result<T> = std::result::Result<T, Error>;
