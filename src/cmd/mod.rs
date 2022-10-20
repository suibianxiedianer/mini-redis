#![cfg_attr(debug_assertions, allow(dead_code, unused_imports, unused_variables, unused_mut))]
/// Redis 对应的命令
/// 操作数据库键值的 Get/Set
/// 消息频道订阅的 Publish/Subscribe/Unsubscribe
mod get;
pub use get::Get;

mod set;
pub use set::Set;

mod publish;
pub use publish::Publish;

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    Publish(Publish),
}
