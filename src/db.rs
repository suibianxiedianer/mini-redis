#![cfg_attr(debug_assertions, allow(dead_code, unused_imports, unused_variables, unused_mut))]
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tokio::{
    sync::{broadcast, Notify},
    time::{self, Duration, Instant},
};
use tracing::debug;

/// 不太明白 DbDropGuard 干什么用的
/// TODO
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    db: Db,
}

/// Db 拥有 `Arc` Shared，在所有连接之间共享
/// TODO
#[derive(Debug, Clone)]
pub(crate) struct Db {
    shared: Arc<Shared>,
}

/// Shared
/// state 加锁，读写数据
/// background_task 用来做什么
/// TODO
#[derive(Debug)]
struct Shared {
    /// 共享的 state 被 mutex 保护，
    /// 因其内部操作都是同步的故使用 `std::sync::Mutex` 而非 `Tokio` mutex
    state: Mutex<State>,
    /// TODO
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// KV 数据
    entries: HashMap<String, Entry>,
    /// 广播、订阅的频道
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
    expirations: BTreeMap<(Instant, u64), String>,
    next_id: u64,
    shutdown: bool,
}

/// 键值存储中的条目
#[derive(Debug)]
struct Entry {
    /// 唯一标识 ID
    id: u64,
    /// 存储的数据
    data: Bytes,
    /// 有效期，超过后将从数据库中删除
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    /// 创建一个包括 `Db` 的 `DbHolder`
    /// 当他被删除时，`Db` 清除任务将被关闭？？
    pub(crate) fn new() -> Self {
        DbDropGuard { db: Db::new() }
    }

    /// 获取共享数据库，因为这是一个 `Arc`，所以直接 clone 即可
    pub(crate) fn db(self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // 通知 `Db` 任务已关闭，清除失效的 keys
        self.db.shutdown_purge_task();
    }
}

impl Db {
    pub(crate) fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // 启动后台任务
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// 通过键查找值
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 首先得到锁，然后查找、克隆值
        // 因为 data 使用 `Bytes` 存储，所以 clone 只是浅拷贝
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// 通过键存储值
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();
        // 获取自增 id
        let id = state.next_id;
        state.next_id += 1;

        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // 失效时间
            let when = Instant::now() + duration;

            // 若当前最早失效时间晚于当前键的有效期
            // 则需通知后台使其更新状态
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            state.expirations.insert((when, id), key.clone());
            when
        });

        // 将新条目添加到 `HashMap` 中，并得到旧的条目
        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: value,
                expires_at,
            }
        );

        // 若替换了旧的 `Entry`，则需将其从有效期清理列表中去除
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, prev.id));
            }
        }

        drop(state);

        if notify {
            self.shared.background_task.notify_one();
        }
    }

    /// 请求订阅一个频道，返回一个 `Reciever` 来接收此频道发送的广播
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // 先获取锁
        let mut state = self.shared.state.lock().unwrap();

        match state.pub_sub.entry(key) {
            // 已有对应的频道
            Entry::Occupied(e) => e.get().subscribe(),
            // 当前无此频道，创建一个并加入
            Entry::Vacant(e) => {
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            },
        }
    }

    /// 向广播中发送数据，并返回此频道的订阅者的数量
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // 发送失败或无此频道则返回 0
            .map(|tx| tx.send(value).unwrap_or(0))
            .unwrap_or(0)
    }

    /// TODO
    fn shutdown_purge_task(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        drop(state);
        self.shared.background_task.notify_one();
    }
}

/// TODO
impl Shared {
    /// 清除所有的已过期的键，并返回最近的将过期的时间
    /// 后台任务将休眠到过期时间再执行清理任务
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            return None;
        }

        let state = &mut *state;

        let now = Instant::now();

        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when)
            }

            // 清理已过期的键
            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }

        None
    }

    /// 当数据库关闭时，返回 `true`
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    /// 下一个临近键的过期时间
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .keys()
            .next()
            .map(|expiration| expiration.0)
    }
}

async fn purge_expired_tasks(shared: Arc<Shared>) {
    // 如果设置了关闭标识，则退出后台任务
    while !shared.is_shutdown() {
        if let Some(when) = shared.purge_expired_keys() {
            tokio::select! {
                _ = time::sleep_until(when) => {},
                // 当在等待时得到通知，则更新最早生效的键的时间
                _ = shared.background_task.notified() => {},
            }
            todo!()
        } else {
            // 没有将生效的键，等待通知
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shutdown")
}
