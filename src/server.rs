use std::{
    future::Future,
    sync::Arc,
};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Semaphore},
    time::{self, Duration},
};
use tracing::{debug, error, info, instrument};

use crate::{Connection, Db, DbDropGuard, Shutdown, Command};

/// 服务监听器，运行在 Server 端，处理连接事项
#[derive(Debug)]
struct Listener {
    /// 共享的数据库
    db_holder: DbDropGuard,

    /// Tcp 监听器
    listener: TcpListener,

    /// 服务器最大连接数
    limit_connections: Arc<Semaphore>,

    /// 向所有存活的连接发送关闭信号，优雅地关闭服务
    /// 在执行 `run` 时初始化 `shutdown`
    notify_shutdown: broadcast::Sender<()>,

    /// 用作正常关闭时，确认客户端已断开连接
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// 每个连接的处理程序，从 `connection` 中读取请求并应用于 `db`
#[derive(Debug)]
struct Handler {
    /// 共享数据库
    db: Db,

    /// 用于处理连接消息，当 `Listener` 接受连接后，生成 `Connection`
    connection: Connection,

    /// 监听关闭通知
    shutdown: Shutdown,

    /// 当所有连接处理程序关闭后，且 `Listener` 亦关闭了发送端，
    /// 则 shutdown_complete_rx 会收到 `None`，服务端知道所有连接已关闭
    _shutdown_complete: mpsc::Sender<()>,
}

/// Redis 服务端接收的最大连接数
const MAX_CONNECTIONS: usize = 255;

/// 运行 mini-redis 服务
/// 接收 `TcpListener` 里的连接，并生成一个任务处理该连接
/// 服务将一直运行，直到 `shutdown` 完成，这意味着此时服务可被优雅地关闭
/// 可使用 `tokio::signal::ctrl_c()` 作为 `shutdown` 的参数，来接收 `SIGINT` 信号
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // 关闭服务时用到的广播发送端和确认连接关闭的 complete 隧道
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // 初始化 Listener
    let mut server = Listener {
        db_holder: DbDropGuard::new(),
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_rx,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run() => {
            // 若服务异常退出，这里抓一下日志
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        },
        _ = shutdown => {},
    }

    // 从 server 中得到关闭消息隧道
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    drop(notify_shutdown);

    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    /// 运行服务
    /// 监听入站连接，并为每个入站连接生成一个任务
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            // 当未达到最大连接数时，返回一个绑定到信号量的许可
            // 当 permit 被释放后，会自动被回收到信号量中
            // acquire_owned() 在信号量关闭时返回 `Err` ，但我们不执行关闭，所以可以安全的
            // `unwrap()`
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;

            let mut handler = Handler {
                db: self.db_holder.db(),
                connection: Connection::new(socket),
                // subscribe 返回接收端
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                // 当所有的 self.shutdown_complete_tx 端被丢弃后，接收端会得到通知
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection err");
                }

                // 恢复可用连接的数量
                drop(permit);
            });
        }
    }

    /// 接收一个入站连接
    /// 若成功则返回一个 `TcpStream` 流，失败后等待并重试
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into())
                    }
                },
            }

            time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

impl Handler {
    /// 处理单个连接
    /// 从套接字时读取 frames ，处理并写入返回消息
    /// 接收到关闭信号后直接退出
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // 服务没收到关闭信号时
        while !self.shutdown.is_shutdown() {
            let frame = tokio::select! {
                // 从连接中有可读消息
                res = self.connection.read_frame() => res?,
                // 接收到关闭信号，退出
                _ = self.shutdown.recv() => {
                    return Ok(())
                }
            };

            // 若 `read_frame()` 返回 `None`，表示连接断开
            let frame = match frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // 从 `frames` 里解析出命令
            let cmd = Command::from_frame(frame)?;
            debug!(?cmd);

            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown).await?;
        }

        Ok(())
    }
}
