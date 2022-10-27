use clap::Parser;
use tokio::{
    net::TcpListener,
    signal,
};
use tracing_subscriber;

use mini_redis::{server, DEFAULT_PORT};

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    // 初始化默认日志收集？
    set_up_logging()?;
    println!("Hello Redis Server...");

    let cli =Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // 绑定一个 TCP 监听器
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

    // 接收 ctrl_c 作为关闭信号
    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[derive(Parser, Debug)]
#[clap(name = "mini-redis-server", version, author, about = "A Redis server")]
struct Cli {
    // 长命令格式：--port NUM
    #[clap(long)]
    port: Option<u16>,
}

fn set_up_logging() -> mini_redis::Result<()> {
    // https://docs.rs/tracing-subscriber/0.3.16/tracing_subscriber/fmt/index.html
    tracing_subscriber::fmt::try_init()
}
