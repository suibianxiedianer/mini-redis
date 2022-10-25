use clap::Parser;
use tokio::{
    net::TcpListener,
    signal,
};

use mini_redis::{server, DEFAULT_PORT};

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
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
