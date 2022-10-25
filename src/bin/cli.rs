use std::{
    num::ParseIntError,
    str,
    time::Duration,
};

use bytes::Bytes;
use clap::{Parser, Subcommand};

use mini_redis::{client, DEFAULT_PORT};

#[tokio::main(flavor = "current_thread")]
async fn main() -> mini_redis::Result<()> {
    println!("mini redis client start...");

    // 解析命令行参数
    let cli = Cli::parse();

    // 确定远程服务地址
    let addr = format!("{}:{}", cli.host, cli.port);

    // 使用 `client` 建立连接
    let mut client = client::connect(&addr).await?;

    // 处理请求的命令
    match cli.command {
        Command::Ping { msg } => {
            let value = client.ping(msg).await?;
            if let Ok(string) = str::from_utf8(&value) {
                println!("\"{}\"", string);
            } else {
                println!("{:?}", value);
            }
        },
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        },
        Command::Set { key, value, expires: None } => {
            client.set(&key, value).await?;
            println!("OK");
        },
        Command::Set { key, value, expires: Some(expires) } => {
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        },
        Command::Publish { channel, message } => {
            client.publish(&channel, message.into()).await?;
            println!("Publish OK");
        },
        // 进入订阅者客户端模式
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err("channel(s) must be provided".into())
            }

            let mut subscriber = client.subscribe(channels).await?;

            while let Some(msg) = subscriber.next_message().await? {
                println!("Got message from channel: {}, message: {:?}", msg.channel, msg.content);
            }
        },
    }

    Ok(())
}

// 还是这种 clap 格式看着方便
#[derive(Parser, Debug)]
#[clap(
    name = "mini-redis-cli",
    version,
    author,
    about = "Issue Redis commands"
)]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[clap(name = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[clap(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        /// ping 命令携带的数据
        msg: Option<String>,
    },
    Get {
        key: String,
    },
    Set {
        key: String,
        #[clap(parse(from_str = bytes_from_str))]
        value: Bytes,
        #[clap(parse(try_from_str = duration_from_ms_str))]
        expires: Option<Duration>,
    },
    Publish {
        channel: String,
        #[clap(parse(from_str = bytes_from_str))]
        message: Bytes,
    },
    Subscribe {
        channels: Vec<String>,
    }
}

// 从 str 生成 Bytes
fn bytes_from_str(src: &str) -> Bytes {
    Bytes::from(src.to_string())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}
