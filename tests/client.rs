use std::net::SocketAddr;

use tokio::net::TcpListener;

use mini_redis::{client, server};

#[tokio::test]
async fn ping_without_message() {
    let addr = start_server().await;

    let mut client = client::connect(addr).await.unwrap();

    let response = client.ping(None).await.unwrap();

    assert_eq!(b"PONG", &response[..]);
}

#[tokio::test]
async fn ping_with_hello() {
    let addr = start_server().await;

    let mut client = client::connect(addr).await.unwrap();

    let response = client.ping(Some("你好".to_string())).await.unwrap();

    assert_eq!("你好".as_bytes(), &response[..]);
}

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move { server::run(listener, tokio::signal::ctrl_c()).await });

    addr
}