use std::net::SocketAddr;

use tokio::net::TcpListener;

use mini_redis::{client, server};

/// ping 不附加消息，返回 `PONG`
#[tokio::test]
async fn ping_without_message() {
    let addr = start_server().await;

    let mut client = client::connect(addr).await.unwrap();

    let response = client.ping(None).await.unwrap();

    assert_eq!(b"PONG", &response[..]);
}

/// ping 时附加消息，返回相同的 `bytes`
#[tokio::test]
async fn ping_with_hello() {
    let addr = start_server().await;

    let mut client = client::connect(addr).await.unwrap();

    let response = client.ping(Some("你好".to_string())).await.unwrap();

    assert_eq!("你好".as_bytes(), &response[..]);
}

/// 设置、查询键值
#[tokio::test]
async fn key_value_set_get() {
    let addr = start_server().await;

    let mut client = client::connect(addr).await.unwrap();

    client.set("hello", "world".into()).await.unwrap();

    let value = client.get("hello").await.unwrap().unwrap();

    assert_eq!(b"world", &value[..]);
}

/// 订阅单个频道并接收消息
#[tokio::test]
async fn recieve_message_from_subscribe_channel() {
    let addr = start_server().await;

    let client = client::connect(addr.clone()).await.unwrap();
    let mut subscriber = client.subscribe(vec!["hello".into()]).await.unwrap();

    // 发送消息
    tokio::spawn(async move {
        let mut client = client::connect(addr).await.unwrap();
        client.publish("hello", "world".into()).await.unwrap();
    });

    let message = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("hello", &message.channel);
    assert_eq!(b"world", &message.content[..]);
}

/// 订阅多个频道并接收消息
#[tokio::test]
async fn recieve_message_from_subscribe_channels() {
    let addr = start_server().await;

    let client = client::connect(addr.clone()).await.unwrap();
    let mut subscriber = client.subscribe(vec!["hello".into(), "foo".into()]).await.unwrap();

    // 发送消息至 `hello`、`foo`
    tokio::spawn(async move {
        let mut client = client::connect(addr).await.unwrap();
        client.publish("hello", "world".into()).await.unwrap();
        client.publish("foo", "bar".into()).await.unwrap();
    });

    let message = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("hello", &message.channel);
    assert_eq!(b"world", &message.content[..]);


    let message = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("foo", &message.channel);
    assert_eq!(b"bar", &message.content[..]);
}

/// 取消订阅
#[tokio::test]
async fn unsubscribe_from_channels() {
    let addr = start_server().await;

    let client = client::connect(addr.clone()).await.unwrap();
    let mut subscriber = client.subscribe(vec!["hello".into(), "foo".into()]).await.unwrap();

    subscriber.unsubscribe(&[]).await.unwrap();
    assert_eq!(0, subscriber.get_subscribed().len());
}

/// 启动服务
async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move { server::run(listener, tokio::signal::ctrl_c()).await });

    addr
}
