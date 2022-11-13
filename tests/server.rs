use std::net::SocketAddr;

use mini_redis::server;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{self, Duration},
};

/// 基本的无生命周期的键值设置与查询操作
#[tokio::test]
async fn key_value_get_set() {
    let addr = start_server().await;

    // 与服务建立连接
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 查找一个不存在的键值
    stream.write_all(b"*2\r\n\
                     $3\r\nGET\r\n\
                     $5\r\nhello\r\n")
        .await
        .unwrap();

    get_null(&mut stream).await;

    // 设置一个键值
    stream.write_all(b"*3\r\n\
                     $3\r\nSET\r\n\
                     $5\r\nhello\r\n\
                     $5\r\nworld\r\n")
        .await
        .unwrap();

    get_ok(&mut stream).await;

    // 查找一个键对应的值
    stream.write_all(b"*2\r\n\
                     $3\r\nGET\r\n\
                     $5\r\nhello\r\n")
        .await
        .unwrap();

    get_world(&mut stream).await;
}

/// 有生命周期的键值测试
#[tokio::test]
async fn key_value_timeout() {
    // pause 后可使用 advance
    time::pause();
    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 设置一个键值，超时时间为 1 秒
    stream.write_all(b"*5\r\n\
                     $3\r\nSET\r\n\
                     $5\r\nhello\r\n\
                     $5\r\nworld\r\n\
                     +EX\r\n\
                     :1\r\n")
        .await
        .unwrap();

    get_ok(&mut stream).await;

    // 可以查找到键值
    stream.write_all(b"*2\r\n\
                     $3\r\nGET\r\n\
                     $5\r\nhello\r\n")
        .await
        .unwrap();

    get_world(&mut stream).await;

    // 数据过期
    time::advance(Duration::from_secs(1)).await;
    stream.write_all(b"*2\r\n\
                     $3\r\nGET\r\n\
                     $5\r\nhello\r\n")
        .await
        .unwrap();

    get_null(&mut stream).await;
}

/// 简单的发布/订阅测试
#[tokio::test]
async fn pub_sub() {
    let addr = start_server().await;

    let mut publisher = TcpStream::connect(addr).await.unwrap();

    // 发布消息至 hello，0 个订阅者
    publisher.write_all(b"*3\r\n\
                     $7\r\nPUBLISH\r\n\
                     $5\r\nhello\r\n\
                     $5\r\nworld\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    publisher.read_exact(&mut response).await.unwrap();
    assert_eq!(b":0\r\n", &response);

    // 第一个订阅者，订阅 `hello` 频道
    let mut sub1 = TcpStream::connect(addr).await.unwrap();
    sub1.write_all(b"*2\r\n\
                     $9\r\nSUBSCRIBE\r\n\
                     $5\r\nhello\r\n")
        .await
        .unwrap();

    let mut response = [0; 34];
    sub1.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $9\r\nsubscribe\r\n\
               $5\r\nhello\r\n\
               :1\r\n",
               &response);

    // 发布消息至 hello，1 个订阅者
    publisher.write_all(b"*3\r\n\
                     $7\r\nPUBLISH\r\n\
                     $5\r\nhello\r\n\
                     $5\r\nworld\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    publisher.read_exact(&mut response).await.unwrap();
    assert_eq!(b":1\r\n", &response);

    // 第一个订阅者收到来自 `hello` 频道的消息
    let mut response = [0; 39];
    sub1.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $7\r\nmessage\r\n\
               $5\r\nhello\r\n\
               $5\r\nworld\r\n",
               &response);

    // 第二个订阅者，订阅 `hello` 和 `foo` 频道
    let mut sub2 = TcpStream::connect(addr).await.unwrap();
    sub2.write_all(b"*3\r\n\
                   $9\r\nSUBSCRIBE\r\n\
                   $5\r\nhello\r\n\
                   $3\r\nfoo\r\n")
        .await
        .unwrap();

    let mut response = [0; 34];
    sub2.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $9\r\nsubscribe\r\n\
               $5\r\nhello\r\n\
               :1\r\n",
               &response);

    let mut response = [0; 32];
    sub2.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $9\r\nsubscribe\r\n\
               $3\r\nfoo\r\n\
               :2\r\n",
               &response);

    // 向 `hello` 频道发送一条消息
    publisher.write_all(b"*3\r\n\
                        $7\r\nPUBLISH\r\n\
                        $5\r\nhello\r\n\
                        $5\r\nJerry\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    publisher.read_exact(&mut response).await.unwrap();
    assert_eq!(b":2\r\n", &response);

    // 向 `foo` 频道发送一条消息
    publisher.write_all(b"*3\r\n\
                        $7\r\nPUBLISH\r\n\
                        $3\r\nfoo\r\n\
                        $3\r\nbar\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    publisher.read_exact(&mut response).await.unwrap();
    assert_eq!(b":1\r\n", &response);

    // 订阅者一、二接收 `hello` 频道的消息
    let mut response = [0; 39];
    sub1.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $7\r\nmessage\r\n\
               $5\r\nhello\r\n\
               $5\r\nJerry\r\n",
               &response);

    sub2.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $7\r\nmessage\r\n\
               $5\r\nhello\r\n\
               $5\r\nJerry\r\n",
               &response);

    // 订阅者一、二接收 `foo` 频道的消息
    let mut response = [0; 35];
    time::timeout(Duration::from_millis(100), sub1.read(&mut response))
        .await
        .unwrap_err();

    sub2.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $7\r\nmessage\r\n\
               $3\r\nfoo\r\n\
               $3\r\nbar\r\n",
               &response);
}

/// 订阅管理测试
#[tokio::test]
async fn manage_subscription() {
    let addr = start_server().await;

    let mut publisher = TcpStream::connect(addr).await.unwrap();

    // sub 订阅 `hello`
    let mut sub = TcpStream::connect(addr).await.unwrap();
    sub.write_all(b"*2\r\n\
                     $9\r\nSUBSCRIBE\r\n\
                     $5\r\nhello\r\n")
        .await
        .unwrap();

    let mut response = [0; 34];
    sub.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $9\r\nsubscribe\r\n\
               $5\r\nhello\r\n\
               :1\r\n",
               &response);

    // sub 订阅 `foo`
    sub.write_all(b"*2\r\n\
                     $9\r\nSUBSCRIBE\r\n\
                     $3\r\nfoo\r\n")
        .await
        .unwrap();

    let mut response = [0; 32];
    sub.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $9\r\nsubscribe\r\n\
               $3\r\nfoo\r\n\
               :2\r\n",
               &response);

    // sub 取消 `hello` 订阅
    sub.write_all(b"*2\r\n\
                     $11\r\nUNSUBSCRIBE\r\n\
                     $5\r\nhello\r\n")
        .await
        .unwrap();

    let mut response = [0; 37];
    sub.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $11\r\nunsubscribe\r\n\
               $5\r\nhello\r\n\
               :1\r\n",
               &response);

    // publisher 向 `hello`、`foo` 发送消息
    publisher.write_all(b"*3\r\n\
                        $7\r\nPUBLISH\r\n\
                        $5\r\nhello\r\n\
                        $5\r\nworld\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    publisher.read_exact(&mut response).await.unwrap();
    assert_eq!(b":0\r\n", &response);

    publisher.write_all(b"*3\r\n\
                        $7\r\nPUBLISH\r\n\
                        $3\r\nfoo\r\n\
                        $3\r\nbar\r\n")
        .await
        .unwrap();

    publisher.read_exact(&mut response).await.unwrap();
    assert_eq!(b":1\r\n", &response);

    // sub 只能读取 `foo` 的消息
    let mut response = [0; 35];
    sub.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $7\r\nmessage\r\n\
               $3\r\nfoo\r\n\
               $3\r\nbar\r\n",
               &response);

    time::timeout(Duration::from_millis(100), sub.read(&mut response))
        .await
        .unwrap_err();

    // 取消所有订阅
    sub.write_all(b"*1\r\n$11\r\nUNSUBSCRIBE\r\n")
        .await
        .unwrap();

    let mut response = [0; 35];
    sub.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $11\r\nunsubscribe\r\n\
               $3\r\nfoo\r\n\
               :0\r\n",
               &response);
}

/// 错误命令格式测试
#[tokio::test]
async fn send_error_unknown_command() {
    let addr = start_server().await;
//    let addr = "127.0.0.1:6379";

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream.write_all(b"*2\r\n\
                     $3\r\nFOO\r\n\
                     $5\r\nhello\r\n")
        .await
        .unwrap();

    let mut response = [0; 29];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"-Err: unknown command \'foo\'\r\n", &response);
}

/// 订阅模式只接收订阅相关命令
#[tokio::test]
async fn send_error_get_set_after_subscribe() {
    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 进入订阅模式
    stream.write_all(b"*2\r\n\
                     $9\r\nSUBSCRIBE\r\n\
                     $5\r\nhello\r\n")
        .await
        .unwrap();

    let mut response = [0; 34];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"*3\r\n\
               $9\r\nsubscribe\r\n\
               $5\r\nhello\r\n\
               :1\r\n",
               &response);

    stream.write_all(b"*3\r\n\
                     $3\r\nSET\r\n\
                     $5\r\nhello\r\n\
                     $5\r\nworld\r\n")
        .await
        .unwrap();

    let mut response = [0; 29];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"-Err: unknown command \'set\'\r\n", &response);

    stream.write_all(b"*2\r\n\
                     $3\r\nGET\r\n\
                     $5\r\nhello\r\n")
        .await
        .unwrap();

    let mut response = [0; 29];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"-Err: unknown command \'get\'\r\n", &response);
}

async fn get_ok(stream: &mut TcpStream) {
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"+OK\r\n", &response);
}

async fn get_null(stream: &mut TcpStream) {
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$-1\r\n", &response);
}

async fn get_world(stream: &mut TcpStream) {
    let mut response = [0; 11];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$5\r\nworld\r\n", &response);
}

/// 启动 mini_redis 服务
async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move { server::run(listener, tokio::signal::ctrl_c()).await });

    addr
}
