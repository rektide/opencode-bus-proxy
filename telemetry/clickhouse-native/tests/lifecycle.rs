use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bus_proxy_clickhouse_native::{BatchClient, Config, Error, Table};
use std::{
    io::Read,
    net::TcpListener,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["hello"]))],
    )
    .unwrap()
}
fn config(port: u16) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port,
        tls: false,
        tls_server_name: None,
        username: "default".into(),
        password: String::new(),
        table: Table::new("default", "test").unwrap(),
        operation_timeout: Duration::from_millis(100),
        max_batch_rows: 10,
        max_batch_bytes: 1024,
    }
}
fn stalled_server(listener: TcpListener) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut buf = [0; 512];
        assert!(socket.read(&mut buf).unwrap() > 0, "native hello");
        // Do not send a server hello. Cleanup must close the connection.
        while socket
            .read(&mut buf)
            .expect("cancelled operation kept socket open")
            > 0
        {}
    })
}

#[test]
fn no_caller_tokio_reactor_is_needed_and_handshake_timeout_closes_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = BatchClient::new(config(listener.local_addr().unwrap().port())).unwrap();
    let server = stalled_server(listener);
    let start = Instant::now();
    assert!(matches!(
        futures_executor::block_on(client.insert(batch())),
        Err(Error::Timeout)
    ));
    client.shutdown().unwrap();
    assert!(start.elapsed() < Duration::from_secs(2));
    server.join().unwrap();
}

#[test]
fn unavailable_server_is_bounded_and_shutdown_is_idempotent() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let client = BatchClient::new(config(port)).unwrap();
    assert!(matches!(
        futures_executor::block_on(client.insert(batch())),
        Err(Error::Connect)
    ));
    client.shutdown().unwrap();
    client.shutdown().unwrap();
    assert!(matches!(
        futures_executor::block_on(client.insert(batch())),
        Err(Error::Closed)
    ));
}

#[test]
fn cancellation_does_not_leave_native_work_running() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = BatchClient::new(config(listener.local_addr().unwrap().port())).unwrap();
    let server = stalled_server(listener);
    runtime.block_on(async {
        assert!(
            tokio::time::timeout(Duration::from_millis(30), client.insert(batch()))
                .await
                .is_err()
        );
        // Caller timer is driven despite the native worker's stalled handshake.
    });
    client.shutdown().unwrap();
    server.join().unwrap();
}

#[test]
fn raw_submissions_are_bounded_and_shutdown_cancels_pending_work() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut cfg = config(listener.local_addr().unwrap().port());
    cfg.operation_timeout = Duration::from_secs(2);
    let client = BatchClient::new(cfg).unwrap();
    let (opened, opening) = tokio::sync::oneshot::channel();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut buf = [0; 512];
        assert!(socket.read(&mut buf).unwrap() > 0);
        opened.send(()).unwrap();
        while socket.read(&mut buf).unwrap() > 0 {}
    });
    runtime.block_on(async {
        let first_client = client.clone();
        let first = tokio::spawn(async move { first_client.insert(batch()).await });
        opening.await.unwrap();
        let second_client = client.clone();
        let second = tokio::spawn(async move { second_client.insert(batch()).await });
        tokio::task::yield_now().await;
        assert!(matches!(client.insert(batch()).await, Err(Error::Busy)));
        first.abort();
        second.abort();
        let _ = first.await;
        let _ = second.await;
    });
    client.shutdown().unwrap();
    server.join().unwrap();
}
