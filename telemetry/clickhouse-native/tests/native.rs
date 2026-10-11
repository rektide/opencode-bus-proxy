//! Disposable local-server integration, opt in with nextest --run-ignored only.
use arrow_array::{RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use bus_proxy_clickhouse_native::{BatchClient, Config, Table};
use std::{
    fs,
    net::TcpListener,
    process::{Child, Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

struct Server {
    child: Child,
    root: tempfile::TempDir,
    port: u16,
}
impl Server {
    fn start() -> Self {
        let scratch = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.test-agent/clickhouse-native");
        fs::create_dir_all(&scratch).unwrap();
        let root = tempfile::tempdir_in(scratch).unwrap();
        let reserve = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reserve.local_addr().unwrap().port();
        drop(reserve);
        let path = root.path().display();
        fs::write(root.path().join("config.xml"), format!(r#"<clickhouse>
          <logger><level>warning</level><log>{path}/server.log</log><errorlog>{path}/error.log</errorlog></logger>
          <listen_host>127.0.0.1</listen_host><tcp_port>{port}</tcp_port><path>{path}/data/</path>
          <tmp_path>{path}/tmp/</tmp_path><user_directories><users_xml><path>{path}/users.xml</path></users_xml></user_directories>
          <mark_cache_size>10485760</mark_cache_size><max_thread_pool_size>128</max_thread_pool_size>
          <background_pool_size>2</background_pool_size><background_schedule_pool_size>2</background_schedule_pool_size>
          <background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size>
          <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
          <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
          <merge_tree><number_of_free_entries_in_pool_to_execute_mutation>1</number_of_free_entries_in_pool_to_execute_mutation>
          <number_of_free_entries_in_pool_to_lower_max_size_of_merge>1</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
          <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>1</number_of_free_entries_in_pool_to_execute_optimize_entire_partition></merge_tree>
        </clickhouse>"#)).unwrap();
        fs::write(root.path().join("users.xml"), r#"<clickhouse><profiles><default><max_threads>2</max_threads></default></profiles>
          <users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota></default></users>
          <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas></clickhouse>"#).unwrap();
        let child = Command::new("clickhouse")
            .args(["server", "--config-file"])
            .arg(root.path().join("config.xml"))
            .current_dir(root.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut server = Self { child, root, port };
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let output = server.command("SELECT 1").output().unwrap();
            if output.status.success() {
                break;
            }
            assert!(
                server.child.try_wait().unwrap().is_none(),
                "server exited: {}",
                fs::read_to_string(server.root.path().join("error.log")).unwrap_or_default()
            );
            assert!(Instant::now() < deadline, "server readiness timed out");
            thread::sleep(Duration::from_millis(50));
        }
        server
    }
    fn command(&self, query: &str) -> Command {
        let mut command = Command::new("clickhouse");
        command.args([
            "client",
            "--host",
            "127.0.0.1",
            "--port",
            &self.port.to_string(),
            "--receive_timeout",
            "2",
            "--connect_timeout",
            "1",
            "--multiquery",
            "--query",
            query,
        ]);
        command
    }
    fn query(&self, query: &str) -> String {
        let output = self.command(query).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }
    fn config(&self, table: &str) -> Config {
        Config {
            host: "127.0.0.1".into(),
            port: self.port,
            tls: false,
            tls_server_name: None,
            username: "default".into(),
            password: String::new(),
            table: Table::new("default", table).unwrap(),
            operation_timeout: Duration::from_secs(3),
            max_batch_rows: 1024,
            max_batch_bytes: 16 * 1024 * 1024,
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
#[ignore = "requires installed clickhouse server/client; starts an isolated disposable instance"]
fn native_insert_roundtrip_and_post_send_failure() {
    let _ = tracing_subscriber::fmt().with_env_filter("clickhouse_arrow=debug").with_test_writer().try_init();
    let server = Server::start();
    server.query("CREATE TABLE raw_test (value String, count UInt64, server_default String DEFAULT 'server', CONSTRAINT valid CHECK value != 'bad') ENGINE=MergeTree ORDER BY tuple()");
    let client = BatchClient::new(server.config("raw_test")).unwrap();
    let batch = |value: &str| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Utf8, false),
                Field::new("count", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![value])),
                Arc::new(UInt64Array::from(vec![u64::MAX])),
            ],
        )
        .unwrap()
    };
    futures_executor::block_on(client.insert(batch("雪"))).unwrap();
    assert_eq!(
        server.query("SELECT value,count,server_default FROM raw_test FORMAT JSONEachRow"),
        "{\"value\":\"雪\",\"count\":18446744073709551615,\"server_default\":\"server\"}\n"
    );
    // Constraint rejection happens after the data send, in the drained response.
    assert!(futures_executor::block_on(client.insert(batch("bad"))).is_err());
    assert_eq!(server.query("SELECT count() FROM raw_test"), "1\n");
    // Interrupted connections are not reused; next batch establishes a new one.
    futures_executor::block_on(client.insert(batch("recovered"))).unwrap();
    client.shutdown().unwrap();
    assert_eq!(server.query("SELECT count() FROM raw_test"), "2\n");
    let mut wrong = server.config("does_not_exist");
    wrong.password = "credential-that-must-not-be-in-errors".into();
    let client = BatchClient::new(wrong).unwrap();
    let error = futures_executor::block_on(client.insert(batch("test"))).unwrap_err();
    assert!(!error.to_string().contains("credential-that"));
    client.shutdown().unwrap();
    eprintln!("Verified {}", server.query("SELECT version()").trim());
}
