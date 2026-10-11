//! Shared disposable-server fixture for exporter and daemon maintainer tests.
use std::{
    fs,
    net::TcpListener,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
pub struct Server {
    child: Child,
    root: tempfile::TempDir,
    pub port: u16,
    binary: std::ffi::OsString,
}
impl Server {
    pub fn start() -> Self {
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
        let binary = std::env::var_os("BUS_PROXY_CLICKHOUSE_TEST_BINARY")
            .unwrap_or_else(|| "clickhouse".into());
        let child = Command::new(&binary)
            .args(["server", "--config-file"])
            .arg(root.path().join("config.xml"))
            .current_dir(root.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut server = Self {
            child,
            root,
            port,
            binary,
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if server
                .command("SELECT 1")
                .output()
                .unwrap()
                .status
                .success()
            {
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
        let mut command = Command::new(&self.binary);
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
    pub fn query(&self, query: &str) -> String {
        let output = self.command(query).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
