#![cfg(not(feature = "otlp"))]

use std::process::Command;

#[test]
fn requested_or_inferred_otlp_fails_before_listening_without_backend() {
    for explicit in [true, false] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_opencode-bus-proxy"));
        for (key, _) in std::env::vars_os() {
            let name = key.to_string_lossy();
            if name.starts_with("OTEL_") || name.starts_with("LISTEN_") {
                command.env_remove(key);
            }
        }
        command.args(["--listen", "127.0.0.1:0", "--allow-unauthenticated"]);
        if explicit {
            command.env("OTEL_TRACES_EXPORTER", "otlp");
        } else {
            command.env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318");
        }
        let output = command.output().unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("--features otlp"), "{stderr}");
    }
}
