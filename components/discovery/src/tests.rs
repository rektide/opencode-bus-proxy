use super::*;
use http::{Response, StatusCode};
use http_body_util::Full;
use hyper::{body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use std::{convert::Infallible, os::unix::fs::PermissionsExt, sync::Arc};
use tokio::{net::TcpListener, sync::RwLock, task::JoinHandle};

struct Fixture {
    _directory: tempfile::TempDir,
    registration: Registration,
    info: Arc<RwLock<serde_json::Value>>,
    health: Arc<RwLock<StatusCode>>,
    requests: Arc<RwLock<Vec<String>>>,
    server: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("bus-proxy-registration-")
            .tempdir_in("/home/rektide/tmp-opencode")
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let info = Arc::new(RwLock::new(
            serde_json::json!({"pid":123,"version":"test-v1"}),
        ));
        let health = Arc::new(RwLock::new(StatusCode::OK));
        let requests = Arc::new(RwLock::new(Vec::new()));
        let (server_info, server_health, server_requests) =
            (info.clone(), health.clone(), requests.clone());
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let (info, health, requests) = (
                    server_info.clone(),
                    server_health.clone(),
                    server_requests.clone(),
                );
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let (info, health, requests) =
                            (info.clone(), health.clone(), requests.clone());
                        async move {
                            let path = request.uri().path();
                            requests.write().await.push(path.to_owned());
                            let authenticated = request
                                .headers()
                                .get(header::AUTHORIZATION)
                                .is_some_and(|value| value == "Basic b3BlbmNvZGU6dGVzdC1zZWNyZXQ=");
                            let (status, body) = if !authenticated {
                                (StatusCode::UNAUTHORIZED, "{}".to_owned())
                            } else if path == "/api/info" {
                                (StatusCode::OK, info.read().await.to_string())
                            } else {
                                (*health.read().await, "{}".to_owned())
                            };
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(status)
                                    .body(Full::new(Bytes::from(body)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(socket), service)
                        .await;
                });
            }
        });
        let registration = Registration::new(
            directory.path().join("service-local.json"),
            &Origin {
                upstream: "http://127.0.0.1:49373",
                username: "opencode",
                password: Some("test-secret"),
                request_timeout: Duration::from_secs(1),
            },
            None,
            address,
            Mode::Stomp,
            true,
        )
        .unwrap();
        Self {
            _directory: directory,
            registration,
            info,
            health,
            requests,
            server,
        }
    }

    fn origin(&self) -> serde_json::Value {
        serde_json::json!({
            "mode":"server", "pid":123, "version":"test-v1", "id":"origin-1",
            "password":"test-secret", "url":"http://0.0.0.0:49373",
            "futureField":{"keep":true}
        })
    }

    fn write(&self, record: &serde_json::Value) -> Vec<u8> {
        let bytes = serde_json::to_vec(record).unwrap();
        std::fs::write(&self.registration.file, &bytes).unwrap();
        bytes
    }

    fn read(&self) -> serde_json::Value {
        serde_json::from_slice(&read_record(&self.registration.file).unwrap()).unwrap()
    }
}

#[test]
fn listener_addresses_use_actual_port_and_loopback_for_wildcards() {
    for (bound, expected) in [
        ("0.0.0.0:32123", "http://127.0.0.1:32123"),
        ("[::]:32124", "http://[::1]:32124"),
        ("127.0.0.2:32125", "http://127.0.0.2:32125"),
    ] {
        assert_eq!(listener_url(bound.parse().unwrap()), expected);
    }
    assert!(same_origin(
        &origin_url("http://0.0.0.0:123").unwrap(),
        &origin_url("http://localhost:123/").unwrap()
    ));
    assert!(!same_origin(
        &origin_url("http://127.0.0.1:123").unwrap(),
        &origin_url("http://127.0.0.1:124").unwrap()
    ));
    assert!(!same_origin(
        &origin_url("http://192.0.2.1:123").unwrap(),
        &origin_url("http://127.0.0.1:123").unwrap()
    ));
    assert!(!same_origin(
        &origin_url("http://[::1]:123").unwrap(),
        &origin_url("http://127.0.0.1:123").unwrap()
    ));
    for value in [
        "http://u:p@localhost:123",
        "http://localhost:123/path",
        "http://localhost:123/?q=1",
        "https://localhost:123",
        "garbage",
    ] {
        assert!(origin_url(value).is_err());
    }
}

#[tokio::test]
async fn publication_preserves_origin_identity_unknown_fields_and_private_permissions() {
    let mut fixture = Fixture::new().await;
    let original = fixture.origin();
    fixture.write(&original);
    fixture.registration.reconcile().await.unwrap();
    let published = fixture.read();
    for field in ["mode", "pid", "version", "id", "password", "futureField"] {
        assert_eq!(published[field], original[field], "{field}");
    }
    assert_ne!(published["url"], original["url"]);
    assert_eq!(published["_busProxy"]["pid"], std::process::id());
    assert_eq!(published["_busProxy"]["originUrl"], original["url"]);
    assert_eq!(
        std::fs::metadata(&fixture.registration.file)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let bytes = read_record(&fixture.registration.file).unwrap();
    fixture.registration.reconcile().await.unwrap();
    assert_eq!(read_record(&fixture.registration.file).unwrap(), bytes);
    assert_eq!(
        fixture.requests.read().await.as_slice(),
        ["/api/info", "/_bus/health"]
    );
    fixture.registration.restore().unwrap();
    assert_eq!(fixture.read(), original);
}

#[tokio::test]
async fn cooperate_only_supplements_and_preserves_native_override_policy() {
    let mut fixture = Fixture::new().await;
    fixture.registration.mode = Mode::Cooperate;
    for contact in ["service", "proxy", "override"] {
        let mut original = fixture.origin();
        let advertised = fixture
            .registration
            .advertised
            .as_str()
            .trim_end_matches('/');
        original["discovery"] = serde_json::json!({
            "version":1, "source":contact, "future":"keep", "endpoints":{
                "service":{"url":original["url"],"pid":123},
                "proxy":{"url":advertised,"description":"preserve"},
                "tunnel":{"url":"https://future.test"}
            }
        });
        if contact == "proxy" {
            original["url"] = advertised.into();
        }
        if contact == "override" {
            original["url"] = "http://localhost:32123".into();
        }
        fixture.write(&original);
        fixture.registration.reconcile().await.unwrap();
        let actual = fixture.read();
        assert_eq!(actual["url"], original["url"]);
        assert_eq!(
            actual["discovery"]["endpoints"]["proxy"]["pid"],
            std::process::id()
        );
        assert_eq!(
            actual["discovery"]["endpoints"]["tunnel"],
            original["discovery"]["endpoints"]["tunnel"]
        );
        assert_eq!(actual["discovery"]["future"], "keep");
        assert_eq!(
            actual["discovery"]["endpoints"]["proxy"]["description"],
            "preserve"
        );
        fixture.registration.restore().unwrap();
        assert_eq!(fixture.read(), original);
    }
    fixture.registration.metadata = false;
    let before = fixture.write(&fixture.origin());
    fixture.registration.reconcile().await.unwrap();
    assert_eq!(read_record(&fixture.registration.file).unwrap(), before);
}

#[tokio::test]
async fn stomp_reapplies_native_publishing_and_recreation_without_self_write_churn() {
    let mut fixture = Fixture::new().await;
    let mut original = fixture.origin();
    original["discovery"] = serde_json::json!({"version":1,"source":"override","endpoints":{
        "service":{"url":original["url"],"pid":123},
        "override":{"url":"http://localhost:32123"}
    }});
    original["url"] = "http://localhost:32123".into();
    for _ in 0..3 {
        let temporary = fixture._directory.path().join("publisher.tmp");
        std::fs::write(&temporary, serde_json::to_vec(&original).unwrap()).unwrap();
        std::fs::rename(temporary, &fixture.registration.file).unwrap();
        fixture.registration.reconcile().await.unwrap();
        assert_eq!(
            fixture.read()["url"],
            fixture
                .registration
                .advertised
                .as_str()
                .trim_end_matches('/')
        );
        let before = std::fs::metadata(&fixture.registration.file)
            .unwrap()
            .modified()
            .unwrap();
        for _ in 0..5 {
            fixture.registration.reconcile().await.unwrap();
        }
        assert_eq!(
            std::fs::metadata(&fixture.registration.file)
                .unwrap()
                .modified()
                .unwrap(),
            before
        );
        assert_eq!(
            fixture.read()["discovery"]["endpoints"]["override"],
            original["discovery"]["endpoints"]["override"]
        );
    }
    std::fs::remove_file(&fixture.registration.file).unwrap();
    fixture.registration.reconcile().await.unwrap();
    assert!(fixture.registration.owned.is_none());
    assert!(!fixture.registration.file.exists());
    fixture.write(&original);
    fixture.registration.reconcile().await.unwrap();
    fixture.registration.restore().unwrap();
    assert_eq!(fixture.read(), original);
}

#[tokio::test]
async fn dead_daemon_recovery_retains_the_publishers_proxy_route_and_metadata() {
    let mut fixture = Fixture::new().await;
    let mut original = fixture.origin();
    let advertised = fixture
        .registration
        .advertised
        .as_str()
        .trim_end_matches('/');
    original["discovery"] = serde_json::json!({"version":1,"source":"proxy","endpoints":{
        "service":{"url":original["url"],"pid":123}, "proxy":{"url":advertised}
    }});
    original["url"] = advertised.into();
    fixture.write(&original);
    fixture.registration.reconcile().await.unwrap();
    let mut stale = fixture.read();
    stale["_busProxy"]["pid"] = 2_000_000_000u32.into();
    stale["discovery"]["endpoints"]["proxy"]["pid"] = 2_000_000_000u32.into();
    fixture.write(&stale);
    fixture.registration.reconcile().await.unwrap();
    fixture.registration.restore().unwrap();
    assert_eq!(fixture.read(), original);
}

#[tokio::test]
async fn stomp_preserves_another_known_proxy_endpoint_instead_of_becoming_a_noop() {
    let mut fixture = Fixture::new().await;
    let mut original = fixture.origin();
    original["discovery"] = serde_json::json!({"version":1,"source":"proxy","endpoints":{
        "service":{"url":original["url"],"pid":123}, "proxy":{"url":"http://127.0.0.1:32123","note":"another known contact"}
    }});
    original["url"] = "http://127.0.0.1:32123".into();
    fixture.write(&original);
    fixture.registration.reconcile().await.unwrap();
    let actual = fixture.read();
    assert_eq!(
        actual["url"],
        fixture
            .registration
            .advertised
            .as_str()
            .trim_end_matches('/')
    );
    assert_eq!(
        actual["discovery"]["endpoints"]["proxy"],
        original["discovery"]["endpoints"]["proxy"]
    );
    assert_eq!(
        actual["discovery"]["endpoints"]["busProxy"]["pid"],
        std::process::id()
    );
    fixture.registration.restore().unwrap();
    assert_eq!(fixture.read(), original);
}

#[tokio::test]
async fn compatible_edits_and_partial_save_recovery_keep_our_token_reconcilable() {
    let mut fixture = Fixture::new().await;
    fixture.write(&fixture.origin());
    fixture.registration.reconcile().await.unwrap();
    let mut edited = fixture.read();
    edited["futureAddition"] = serde_json::json!({"keep":"new"});
    std::fs::write(&fixture.registration.file, b"{partial").unwrap();
    assert!(fixture.registration.reconcile().await.is_err());
    fixture.write(&edited);
    fixture.registration.reconcile().await.unwrap();
    assert!(fixture.registration.owned.is_some());
    assert_eq!(fixture.read()["futureAddition"], edited["futureAddition"]);
    fixture.registration.restore().unwrap();
    let mut restored = fixture.origin();
    restored["futureAddition"] = edited["futureAddition"].clone();
    assert_eq!(fixture.read(), restored);
}

#[tokio::test]
async fn origin_restart_reapplies_url_and_restores_latest_identity() {
    let mut fixture = Fixture::new().await;
    fixture.write(&fixture.origin());
    fixture.registration.reconcile().await.unwrap();
    let mut restart = fixture.origin();
    restart["pid"] = 456.into();
    restart["version"] = "test-v2".into();
    restart["id"] = "origin-2".into();
    fixture.write(&restart);
    // The new publisher may precede HTTP readiness. Don't restore the old one.
    fixture.registration.reconcile().await.unwrap();
    assert!(fixture.registration.owned.is_none());
    assert_eq!(fixture.read(), restart);
    *fixture.info.write().await = serde_json::json!({"pid":456,"version":"test-v2"});
    fixture.registration.reconcile().await.unwrap();
    let published = fixture.read();
    assert_eq!(published["pid"], 456);
    assert_eq!(published["version"], "test-v2");
    fixture.registration.restore().unwrap();
    assert_eq!(fixture.read(), restart);
}

#[tokio::test]
async fn absent_managed_foreign_malformed_and_bad_credentials_are_not_claimed() {
    let mut fixture = Fixture::new().await;
    fixture.registration.reconcile().await.unwrap();
    assert!(!fixture.registration.file.exists());
    for mutation in [
        serde_json::json!({"mode":null}),
        serde_json::json!({"url":"http://127.0.0.1:49374"}),
        serde_json::json!({"password":"another-secret"}),
        serde_json::json!({"pid":0}),
        serde_json::json!({"_busProxy":{"format":99,"pid":1,"token":"foreign","originUrl":"http://127.0.0.1:49373"}}),
    ] {
        let mut record = fixture.origin();
        for (field, value) in mutation.as_object().unwrap() {
            record[field] = value.clone();
        }
        let before = fixture.write(&record);
        fixture.registration.reconcile().await.unwrap();
        assert_eq!(read_record(&fixture.registration.file).unwrap(), before);
    }
    std::fs::write(&fixture.registration.file, b"invalid-json test-secret").unwrap();
    let error = fixture
        .registration
        .reconcile()
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(!error.contains("test-secret"));
    assert_eq!(
        read_record(&fixture.registration.file).unwrap(),
        b"invalid-json test-secret"
    );
    assert!(fixture.requests.read().await.is_empty());
}

#[tokio::test]
async fn health_pid_version_and_source_readiness_gate_publication() {
    let mut fixture = Fixture::new().await;
    let before = fixture.write(&fixture.origin());
    for bad in [
        serde_json::json!({"pid":999,"version":"test-v1"}),
        serde_json::json!({"pid":123,"version":"wrong"}),
        serde_json::json!({"pid":123}),
    ] {
        *fixture.info.write().await = bad;
        fixture.registration.reconcile().await.unwrap();
        assert_eq!(read_record(&fixture.registration.file).unwrap(), before);
    }
    *fixture.info.write().await = serde_json::json!({"pid":123,"version":"test-v1"});
    *fixture.health.write().await = StatusCode::SERVICE_UNAVAILABLE;
    fixture.registration.reconcile().await.unwrap();
    assert_eq!(read_record(&fixture.registration.file).unwrap(), before);
    *fixture.health.write().await = StatusCode::OK;
    fixture.registration.reconcile().await.unwrap();
    assert!(fixture.registration.owned.is_some());
}

#[tokio::test]
async fn shutdown_and_compare_write_never_overwrite_changed_or_deleted_slot() {
    let mut fixture = Fixture::new().await;
    let old = fixture.write(&fixture.origin());
    fixture.registration.reconcile().await.unwrap();
    let mut foreign = fixture.origin();
    foreign["url"] = "http://127.0.0.1:49374".into();
    let foreign_bytes = fixture.write(&foreign);
    assert!(!replace_if_unchanged(&fixture.registration.file, &old, b"replacement").unwrap());
    fixture.registration.restore().unwrap();
    assert_eq!(
        read_record(&fixture.registration.file).unwrap(),
        foreign_bytes
    );
    fixture.write(&fixture.origin());
    fixture.registration.reconcile().await.unwrap();
    std::fs::remove_file(&fixture.registration.file).unwrap();
    fixture.registration.restore().unwrap();
    assert!(!fixture.registration.file.exists());
    assert_eq!(
        std::fs::read_dir(fixture._directory.path())
            .unwrap()
            .count(),
        1
    ); // lock only
}

#[tokio::test]
async fn stale_marker_recovery_is_validated_and_live_contributor_is_preserved() {
    let mut fixture = Fixture::new().await;
    let mut stale = fixture.origin();
    stale["url"] = fixture
        .registration
        .advertised
        .as_str()
        .trim_end_matches('/')
        .into();
    stale["_busProxy"] = serde_json::json!({"format":1,"pid":std::process::id(),"token":"other","originUrl":"http://0.0.0.0:49373"});
    let live = fixture.write(&stale);
    fixture.registration.reconcile().await.unwrap();
    assert_eq!(read_record(&fixture.registration.file).unwrap(), live);
    // Linux PID maximum is below this; kill zero observes ESRCH.
    stale["_busProxy"]["pid"] = 2_000_000_000u32.into();
    fixture.write(&stale);
    fixture.registration.reconcile().await.unwrap();
    assert_eq!(
        fixture.read()["_busProxy"]["token"],
        fixture.registration.token
    );
    fixture.registration.restore().unwrap();
    assert_eq!(fixture.read(), fixture.origin());
}

#[tokio::test]
async fn locking_configuration_symlinks_and_size_fail_closed() {
    let fixture = Fixture::new().await;
    let create = |file: PathBuf, upstream: &str, advertised: Option<&str>, username: &str| {
        Registration::new(
            file,
            &Origin {
                upstream,
                username,
                password: None,
                request_timeout: Duration::from_secs(10),
            },
            advertised,
            "127.0.0.1:32123".parse().unwrap(),
            Mode::Cooperate,
            true,
        )
    };
    assert!(
        create(
            fixture.registration.file.clone(),
            "http://127.0.0.1:49373",
            None,
            "opencode"
        )
        .is_err()
    );
    assert!(
        create(
            "relative.json".into(),
            "http://127.0.0.1:49373",
            None,
            "opencode"
        )
        .is_err()
    );
    for upstream in [
        "http://192.0.2.1:49373",
        "https://localhost:49373",
        "http://127.0.0.1:32123",
    ] {
        assert!(
            create(
                fixture.registration.file.clone(),
                upstream,
                None,
                "opencode"
            )
            .is_err()
        );
    }
    assert!(
        create(
            fixture.registration.file.clone(),
            "http://127.0.0.1:49373",
            None,
            "other"
        )
        .is_err()
    );
    let target = fixture._directory.path().join("target");
    std::fs::write(&target, b"untouched").unwrap();
    std::os::unix::fs::symlink(&target, &fixture.registration.file).unwrap();
    assert!(read_record(&fixture.registration.file).is_err());
    assert!(replace_if_unchanged(&fixture.registration.file, b"untouched", b"wrong").is_err());
    assert_eq!(std::fs::read(&target).unwrap(), b"untouched");
    std::fs::remove_file(&fixture.registration.file).unwrap();
    std::fs::write(
        &fixture.registration.file,
        vec![b'x'; MAX_RECORD_BYTES as usize + 1],
    )
    .unwrap();
    assert!(read_record(&fixture.registration.file).is_err());
}
