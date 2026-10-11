//! Host-only native configuration: environment, files and systemd credentials.
use super::setting;
use crate::Result;
use bus_proxy_telemetry::clickhouse_native::{Config, Table};
use std::{path::PathBuf, time::Duration};

const PREFIX: &str = "BUS_PROXY_CLICKHOUSE_NATIVE_";
fn get(suffix: &str) -> Option<String> {
    setting(&format!("{PREFIX}{suffix}"))
}
fn number<T: std::str::FromStr>(suffix: &str, default: T) -> Result<T> {
    get(suffix).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| format!("{PREFIX}{suffix} must be a valid positive integer").into())
    })
}
fn boolean(value: Option<&str>) -> Result<bool> {
    match value {
        None => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(format!("{PREFIX}TLS must be true or false").into()),
    }
}
fn password(
    file: Option<PathBuf>,
    credential: Option<String>,
    environment: Option<String>,
) -> Result<String> {
    if file.is_some() && credential.is_some() {
        return Err(
            format!("{PREFIX}PASSWORD_FILE and {PREFIX}CREDENTIAL are mutually exclusive").into(),
        );
    }
    let path = if let Some(file) = file {
        Some(file)
    } else if let Some(credential) = credential {
        Some(crate::credential_path(
            &credential,
            std::env::var_os("CREDENTIALS_DIRECTORY").as_deref(),
        )?)
    } else {
        None
    };
    if let Some(path) = path {
        Ok(std::fs::read_to_string(path)?
            .trim_end_matches(['\r', '\n'])
            .to_owned())
    } else {
        Ok(environment.unwrap_or_default())
    }
}
pub fn config() -> Result<Config> {
    for name in bus_proxy_telemetry::clickhouse_native::UPSTREAM_ENVIRONMENT_FLAGS {
        if std::env::var_os(name).is_some() {
            return Err(format!("native ClickHouse upstream environment flag {name} is unsupported by this daemon; unset it (debug Arrow can expose batch data, buffer flags bypass typed bounds)").into());
        }
    }
    let tls = boolean(get("TLS").as_deref())?;
    let config = Config {
        host: get("HOST").ok_or("native ClickHouse export requires BUS_PROXY_CLICKHOUSE_NATIVE_HOST (native host, not an HTTP URL)")?,
        port: number("PORT", if tls { 9440 } else { 9000 })?,
        tls,
        tls_server_name: get("TLS_SERVER_NAME"),
        username: get("USERNAME").unwrap_or_else(|| "default".into()),
        password: password(get("PASSWORD_FILE").map(PathBuf::from), get("CREDENTIAL"), get("PASSWORD"))?,
        table: Table::new(get("DATABASE").unwrap_or_else(|| "default".into()), get("TABLE").ok_or("native ClickHouse export requires BUS_PROXY_CLICKHOUSE_NATIVE_TABLE; provision the documented schema explicitly")?)?,
        operation_timeout: Duration::from_millis(number("TIMEOUT_MS", 10_000)?),
        max_batch_rows: number("MAX_BATCH_ROWS", 4096)?,
        max_batch_bytes: number("MAX_BATCH_BYTES", 16 * 1024 * 1024)?,
    };
    config.validate()?;
    Ok(config)
}

pub fn projection(config: &Config) -> Result<bus_proxy_telemetry::clickhouse_native::otel::Policy> {
    use bus_proxy_telemetry::clickhouse_native::otel::Policy;
    let defaults = Policy::default();
    let mut policy = Policy {
        max_spans: config.max_batch_rows,
        max_batch_bytes: config.max_batch_bytes,
        max_string_bytes: number("MAX_STRING_BYTES", defaults.max_string_bytes)?,
        max_attributes: number("MAX_ATTRIBUTES", defaults.max_attributes)?,
        max_events: number("MAX_EVENTS", defaults.max_events)?,
        max_links: number("MAX_LINKS", defaults.max_links)?,
        max_array_items: number("MAX_ARRAY_ITEMS", defaults.max_array_items)?,
        redact_keys: defaults.redact_keys,
    };
    if let Some(keys) = get("REDACT_KEYS") {
        for key in keys.split(',').map(str::trim) {
            if key.is_empty() {
                return Err("BUS_PROXY_CLICKHOUSE_NATIVE_REDACT_KEYS must contain nonempty comma-separated keys".into());
            }
            policy.redact_keys.push(key.to_owned());
        }
    }
    policy.validate()?;
    Ok(policy)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tls_boolean_is_strict() {
        assert!(boolean(None).unwrap());
        assert!(!boolean(Some("false")).unwrap());
        assert!(boolean(Some("TRUE")).unwrap());
        assert!(boolean(Some("1")).is_err());
    }
    #[test]
    fn secret_file_precedence_and_newline_policy() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("password");
        std::fs::write(&file, " secret \r\n").unwrap();
        assert_eq!(
            password(Some(file.clone()), None, Some("environment".into())).unwrap(),
            " secret "
        );
        assert_eq!(
            password(None, None, Some("environment \n".into())).unwrap(),
            "environment \n"
        );
        assert!(password(Some(file), Some("credential".into()), None).is_err());
        assert!(password(None, Some("../traversal".into()), None).is_err());
    }
}
