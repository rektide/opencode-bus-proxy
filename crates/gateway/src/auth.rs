use crate::Config;
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use hmac::{Hmac, Mac};
use http::{Request, header};
use sha2::Sha256;
use subtle::ConstantTimeEq;

fn session(token: &str, password: &str) -> bool {
    let Some((expires, signature)) = token.split_once('.') else {
        return false;
    };
    // Native Number() accepts integer numeric notation; sign the original spelling.
    let Ok(expiry) = expires.trim().parse::<f64>() else {
        return false;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    if !expiry.is_finite()
        || expiry.fract() != 0.0
        || expiry.abs() > 9_007_199_254_740_991.0
        || expiry <= now
    {
        return false;
    }
    let mut derive =
        Hmac::<Sha256>::new_from_slice(password.as_bytes()).expect("HMAC accepts any key");
    derive.update(b"opencode-session-v1");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(&derive.finalize().into_bytes()).expect("HMAC key");
    mac.update(expires.as_bytes());
    let expected = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    bool::from(expected.as_bytes().ct_eq(signature.as_bytes()))
}

pub(crate) fn authorized<B>(request: &Request<B>, config: &Config) -> bool {
    let Some(password) = config.password.as_deref().filter(|v| !v.is_empty()) else {
        return true;
    };
    let query = url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
        .find(|(key, _)| key == "auth_token")
        .map(|(_, v)| v.into_owned())
        .filter(|v| !v.is_empty());
    let basic = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split_once(char::is_whitespace))
        .filter(|(kind, _)| kind.eq_ignore_ascii_case("basic"))
        .map(|(_, value)| value.trim_start().to_owned());
    if let Some(decoded) = query
        .or(basic)
        .and_then(|v| STANDARD.decode(v).ok())
        .and_then(|v| String::from_utf8(v).ok())
        && let Some((user, secret)) = decoded.split_once(':')
        && user == config.username
        && (bool::from(secret.as_bytes().ct_eq(password.as_bytes())) || session(secret, password))
    {
        return true;
    }
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if let Some(origin) = request.headers().get(header::ORIGIN) {
        let matches = origin
            .to_str()
            .ok()
            .and_then(|v| url::Url::parse(v).ok())
            .map(|v| v[url::Position::BeforeHost..url::Position::AfterPort].to_owned());
        if matches.as_deref() != Some(host) {
            return false;
        }
    }
    let parsed = url::Url::parse(&format!("http://{host}"));
    let name = parsed
        .ok()
        .and_then(|v| v.port())
        .map(|p| format!("opencode_session_{p}"))
        .unwrap_or_else(|| "opencode_session".into());
    request
        .headers()
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|v| v.trim().split_once('='))
        .any(|(key, value)| key == name && session(value, password))
}

pub(crate) fn cors_allowed(origin: &str, config: &Config) -> bool {
    if origin.starts_with("http://localhost:")
        || origin.starts_with("http://127.0.0.1:")
        || origin.starts_with("oc://renderer")
        || [
            "tauri://localhost",
            "http://tauri.localhost",
            "https://tauri.localhost",
        ]
        .contains(&origin)
        || config.cors.iter().any(|v| v == origin)
    {
        return true;
    }
    let Some(host) = origin.strip_prefix("https://") else {
        return false;
    };
    let labels: Vec<_> = host.split('.').collect();
    labels.len() >= 2
        && labels[labels.len() - 2..] == ["opencode", "ai"]
        && labels[..labels.len() - 2].iter().all(|v| {
            !v.is_empty()
                && v.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credential_precedence_and_native_challenge_boundary() {
        let config = Config {
            password: Some("secret".into()),
            ..Config::default()
        };
        let valid = format!("Basic {}", STANDARD.encode("opencode:secret"));
        let request = Request::builder()
            .uri("/api/event?auth_token=invalid")
            .header("authorization", &valid)
            .body(())
            .unwrap();
        assert!(
            !authorized(&request, &config),
            "query credentials take precedence"
        );
        let request = Request::builder()
            .uri("/api/event")
            .header("authorization", &valid)
            .body(())
            .unwrap();
        assert!(authorized(&request, &config));
        let request = Request::builder()
            .uri("/api/event")
            .header(
                "authorization",
                format!("Basic {}", STANDARD.encode("other:secret")),
            )
            .body(())
            .unwrap();
        assert!(!authorized(&request, &config));
        assert!(!session("0.expired", "secret"));
        assert!(!session("4102444800.forged", "secret"));
    }
    #[test]
    fn native_default_cors_and_exact_configured_origins() {
        let config = Config {
            cors: vec!["https://custom.test".into()],
            ..Config::default()
        };
        for origin in [
            "http://localhost:1",
            "http://127.0.0.1:2",
            "oc://renderer",
            "tauri://localhost",
            "http://tauri.localhost",
            "https://tauri.localhost",
            "https://opencode.ai",
            "https://a-b.opencode.ai",
            "https://custom.test",
        ] {
            assert!(cors_allowed(origin, &config), "{origin}");
        }
        for origin in [
            "https://opencode.ai.evil.test",
            "https://OPENCODE.ai",
            "https://opencode.ai/",
            "http://opencode.ai",
            "https://evil.test",
        ] {
            assert!(!cors_allowed(origin, &config), "{origin}");
        }
    }
}
