use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Destination {
    Otlp,
    ClickhouseNative,
}
impl Destination {
    pub fn feature(self) -> &'static str {
        match self {
            Self::Otlp => "otlp",
            Self::ClickhouseNative => "clickhouse-native",
        }
    }
}

pub fn select(
    disabled: Option<&str>,
    exporter: Option<&str>,
    otlp_endpoint: bool,
) -> Result<Vec<Destination>> {
    if disabled.is_some_and(|value| value.eq_ignore_ascii_case("true")) {
        return Ok(Vec::new());
    }
    let Some(exporter) = exporter else {
        return Ok(if otlp_endpoint {
            vec![Destination::Otlp]
        } else {
            Vec::new()
        });
    };
    let names: Vec<_> = exporter.split(',').map(str::trim).collect();
    if names == ["none"] {
        return Ok(Vec::new());
    }
    let mut selected = Vec::new();
    for name in names {
        let destination = match name {
            "otlp" => Destination::Otlp,
            "clickhouse-native" => Destination::ClickhouseNative,
            _ => return Err("OTEL_TRACES_EXPORTER must be otlp, clickhouse-native, their comma-separated pair, or none alone".into()),
        };
        if selected.contains(&destination) {
            return Err("duplicate OTEL_TRACES_EXPORTER destination".into());
        }
        selected.push(destination);
    }
    Ok(selected)
}
pub fn require(destination: Destination, compiled: bool) -> Result<()> {
    if !compiled {
        let feature = destination.feature();
        return Err(format!("{feature} export requested but not compiled; rebuild opencode-bus-proxy with --features {feature} (or disable with OTEL_TRACES_EXPORTER=none)").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_is_additive_disable_wins_and_inference_is_otlp_only() {
        assert!(select(None, None, false).unwrap().is_empty());
        assert_eq!(select(None, None, true).unwrap(), [Destination::Otlp]);
        assert_eq!(
            select(None, Some("clickhouse-native, otlp"), false).unwrap(),
            [Destination::ClickhouseNative, Destination::Otlp]
        );
        assert!(select(None, Some("none"), true).unwrap().is_empty());
        assert!(select(Some("TRUE"), Some("typo"), true).unwrap().is_empty());
        for invalid in ["none,otlp", "otlp,none", "", "typo", "otlp,otlp", "otlp,"] {
            assert!(select(None, Some(invalid), true).is_err());
        }
    }
    #[test]
    fn unavailable_destinations_are_actionable() {
        for destination in [Destination::Otlp, Destination::ClickhouseNative] {
            assert!(
                require(destination, false)
                    .unwrap_err()
                    .to_string()
                    .contains(&format!("--features {}", destination.feature()))
            );
            assert!(require(destination, true).is_ok());
        }
    }
}
