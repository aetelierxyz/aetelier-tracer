use std::collections::BTreeMap;
use std::path::Path;

pub const SCHEMA: &str = "tracer.params.v1";

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolParams {
    pub stem: String,
    pub bar_threshold_usd: f64,
    pub maker_fee_leg_bps: f64,
    pub taker_fee_leg_bps: f64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TracerParams {
    pub schema: String,
    pub params_version: u32,
    pub tau_secs: f64,
    pub hold_ms: u64,
    pub notional_usd: f64,
    pub exit_staleness_ms: u64,
    pub t_split_ms: u64,
    pub decile_thresholds: Vec<f64>,
    pub symbols: Vec<SymbolParams>,
    pub input_sha256: BTreeMap<String, String>,
}

impl TracerParams {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let params: TracerParams = serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("{}: schema mismatch: {e}", path.display()))?;
        anyhow::ensure!(
            params.schema == SCHEMA,
            "{}: schema {} != {SCHEMA}",
            path.display(),
            params.schema
        );
        anyhow::ensure!(
            params.decile_thresholds.len() == 9,
            "{}: {} decile thresholds, want 9",
            path.display(),
            params.decile_thresholds.len()
        );
        anyhow::ensure!(!params.symbols.is_empty(), "{}: no symbols", path.display());
        Ok(params)
    }

    pub fn symbol(&self, stem: &str) -> anyhow::Result<&SymbolParams> {
        self.symbols.iter().find(|s| s.stem == stem).ok_or_else(|| {
            anyhow::anyhow!(
                "symbol {stem} absent from params artifact (have: {})",
                self.symbols
                    .iter()
                    .map(|s| s.stem.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn committed_artifact() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../params/tracer_params.v1.json")
    }

    #[test]
    fn committed_artifact_loads_and_is_complete() {
        let p = TracerParams::load(&committed_artifact()).unwrap();
        assert_eq!(p.params_version, 1);
        assert_eq!(p.tau_secs, 60.0);
        assert_eq!(p.hold_ms, 900_000);
        assert_eq!(p.notional_usd, 1_000.0);
        assert_eq!(p.exit_staleness_ms, 225_000);
        assert_eq!(p.symbols.len(), 5);
        assert_eq!(p.input_sha256.len(), 5);
        let tsla = p.symbol("xyz_TSLA").unwrap();
        assert_eq!(tsla.maker_fee_leg_bps, -0.015);
        assert_eq!(tsla.taker_fee_leg_bps, 1.044);
        let iren = p.symbol("para_IREN").unwrap();
        assert!(iren.maker_fee_leg_bps > 0.0);
    }

    #[test]
    fn unknown_symbol_fails_loudly() {
        let p = TracerParams::load(&committed_artifact()).unwrap();
        let err = p.symbol("xyz_GME").unwrap_err().to_string();
        assert!(err.contains("xyz_GME"));
        assert!(err.contains("xyz_TSLA"));
    }

    fn write_temp(name: &str, content: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("tracer_params_tests");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn wrong_schema_and_missing_fields_fail_loudly() {
        let wrong_schema = write_temp(
            "wrong_schema.json",
            r#"{"schema":"tracer.params.v2","params_version":1,"tau_secs":60.0,"hold_ms":900000,"notional_usd":1000.0,"exit_staleness_ms":225000,"t_split_ms":1,"decile_thresholds":[1,2,3,4,5,6,7,8,9],"symbols":[{"stem":"a","bar_threshold_usd":1.0,"maker_fee_leg_bps":0.0,"taker_fee_leg_bps":1.0}],"input_sha256":{}}"#,
        );
        assert!(TracerParams::load(&wrong_schema).is_err());

        let missing_field = write_temp("missing_field.json", r#"{"schema":"tracer.params.v1"}"#);
        assert!(TracerParams::load(&missing_field).is_err());

        let unknown_field = write_temp(
            "unknown_field.json",
            r#"{"schema":"tracer.params.v1","params_version":1,"tau_secs":60.0,"hold_ms":900000,"notional_usd":1000.0,"exit_staleness_ms":225000,"t_split_ms":1,"decile_thresholds":[1,2,3,4,5,6,7,8,9],"symbols":[],"input_sha256":{},"surprise":true}"#,
        );
        assert!(TracerParams::load(&unknown_field).is_err());

        let eight_thresholds = write_temp(
            "eight_thresholds.json",
            r#"{"schema":"tracer.params.v1","params_version":1,"tau_secs":60.0,"hold_ms":900000,"notional_usd":1000.0,"exit_staleness_ms":225000,"t_split_ms":1,"decile_thresholds":[1,2,3,4,5,6,7,8],"symbols":[{"stem":"a","bar_threshold_usd":1.0,"maker_fee_leg_bps":0.0,"taker_fee_leg_bps":1.0}],"input_sha256":{}}"#,
        );
        assert!(TracerParams::load(&eight_thresholds).is_err());

        let empty_symbols = write_temp(
            "empty_symbols.json",
            r#"{"schema":"tracer.params.v1","params_version":1,"tau_secs":60.0,"hold_ms":900000,"notional_usd":1000.0,"exit_staleness_ms":225000,"t_split_ms":1,"decile_thresholds":[1,2,3,4,5,6,7,8,9],"symbols":[],"input_sha256":{}}"#,
        );
        assert!(TracerParams::load(&empty_symbols).is_err());
    }
}
