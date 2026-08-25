pub mod bars;
pub mod book;
pub mod corrected;
pub mod deciles;
pub mod engine;
pub mod ledger;
pub mod market_engine;
pub mod nyse;
pub mod paper;
pub mod params;
pub mod replay;
pub mod signal;
pub mod stats;
pub mod trace;
pub mod verdict;

pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn engine_version_is_workspace_version() {
        assert_eq!(super::ENGINE_VERSION, "0.1.0");
    }
}
