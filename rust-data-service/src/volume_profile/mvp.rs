//! Single MVP collector/staging identity. Canonical bytes are versioned and immutable.
use super::work_policy::Limits;
use ring::digest::{digest, SHA256};

pub const HOUR: i64 = 3_600_000;
pub const RAW: usize = 2 * 1024 * 1024;
pub const ARCHIVE: usize = RAW + 64 * 1024;
pub const MANIFEST: u64 = RAW as u64;
pub const TIMEOUT_MS: u64 = 3000;
pub const WORK: Limits = Limits {
    requests: 200,
    response_bytes: 25 * 1024 * 1024,
    elapsed_ms: 300_000,
};
pub const MAX_RETRIES: u32 = 3;
pub const BASE_DELAY_MS: u64 = 250;
pub const MAX_DELAY_MS: u64 = 4000;
pub const GRID: &str = "btc_spot_usdt_10_v1";
// Canonical ASCII bytes, LF terminated, no whitespace normalization. Changing
// semantic or runtime configuration changes the job identity SHA-256.
pub const CONFIG: &str = concat!(
    "format=vp-collector-config-v1\nschema=1\nalgorithm=vp-v1\n",
    "product=BINANCE:BTCUSDT-SPOT\nendpoint=https://data-api.binance.vision/api/v3/aggTrades\n",
    "page_limit=1000\ngrid_id=btc_spot_usdt_10_v1\norigin=0\nstep=10\nunit=USDT\n",
    "timeout_ms=3000\nrequests=200\nresponse_bytes=26214400\nelapsed_ms=300000\n",
    "max_retries=3\nbase_delay_ms=250\nmax_delay_ms=4000\n",
    "raw_bytes=2097152\narchive_bytes=2162688\nmanifest_bytes=2097152\n"
);
pub fn config_hash() -> [u8; 32] {
    digest(&SHA256, CONFIG.as_bytes())
        .as_ref()
        .try_into()
        .unwrap()
}
pub fn config_hex() -> String {
    config_hash()
        .iter()
        .flat_map(|byte| [byte >> 4, byte & 15])
        .map(|n| b"0123456789abcdef"[n as usize] as char)
        .collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn canonical_config_golden_is_unchanged() {
        assert!(super::CONFIG.is_ascii());
        assert_eq!(
            super::config_hex(),
            "5750225c152de5eb92a31bbe325f6b4da1a186371b41bf3b7e00d1fdcb62e10e"
        );
    }
}
