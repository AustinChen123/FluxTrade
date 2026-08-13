use crate::{model, AggregationSourceEvent, TaskId};
use anyhow::Result;
use std::collections::HashSet;
use tokio::{sync::mpsc, task::JoinSet};
use tracing::info;

pub(crate) struct LiveRuntimeOptions {
    exchange: Option<String>,
    symbol: Option<String>,
    #[cfg(feature = "rithmic")]
    rithmic_profile: Option<String>,
    #[cfg(feature = "rithmic")]
    rithmic_account_id: Option<String>,
    #[cfg(feature = "rithmic")]
    rithmic_product_id: Option<String>,
    #[cfg(feature = "rithmic")]
    rithmic_exchange: Option<String>,
    #[cfg(feature = "rithmic")]
    rithmic_symbol: Option<String>,
}

impl LiveRuntimeOptions {
    pub(crate) fn new(
        exchange: Option<String>,
        symbol: Option<String>,
        #[cfg(feature = "rithmic")] rithmic_profile: Option<String>,
        #[cfg(feature = "rithmic")] rithmic_account_id: Option<String>,
        #[cfg(feature = "rithmic")] rithmic_product_id: Option<String>,
        #[cfg(feature = "rithmic")] rithmic_exchange: Option<String>,
        #[cfg(feature = "rithmic")] rithmic_symbol: Option<String>,
    ) -> Self {
        Self {
            exchange,
            symbol,
            #[cfg(feature = "rithmic")]
            rithmic_profile,
            #[cfg(feature = "rithmic")]
            rithmic_account_id,
            #[cfg(feature = "rithmic")]
            rithmic_product_id,
            #[cfg(feature = "rithmic")]
            rithmic_exchange,
            #[cfg(feature = "rithmic")]
            rithmic_symbol,
        }
    }
}

pub(crate) struct LiveRuntime {
    enabled_exchanges: Vec<String>,
    symbols: Vec<String>,
    backpack_symbols: Vec<String>,
    binance_user_stream_enabled: bool,
    backpack_user_stream_enabled: bool,
    #[cfg(feature = "rithmic")]
    rithmic_watchdog_identity: Option<(String, String)>,
    #[cfg(feature = "rithmic")]
    rithmic_config: Option<super::rithmic::live::LiveConfig>,
}

impl LiveRuntime {
    pub(crate) fn prepare(options: LiveRuntimeOptions) -> Result<Self> {
        Self::prepare_with_lookup(options, |name| std::env::var(name).ok())
    }

    fn prepare_with_lookup(
        options: LiveRuntimeOptions,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let enabled_exchanges_raw = options
            .exchange
            .or_else(|| non_empty_value(lookup("EXCHANGE_ENABLED")))
            .unwrap_or_else(|| "binance,bybit,backpack".into());
        let enabled_exchanges = validate_enabled_exchanges(&enabled_exchanges_raw)?;
        let (binance_user_stream_enabled, backpack_user_stream_enabled) =
            preflight_user_stream_credentials(&enabled_exchanges, &lookup)?;

        #[cfg(feature = "rithmic")]
        let (rithmic_watchdog_identity, rithmic_config) = {
            let resolved = super::rithmic::live::resolve_live_options(
                &enabled_exchanges,
                super::rithmic::live::LiveOptions::new(
                    options.rithmic_profile,
                    options.rithmic_account_id,
                    options.rithmic_product_id,
                    options.rithmic_exchange,
                    options.rithmic_symbol,
                ),
                &lookup,
            )?;
            match resolved {
                Some(resolved) => {
                    let (profile, account_id) = resolved.watchdog_identity();
                    let identity = Some((profile.to_string(), account_id.to_string()));
                    let config = Some(resolved.configure()?);
                    (identity, config)
                }
                None => (None, None),
            }
        };

        let symbols_str = options
            .symbol
            .or_else(|| non_empty_value(lookup("MARKET_DATA_SYMBOLS")))
            .unwrap_or_else(|| "BTCUSDT,SOLUSDC".into());
        let symbols = parse_unique_csv("MARKET_DATA_SYMBOLS", &symbols_str, str::to_uppercase)?;
        let backpack_symbols = if enabled_exchanges.iter().any(|value| value == "backpack") {
            super::backpack::resolve_market_data_symbols(
                non_empty_value(lookup("BACKPACK_MARKET_DATA_SYMBOLS")).as_deref(),
            )?
        } else {
            Vec::new()
        };

        Ok(Self {
            enabled_exchanges,
            symbols,
            backpack_symbols,
            binance_user_stream_enabled,
            backpack_user_stream_enabled,
            #[cfg(feature = "rithmic")]
            rithmic_watchdog_identity,
            #[cfg(feature = "rithmic")]
            rithmic_config,
        })
    }

    pub(crate) fn watchdog_identity(&self) -> Option<(&str, &str)> {
        #[cfg(feature = "rithmic")]
        {
            return self
                .rithmic_watchdog_identity
                .as_ref()
                .map(|(profile, account_id)| (profile.as_str(), account_id.as_str()));
        }
        #[cfg(not(feature = "rithmic"))]
        None
    }

    pub(crate) fn spawn(
        mut self,
        join_set: &mut JoinSet<(TaskId, Result<()>)>,
        trade_tx: mpsc::Sender<model::Trade>,
        candle_tx: mpsc::Sender<model::Candlestick>,
        user_tx: mpsc::Sender<model::UserStreamEvent>,
        aggregation_source_tx: mpsc::Sender<AggregationSourceEvent>,
    ) {
        #[cfg(not(feature = "rithmic"))]
        let _ = aggregation_source_tx;
        for exchange_name in std::mem::take(&mut self.enabled_exchanges) {
            let trade_tx = trade_tx.clone();
            let candle_tx = candle_tx.clone();
            let user_tx = user_tx.clone();
            let symbols = self.symbols.clone();
            let backpack_symbols = self.backpack_symbols.clone();

            match exchange_name.as_str() {
                "binance" => {
                    let user_stream_enabled = self.binance_user_stream_enabled;
                    join_set.spawn(async move {
                        let result = super::binance::run(
                            symbols,
                            trade_tx,
                            candle_tx,
                            user_tx,
                            user_stream_enabled,
                        )
                        .await;
                        (TaskId::Connector("binance".to_string()), result)
                    });
                    info!("Supervised task spawned: connector:binance");
                }
                "bybit" => {
                    join_set.spawn(async move {
                        let result = super::bybit::run(symbols, trade_tx, candle_tx).await;
                        (TaskId::Connector("bybit".to_string()), result)
                    });
                    info!("Supervised task spawned: connector:bybit");
                }
                "backpack" => {
                    let user_stream_enabled = self.backpack_user_stream_enabled;
                    join_set.spawn(async move {
                        let result = super::backpack::run(
                            backpack_symbols,
                            trade_tx,
                            candle_tx,
                            user_tx,
                            user_stream_enabled,
                        )
                        .await;
                        (TaskId::Connector("backpack".to_string()), result)
                    });
                    info!("Supervised task spawned: connector:backpack");
                }
                #[cfg(feature = "rithmic")]
                "rithmic" => {
                    let config = self
                        .rithmic_config
                        .take()
                        .expect("Rithmic configuration loaded");
                    let aggregation_source_tx = aggregation_source_tx.clone();
                    join_set.spawn(async move {
                        let result = super::rithmic::live::run(config, aggregation_source_tx).await;
                        (TaskId::Connector("rithmic".to_string()), result)
                    });
                    info!("Supervised task spawned: connector:rithmic");
                }
                _ => unreachable!("validated exchange: {exchange_name}"),
            }
        }
    }
}

fn non_empty_value(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_unique_csv(
    name: &str,
    value: &str,
    canonicalize: fn(&str) -> String,
) -> Result<Vec<String>> {
    let values: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(canonicalize)
        .collect();
    if values.is_empty() {
        anyhow::bail!("{name} must contain at least one value");
    }
    let unique: HashSet<&str> = values.iter().map(String::as_str).collect();
    if unique.len() != values.len() {
        anyhow::bail!("{name} must not contain duplicate values");
    }
    Ok(values)
}

fn validate_enabled_exchanges(value: &str) -> Result<Vec<String>> {
    let exchanges = parse_unique_csv("EXCHANGE_ENABLED", value, str::to_lowercase)?;
    for exchange in &exchanges {
        match exchange.as_str() {
            "binance" | "bybit" | "backpack" => {}
            #[cfg(feature = "rithmic")]
            "rithmic" => {}
            _ => anyhow::bail!("unsupported or unavailable exchange: {exchange}"),
        }
    }
    Ok(exchanges)
}

fn preflight_user_stream_credentials(
    enabled_exchanges: &[String],
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<(bool, bool)> {
    let binance_enabled = enabled_exchanges.iter().any(|value| value == "binance");
    let backpack_enabled = enabled_exchanges.iter().any(|value| value == "backpack");
    let binance_user_stream = if binance_enabled {
        super::binance::preflight_user_stream_credentials(&lookup)?
    } else {
        false
    };
    let backpack_user_stream = if backpack_enabled {
        super::backpack::preflight_user_stream_credentials(&lookup)?
    } else {
        false
    };
    Ok((binance_user_stream, backpack_user_stream))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(exchange: Option<String>, symbol: Option<String>) -> LiveRuntimeOptions {
        LiveRuntimeOptions::new(
            exchange,
            symbol,
            #[cfg(feature = "rithmic")]
            None,
            #[cfg(feature = "rithmic")]
            None,
            #[cfg(feature = "rithmic")]
            None,
            #[cfg(feature = "rithmic")]
            None,
            #[cfg(feature = "rithmic")]
            None,
        )
    }

    #[test]
    fn production_runtime_csv_values_fail_closed() {
        assert!(parse_unique_csv("MARKET_DATA_SYMBOLS", " , ", str::to_uppercase).is_err());
        assert_eq!(
            parse_unique_csv("MARKET_DATA_SYMBOLS", " btcusdt, mnqu6 ", str::to_uppercase).unwrap(),
            vec!["BTCUSDT", "MNQU6"]
        );
        assert!(parse_unique_csv("MARKET_DATA_SYMBOLS", "mnqu6,MNQU6", str::to_uppercase).is_err());
        assert!(validate_enabled_exchanges("unknown").is_err());
        assert_eq!(
            validate_enabled_exchanges("BINANCE,bybit").unwrap(),
            vec!["binance", "bybit"]
        );
        assert!(validate_enabled_exchanges("binance,BINANCE").is_err());
    }

    #[test]
    fn optional_environment_values_are_trimmed() {
        assert_eq!(non_empty_value(None), None);
        assert_eq!(non_empty_value(Some(String::new())), None);
        assert_eq!(non_empty_value(Some("  ".to_string())), None);
        assert_eq!(
            non_empty_value(Some(" key ".to_string())),
            Some("key".to_string())
        );
    }

    #[test]
    fn user_stream_credentials_are_preflighted_for_enabled_exchanges() {
        let enabled = vec!["binance".to_string(), "backpack".to_string()];
        let complete = std::collections::HashMap::from([
            ("BINANCE_API_KEY", "binance-key"),
            ("EXCHANGE_API_KEY", "backpack-key"),
            ("EXCHANGE_SECRET", "backpack-secret"),
        ]);
        assert_eq!(
            preflight_user_stream_credentials(&enabled, |name| {
                complete.get(name).map(|value| (*value).to_string())
            })
            .unwrap(),
            (true, true)
        );

        let partial = std::collections::HashMap::from([("EXCHANGE_API_KEY", "backpack-key")]);
        assert!(preflight_user_stream_credentials(&enabled, |name| {
            partial.get(name).map(|value| (*value).to_string())
        })
        .is_err());

        assert_eq!(
            preflight_user_stream_credentials(&enabled, |_| None).unwrap(),
            (false, false)
        );

        let binance_only = vec!["binance".to_string()];
        assert_eq!(
            preflight_user_stream_credentials(&binance_only, |name| {
                partial.get(name).map(|value| (*value).to_string())
            })
            .unwrap(),
            (false, false)
        );

        let backpack_only = vec!["backpack".to_string()];
        let backpack_complete_with_invalid_binance = std::collections::HashMap::from([
            ("BINANCE_API_KEY", " binance-key "),
            ("EXCHANGE_API_KEY", "backpack-key"),
            ("EXCHANGE_SECRET", "backpack-secret"),
        ]);
        assert_eq!(
            preflight_user_stream_credentials(&backpack_only, |name| {
                backpack_complete_with_invalid_binance
                    .get(name)
                    .map(|value| (*value).to_string())
            })
            .unwrap(),
            (false, true)
        );

        let both_invalid = std::collections::HashMap::from([
            ("BINANCE_API_KEY", " binance-key "),
            ("EXCHANGE_API_KEY", "backpack-key"),
        ]);
        assert_eq!(
            preflight_user_stream_credentials(&enabled, |name| {
                both_invalid.get(name).map(|value| (*value).to_string())
            })
            .unwrap_err()
            .to_string(),
            "BINANCE_API_KEY must not contain surrounding whitespace"
        );
    }

    #[test]
    fn preparation_preserves_venue_order_symbols_and_stream_flags() {
        let values = std::collections::HashMap::from([
            ("BINANCE_API_KEY", "binance-key"),
            ("EXCHANGE_API_KEY", "backpack-key"),
            ("EXCHANGE_SECRET", "backpack-secret"),
            (
                "BACKPACK_MARKET_DATA_SYMBOLS",
                "SOL_USDC_PERP,BTC_USDC_PERP",
            ),
        ]);
        let runtime = LiveRuntime::prepare_with_lookup(
            options(
                Some("backpack,binance,bybit".to_string()),
                Some("ethusdt,btcusdt".to_string()),
            ),
            |name| values.get(name).map(|value| (*value).to_string()),
        )
        .unwrap();

        assert_eq!(runtime.enabled_exchanges, ["backpack", "binance", "bybit"]);
        assert_eq!(runtime.symbols, ["ETHUSDT", "BTCUSDT"]);
        assert_eq!(runtime.backpack_symbols, ["SOL_USDC_PERP", "BTC_USDC_PERP"]);
        assert!(runtime.binance_user_stream_enabled);
        assert!(runtime.backpack_user_stream_enabled);
        assert_eq!(runtime.watchdog_identity(), None);
    }

    #[test]
    fn credential_failure_precedes_later_symbol_validation() {
        let values = std::collections::HashMap::from([("EXCHANGE_API_KEY", "backpack-key")]);
        let error = LiveRuntime::prepare_with_lookup(
            options(Some("backpack".to_string()), Some(" , ".to_string())),
            |name| values.get(name).map(|value| (*value).to_string()),
        )
        .err()
        .unwrap();

        assert_eq!(
            error.to_string(),
            "optional credentials must be provided together: EXCHANGE_API_KEY, EXCHANGE_SECRET"
        );
    }

    #[test]
    fn production_composition_has_one_row_per_supported_connector() {
        let product = include_str!("live_runtime.rs")
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .unwrap()
            .0;
        for provider_call in [
            "super::binance::run(",
            "super::bybit::run(",
            "super::backpack::run(",
            "super::rithmic::live::run(",
        ] {
            assert_eq!(product.matches(provider_call).count(), 1, "{provider_call}");
        }
    }

    #[cfg(not(feature = "rithmic"))]
    #[test]
    fn rithmic_requires_a_rithmic_enabled_build() {
        assert!(validate_enabled_exchanges("rithmic").is_err());
    }

    #[cfg(feature = "rithmic")]
    #[test]
    fn rithmic_live_option_policy_is_composed_once() {
        let product = include_str!("live_runtime.rs")
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .unwrap()
            .0;
        for operation in [
            "super::rithmic::live::resolve_live_options(",
            "resolved.watchdog_identity()",
            "resolved.configure()?",
        ] {
            assert_eq!(product.matches(operation).count(), 1, "{operation}");
        }
    }
}
