use crate::{live_event_pipeline::AggregationSourceEvent, model, runtime_supervisor::TaskId};
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
        let execution_venue = non_empty_value(lookup("EXCHANGE_ID"))
            .ok_or_else(|| anyhow::anyhow!("EXCHANGE_ID must be set explicitly in live"))?
            .to_ascii_lowercase();
        if !enabled_exchanges.contains(&execution_venue) {
            anyhow::bail!("EXCHANGE_ID must be included in EXCHANGE_ENABLED: {execution_venue}");
        }

        let mut symbol = options.symbol;
        let mut resolved_symbols = None;
        if execution_venue != "rithmic" {
            let (symbols, backpack_symbols) =
                resolve_market_symbols(symbol.take(), &enabled_exchanges, &lookup)?;
            let product_ids = lookup("INSTRUMENT_PRODUCT_IDS").unwrap_or_default();
            validate_execution_product_coverage(
                &execution_venue,
                &product_ids,
                &symbols,
                &backpack_symbols,
            )?;
            resolved_symbols = Some((symbols, backpack_symbols));
        }

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

        let (symbols, backpack_symbols) = match resolved_symbols {
            Some(symbols) => symbols,
            None => resolve_market_symbols(symbol, &enabled_exchanges, &lookup)?,
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

fn resolve_market_symbols(
    symbol: Option<String>,
    enabled_exchanges: &[String],
    lookup: &impl Fn(&str) -> Option<String>,
) -> Result<(Vec<String>, Vec<String>)> {
    let symbols_str = symbol
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
    Ok((symbols, backpack_symbols))
}

fn validate_execution_product_coverage(
    execution_venue: &str,
    product_ids: &str,
    symbols: &[String],
    backpack_symbols: &[String],
) -> Result<()> {
    let product_ids = parse_unique_csv("INSTRUMENT_PRODUCT_IDS", product_ids, str::to_string)?;
    let expected_venue = execution_venue.to_ascii_uppercase();
    let available_symbols = if execution_venue == "backpack" {
        backpack_symbols
    } else {
        symbols
    };

    for product_id in product_ids {
        let Some((venue, product)) = product_id.split_once(':') else {
            anyhow::bail!("invalid live execution product id for {execution_venue}: {product_id}");
        };
        if venue != expected_venue {
            anyhow::bail!(
                "INSTRUMENT_PRODUCT_IDS must contain only {expected_venue} products: {product_id}"
            );
        }
        let Some(contract) = product.strip_suffix("-PERP") else {
            anyhow::bail!("invalid live execution product id for {execution_venue}: {product_id}");
        };
        let valid_contract = !contract.is_empty()
            && contract
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_');
        let connector_symbol = if execution_venue == "backpack" {
            let parts: Vec<&str> = contract.split('_').collect();
            if !valid_contract || parts.len() != 2 || parts.iter().any(|part| part.is_empty()) {
                anyhow::bail!(
                    "invalid live execution product id for {execution_venue}: {product_id}"
                );
            }
            format!("{contract}_PERP")
        } else {
            if !valid_contract || contract.contains('_') {
                anyhow::bail!(
                    "invalid live execution product id for {execution_venue}: {product_id}"
                );
            }
            contract.to_string()
        };
        if !available_symbols.contains(&connector_symbol) {
            anyhow::bail!(
                "INSTRUMENT_PRODUCT_IDS product is not covered by {execution_venue} market data: {product_id}"
            );
        }
    }
    Ok(())
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
    fn execution_venue_must_be_covered_before_later_runtime_preparation() {
        let cases = [
            (None, "EXCHANGE_ID must be set explicitly in live"),
            (
                Some("binance"),
                "EXCHANGE_ID must be included in EXCHANGE_ENABLED: binance",
            ),
        ];

        for (execution_venue, expected) in cases {
            let lookup_count = std::cell::Cell::new(0);
            let error = LiveRuntime::prepare_with_lookup(
                options(Some("bybit".to_string()), Some("btcusdt".to_string())),
                |name| {
                    assert_eq!(name, "EXCHANGE_ID");
                    lookup_count.set(lookup_count.get() + 1);
                    execution_venue.map(str::to_string)
                },
            )
            .err()
            .unwrap();

            assert_eq!(error.to_string(), expected);
            assert_eq!(lookup_count.get(), 1);
        }
    }

    #[test]
    fn execution_venue_coverage_precedes_credentials_and_symbols() {
        let cases = [
            (
                options(Some("binance".to_string()), Some("btcusdt".to_string())),
                "bybit",
                "BINANCE_API_KEY",
            ),
            (
                options(Some("bybit".to_string()), None),
                "binance",
                "MARKET_DATA_SYMBOLS",
            ),
        ];

        for (options, execution_venue, forbidden_lookup) in cases {
            let error = LiveRuntime::prepare_with_lookup(options, |name| match name {
                "EXCHANGE_ID" => Some(execution_venue.to_string()),
                name if name == forbidden_lookup => panic!("later preparation must not run"),
                _ => None,
            })
            .err()
            .unwrap();

            assert_eq!(
                error.to_string(),
                format!("EXCHANGE_ID must be included in EXCHANGE_ENABLED: {execution_venue}")
            );
        }
    }

    #[test]
    fn execution_products_require_matching_market_data_symbols() {
        for (enabled, execution_venue, products, symbols, backpack_symbols, expected) in [
            (
                "binance",
                "binance",
                "BINANCE:ETHUSDT-PERP",
                "BTCUSDT",
                None,
                "INSTRUMENT_PRODUCT_IDS product is not covered by binance market data: BINANCE:ETHUSDT-PERP",
            ),
            (
                "bybit",
                "bybit",
                "BYBIT:ETHUSDT-PERP",
                "BTCUSDT",
                None,
                "INSTRUMENT_PRODUCT_IDS product is not covered by bybit market data: BYBIT:ETHUSDT-PERP",
            ),
            (
                "backpack",
                "backpack",
                "BACKPACK:ETH_USDC-PERP",
                "BTCUSDT",
                Some("BTC_USDC_PERP"),
                "INSTRUMENT_PRODUCT_IDS product is not covered by backpack market data: BACKPACK:ETH_USDC-PERP",
            ),
        ] {
            let values = std::collections::HashMap::from([
                ("EXCHANGE_ID", execution_venue),
                ("INSTRUMENT_PRODUCT_IDS", products),
                ("MARKET_DATA_SYMBOLS", symbols),
                (
                    "BACKPACK_MARKET_DATA_SYMBOLS",
                    backpack_symbols.unwrap_or_default(),
                ),
            ]);
            let error = LiveRuntime::prepare_with_lookup(options(Some(enabled.into()), None), |name| {
                values
                    .get(name)
                    .filter(|value| !value.is_empty())
                    .map(|value| (*value).to_string())
            })
            .err()
            .unwrap();

            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn execution_products_accept_exact_and_strict_superset_coverage() {
        for (enabled, execution_venue, products, symbols, backpack_symbols) in [
            (
                "binance",
                "binance",
                "BINANCE:BTCUSDT-PERP",
                "BTCUSDT",
                None,
            ),
            (
                "binance",
                "binance",
                "BINANCE:BTCUSDT-PERP",
                "BTCUSDT,ETHUSDT",
                None,
            ),
            (
                "bybit",
                "bybit",
                "BYBIT:BTCUSDT-PERP,BYBIT:ETHUSDT-PERP",
                "BTCUSDT,ETHUSDT",
                None,
            ),
            (
                "bybit",
                "bybit",
                "BYBIT:BTCUSDT-PERP,BYBIT:ETHUSDT-PERP",
                "SOLUSDT,BTCUSDT,ETHUSDT",
                None,
            ),
            (
                "backpack",
                "backpack",
                "BACKPACK:BTC_USDC-PERP",
                "UNRELATED",
                Some("BTC_USDC_PERP"),
            ),
            (
                "backpack",
                "backpack",
                "BACKPACK:BTC_USDC-PERP",
                "UNRELATED",
                Some("SOL_USDC_PERP,BTC_USDC_PERP"),
            ),
        ] {
            let values = std::collections::HashMap::from([
                ("EXCHANGE_ID", execution_venue),
                ("INSTRUMENT_PRODUCT_IDS", products),
                ("MARKET_DATA_SYMBOLS", symbols),
                (
                    "BACKPACK_MARKET_DATA_SYMBOLS",
                    backpack_symbols.unwrap_or_default(),
                ),
            ]);
            let runtime =
                LiveRuntime::prepare_with_lookup(options(Some(enabled.into()), None), |name| {
                    values
                        .get(name)
                        .filter(|value| !value.is_empty())
                        .map(|value| (*value).to_string())
                })
                .unwrap();

            assert_eq!(runtime.enabled_exchanges, [execution_venue]);
        }
    }

    #[test]
    fn execution_product_shape_and_venue_fail_closed() {
        for (execution_venue, products, expected) in [
            (
                "binance",
                "",
                "INSTRUMENT_PRODUCT_IDS must contain at least one value",
            ),
            (
                "binance",
                "BINANCE:BTCUSDT-PERP,BINANCE:BTCUSDT-PERP",
                "INSTRUMENT_PRODUCT_IDS must not contain duplicate values",
            ),
            (
                "binance",
                "BYBIT:BTCUSDT-PERP",
                "INSTRUMENT_PRODUCT_IDS must contain only BINANCE products: BYBIT:BTCUSDT-PERP",
            ),
            (
                "binance",
                "BINANCE:BTCUSDT",
                "invalid live execution product id for binance: BINANCE:BTCUSDT",
            ),
            (
                "binance",
                "BINANCE:btcusdt-PERP",
                "invalid live execution product id for binance: BINANCE:btcusdt-PERP",
            ),
            (
                "backpack",
                "BACKPACK:BTCUSDC-PERP",
                "invalid live execution product id for backpack: BACKPACK:BTCUSDC-PERP",
            ),
            (
                "backpack",
                "BACKPACK:BTC_USDC_EXTRA-PERP",
                "invalid live execution product id for backpack: BACKPACK:BTC_USDC_EXTRA-PERP",
            ),
        ] {
            let values = std::collections::HashMap::from([
                ("EXCHANGE_ID", execution_venue),
                ("INSTRUMENT_PRODUCT_IDS", products),
                ("MARKET_DATA_SYMBOLS", "BTCUSDT"),
                ("BACKPACK_MARKET_DATA_SYMBOLS", "BTC_USDC_PERP"),
            ]);
            let error = LiveRuntime::prepare_with_lookup(
                options(Some(execution_venue.into()), None),
                |name| values.get(name).map(|value| (*value).to_string()),
            )
            .err()
            .unwrap();

            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn multi_source_coverage_constrains_only_the_execution_venue() {
        let values = std::collections::HashMap::from([
            ("EXCHANGE_ID", "bybit"),
            ("INSTRUMENT_PRODUCT_IDS", "BYBIT:ETHUSDT-PERP"),
            ("MARKET_DATA_SYMBOLS", "ETHUSDT"),
            ("BACKPACK_MARKET_DATA_SYMBOLS", "BTC_USDC_PERP"),
        ]);
        let runtime = LiveRuntime::prepare_with_lookup(
            options(Some("binance,backpack,bybit".into()), None),
            |name| values.get(name).map(|value| (*value).to_string()),
        )
        .unwrap();

        assert_eq!(runtime.enabled_exchanges, ["binance", "backpack", "bybit"]);
    }

    #[test]
    fn coverage_rejection_precedes_credential_lookup() {
        let error = LiveRuntime::prepare_with_lookup(
            options(Some("binance".into()), Some("BTCUSDT".into())),
            |name| match name {
                "EXCHANGE_ID" => Some("binance".into()),
                "INSTRUMENT_PRODUCT_IDS" => Some("BINANCE:ETHUSDT-PERP".into()),
                name if name.contains("API_KEY") || name.contains("SECRET") => {
                    panic!("credential lookup must not run before coverage rejection")
                }
                _ => None,
            },
        )
        .err()
        .unwrap();

        assert_eq!(
            error.to_string(),
            "INSTRUMENT_PRODUCT_IDS product is not covered by binance market data: BINANCE:ETHUSDT-PERP"
        );
    }

    #[test]
    fn enabled_exchange_validation_precedes_execution_membership() {
        for (enabled, expected) in [
            ("unknown", "unsupported or unavailable exchange: unknown"),
            (
                "binance,BINANCE",
                "EXCHANGE_ENABLED must not contain duplicate values",
            ),
        ] {
            let error = LiveRuntime::prepare_with_lookup(
                options(Some(enabled.to_string()), Some("btcusdt".to_string())),
                |_| panic!("execution venue lookup must not run"),
            )
            .err()
            .unwrap();

            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn execution_venue_membership_accepts_exact_and_multi_source_market_data() {
        for (enabled, execution_venue, product_id, expected) in [
            ("bybit", " BYBIT ", "BYBIT:BTCUSDT-PERP", vec!["bybit"]),
            (
                "binance,bybit",
                "bybit",
                "BYBIT:BTCUSDT-PERP",
                vec!["binance", "bybit"],
            ),
        ] {
            let runtime = LiveRuntime::prepare_with_lookup(
                options(Some(enabled.to_string()), Some("btcusdt".to_string())),
                |name| match name {
                    "EXCHANGE_ID" => Some(execution_venue.to_string()),
                    "INSTRUMENT_PRODUCT_IDS" => Some(product_id.to_string()),
                    _ => None,
                },
            )
            .unwrap();

            assert_eq!(runtime.enabled_exchanges, expected);
        }
    }

    #[test]
    fn environment_enabled_list_enforces_mismatch_and_accepts_multi_source_membership() {
        let mismatch = std::collections::HashMap::from([
            ("EXCHANGE_ENABLED", "bybit"),
            ("EXCHANGE_ID", "binance"),
        ]);
        let error =
            LiveRuntime::prepare_with_lookup(options(None, Some("btcusdt".to_string())), |name| {
                mismatch.get(name).map(|value| (*value).to_string())
            })
            .err()
            .unwrap();
        assert_eq!(
            error.to_string(),
            "EXCHANGE_ID must be included in EXCHANGE_ENABLED: binance"
        );

        let accepted = std::collections::HashMap::from([
            ("EXCHANGE_ENABLED", "bybit,backpack"),
            ("EXCHANGE_ID", "backpack"),
            ("EXCHANGE_API_KEY", "backpack-key"),
            ("EXCHANGE_SECRET", "backpack-secret"),
            ("INSTRUMENT_PRODUCT_IDS", "BACKPACK:BTC_USDC-PERP"),
        ]);
        let runtime =
            LiveRuntime::prepare_with_lookup(options(None, Some("btcusdt".to_string())), |name| {
                accepted.get(name).map(|value| (*value).to_string())
            })
            .unwrap();
        assert_eq!(runtime.enabled_exchanges, vec!["bybit", "backpack"]);
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
            ("EXCHANGE_ID", " BINANCE "),
            ("BINANCE_API_KEY", "binance-key"),
            ("EXCHANGE_API_KEY", "backpack-key"),
            ("EXCHANGE_SECRET", "backpack-secret"),
            (
                "BACKPACK_MARKET_DATA_SYMBOLS",
                "SOL_USDC_PERP,BTC_USDC_PERP",
            ),
            (
                "INSTRUMENT_PRODUCT_IDS",
                "BINANCE:ETHUSDT-PERP,BINANCE:BTCUSDT-PERP",
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
    fn symbol_validation_precedes_credential_failure() {
        let values = std::collections::HashMap::from([
            ("EXCHANGE_ID", "backpack"),
            ("EXCHANGE_API_KEY", "backpack-key"),
        ]);
        let error = LiveRuntime::prepare_with_lookup(
            options(Some("backpack".to_string()), Some(" , ".to_string())),
            |name| values.get(name).map(|value| (*value).to_string()),
        )
        .err()
        .unwrap();

        assert_eq!(
            error.to_string(),
            "MARKET_DATA_SYMBOLS must contain at least one value"
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

    #[cfg(feature = "rithmic")]
    #[test]
    fn matching_rithmic_execution_venue_reaches_rithmic_configuration() {
        let error = LiveRuntime::prepare_with_lookup(
            options(Some("rithmic".to_string()), Some("mnqu6".to_string())),
            |name| (name == "EXCHANGE_ID").then(|| "RITHMIC".to_string()),
        )
        .err()
        .unwrap();

        assert_eq!(error.to_string(), "--rithmic-profile is required");
    }

    #[cfg(feature = "rithmic")]
    #[test]
    fn rithmic_configuration_precedes_generic_symbol_validation() {
        let error = LiveRuntime::prepare_with_lookup(
            options(Some("rithmic".into()), Some(" , ".into())),
            |name| (name == "EXCHANGE_ID").then(|| "rithmic".into()),
        )
        .err()
        .unwrap();

        assert_eq!(error.to_string(), "--rithmic-profile is required");
    }

    #[cfg(feature = "rithmic")]
    #[test]
    fn rithmic_mismatch_precedes_rithmic_configuration_lookup() {
        let error = LiveRuntime::prepare_with_lookup(
            options(Some("rithmic".to_string()), Some("mnqu6".to_string())),
            |name| match name {
                "EXCHANGE_ID" => Some("binance".to_string()),
                name if name.starts_with("RITHMIC_") => {
                    panic!("Rithmic configuration lookup must not run")
                }
                _ => None,
            },
        )
        .err()
        .unwrap();

        assert_eq!(
            error.to_string(),
            "EXCHANGE_ID must be included in EXCHANGE_ENABLED: binance"
        );
    }

    #[cfg(feature = "rithmic")]
    #[test]
    fn ccxt_coverage_rejection_precedes_rithmic_configuration_lookup() {
        let error = LiveRuntime::prepare_with_lookup(
            options(Some("binance,rithmic".into()), Some("BTCUSDT".into())),
            |name| match name {
                "EXCHANGE_ID" => Some("binance".into()),
                "INSTRUMENT_PRODUCT_IDS" => Some("BINANCE:ETHUSDT-PERP".into()),
                name if name.starts_with("RITHMIC_") => {
                    panic!("Rithmic configuration lookup must not precede CCXT coverage")
                }
                _ => None,
            },
        )
        .err()
        .unwrap();

        assert_eq!(
            error.to_string(),
            "INSTRUMENT_PRODUCT_IDS product is not covered by binance market data: BINANCE:ETHUSDT-PERP"
        );
    }
}
