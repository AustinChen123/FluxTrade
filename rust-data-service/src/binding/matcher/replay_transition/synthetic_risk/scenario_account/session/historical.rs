//! Strict historical-node transport into the already configured scenario owner.
use super::super::historical::{BarPair, NodeInput, OrderKind, WorkingOrderMeta};
use super::*;

#[cfg(test)]
mod tests;

const HISTORICAL_NODE_SCHEMA: &str = "historical_node_v1";

fn decimal4(value: &wire::Json) -> Result<[Decimal; 4], Fault> {
    let rows = value.object(&["open", "high", "low", "close"], &[])?;
    Ok([
        rows["open"].decimal()?,
        rows["high"].decimal()?,
        rows["low"].decimal()?,
        rows["close"].decimal()?,
    ])
}

fn parse_bar_pair(
    value: &wire::Json,
    product: &Product,
    bar_open_ms: i64,
) -> Result<BarPair, Fault> {
    let rows = value.object(&["product_id", "trade", "mark"], &[])?;
    if rows["product_id"].id()? != product_id(product) {
        return Err("INVALID_SCHEMA");
    }
    let trade = rows["trade"].object(
        &[
            "open",
            "high",
            "low",
            "close",
            "volume_contracts",
            "confirmed",
            "source_row_hash",
        ],
        &[],
    )?;
    let mark = rows["mark"].object(
        &[
            "open",
            "high",
            "low",
            "close",
            "confirmed",
            "source_row_hash",
        ],
        &[],
    )?;
    let confirmed = trade["confirmed"].boolean()? && mark["confirmed"].boolean()?;
    if !confirmed {
        return Err("INVALID_HISTORICAL_BAR");
    }
    Ok(BarPair {
        product: product.clone(),
        bar_open_ms,
        bar_duration_ms: 60_000,
        trade_ohlc: decimal4(&wire::fields(
            ["open", "high", "low", "close"]
                .into_iter()
                .map(|field| (field, trade[field].clone()))
                .collect(),
        ))?,
        mark_ohlc: decimal4(&wire::fields(
            ["open", "high", "low", "close"]
                .into_iter()
                .map(|field| (field, mark[field].clone()))
                .collect(),
        ))?,
        volume_contracts: trade["volume_contracts"].decimal()?,
        confirmed,
        trade_source_row_hash: trade["source_row_hash"].hash()?,
        mark_source_row_hash: mark["source_row_hash"].hash()?,
    })
}

fn parse_order(value: &wire::Json) -> Result<WorkingOrderMeta, Fault> {
    let rows = value.object(
        &[
            "order_id",
            "product_id",
            "order_version",
            "status",
            "remaining_quantity_contracts",
            "accepted_at",
            "accepted_source_sequence",
            "order_kind",
            "side",
            "limit_price",
            "risk_cancel_pending",
        ],
        &[],
    )?;
    let kind = match rows["order_kind"].text()? {
        "LIMIT" => OrderKind::Limit,
        "MARKET" => OrderKind::Market,
        _ => return Err("INVALID_SCHEMA"),
    };
    let limit_price = match &rows["limit_price"] {
        wire::Json::Null => None,
        value => Some(value.decimal()?),
    };
    Ok(WorkingOrderMeta {
        order_id: rows["order_id"].id()?,
        product: Product(rows["product_id"].id()?.into()),
        order_version: u64::try_from(rows["order_version"].integer(true)?)
            .map_err(|_| "INVALID_SCHEMA")?,
        status: rows["status"].text()?.into(),
        remaining: rows["remaining_quantity_contracts"].decimal()?,
        accepted_at: rows["accepted_at"].integer(true)?,
        accepted_source_sequence: rows["accepted_source_sequence"].integer(true)?,
        kind,
        side: rows["side"].side()?,
        limit_price,
        risk_cancel_pending: rows["risk_cancel_pending"].boolean()?,
    })
}

fn decode_node(request: &str, owner: &ScenarioAccount) -> Result<NodeInput, Fault> {
    let value = wire::decode(request)?;
    let rows = value.object(
        &[
            "schema_version",
            "model_id",
            "model_version",
            "run_contract_hash",
            "bar_open_ms",
            "bar_duration_ms",
            "step_index",
            "market_slippage_bps",
            "bars",
            "working_orders",
        ],
        &[],
    )?;
    if rows["schema_version"].text()? != HISTORICAL_NODE_SCHEMA {
        return Err("INVALID_SCHEMA");
    }
    let (scenario, _) = owner.btc_context()?;
    if scenario.configured.is_none() {
        return Err("INVALID_SCHEMA");
    }
    let model_id = rows["model_id"].text()?.to_owned();
    let model_version = rows["model_version"].text()?.to_owned();
    super::super::historical::Model::parse(&model_id, &model_version)?;
    let bar_open_ms = rows["bar_open_ms"].integer(true)?;
    let bar_duration_ms = rows["bar_duration_ms"].integer(true)?;
    if bar_duration_ms != 60_000 {
        return Err("INVALID_HISTORICAL_BAR");
    }
    let step_index =
        u8::try_from(rows["step_index"].integer(true)?).map_err(|_| "INVALID_SCHEMA")?;
    if step_index > 3 {
        return Err("INVALID_HISTORICAL_INPUT");
    }
    let products = scenario.products();
    let bar_rows = rows["bars"].array()?;
    if bar_rows.len() != products.len() {
        return Err("INVALID_SCHEMA");
    }
    let bars = bar_rows
        .iter()
        .zip(&products)
        .map(|(bar, product)| parse_bar_pair(bar, product, bar_open_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let working_orders = rows["working_orders"]
        .array()?
        .iter()
        .map(parse_order)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NodeInput {
        model_id,
        model_version,
        run_contract_hash: rows["run_contract_hash"].hash()?,
        step_index,
        market_slippage_bps: rows["market_slippage_bps"].decimal()?,
        bars,
        working_orders,
    })
}

fn encode_result(
    result: &super::super::historical::StepResult,
    evidence: wire::Json,
) -> Result<String, Fault> {
    let products = result
        .products
        .iter()
        .map(|product| {
            let fills = product
                .fills
                .iter()
                .map(|fill| {
                    Ok(wire::fields(vec![
                        ("order_id", wire::string(&fill.order_id)),
                        ("quantity_contracts", wire::decimal(fill.quantity)),
                        ("price", wire::decimal(fill.price)),
                        ("execution_id", wire::hash(fill.execution_id)),
                        ("source_event_id", wire::string(&fill.source_event_id)),
                        ("occurrence_index", wire::number(fill.occurrence_index)?),
                    ]))
                })
                .collect::<Result<Vec<_>, Fault>>()?;
            Ok(wire::fields(vec![
                ("product_id", wire::string(&product.product_id)),
                ("capacity", wire::decimal(product.capacity)),
                ("discarded_volume", wire::decimal(product.discarded_volume)),
                ("fills", wire::Json::Array(fills)),
            ]))
        })
        .collect::<Result<Vec<_>, Fault>>()?;
    wire::fields(vec![
        ("schema_version", wire::string("historical_node_result_v1")),
        ("raw_time_ms", wire::number(result.raw_time_ms)?),
        ("effective_at", wire::number(result.effective_at)?),
        ("products", wire::Json::Array(products)),
        ("owner_evidence", evidence),
    ])
    .canonical()
}

fn encode_working_orders(
    orders: &[super::super::historical::WorkingOrderMeta],
) -> Result<String, Fault> {
    let rows = orders
        .iter()
        .map(|order| {
            Ok(wire::fields(vec![
                ("order_id", wire::string(&order.order_id)),
                ("product_id", wire::string(order.product.0.as_ref())),
                ("order_version", wire::number(order.order_version)?),
                ("status", wire::string(&order.status)),
                (
                    "remaining_quantity_contracts",
                    wire::decimal(order.remaining),
                ),
                ("accepted_at", wire::number(order.accepted_at)?),
                (
                    "accepted_source_sequence",
                    wire::number(order.accepted_source_sequence)?,
                ),
                ("order_kind", wire::string("LIMIT")),
                (
                    "side",
                    wire::string(if order.side == Side::Long {
                        "LONG"
                    } else {
                        "SHORT"
                    }),
                ),
                (
                    "limit_price",
                    wire::decimal(order.limit_price.ok_or("NATIVE_INVARIANT")?),
                ),
                (
                    "risk_cancel_pending",
                    wire::Json::Bool(order.risk_cancel_pending),
                ),
            ]))
        })
        .collect::<Result<Vec<_>, Fault>>()?;
    wire::Json::Array(rows).canonical()
}

impl Session {
    pub(super) fn historical_working_orders(&mut self) -> Reply {
        self.guarded(false, |session| {
            let orders = session
                .owner
                .historical_working_orders()
                .map_err(boundary)?;
            encode_working_orders(&orders).map_err(|_| BoundaryError::Invariant)
        })
    }

    pub(super) fn historical_market_step(&mut self, request: &str) -> Reply {
        self.guarded(true, |session| {
            session.historical_market_step_inner(request)
        })
    }

    fn historical_market_step_inner(&mut self, request: &str) -> Reply {
        let node = decode_node(request, &self.owner).map_err(boundary)?;
        let result = self.owner.historical_market_step(&node).map_err(boundary)?;
        let evidence = self
            .owner
            .inspect_state()
            .and_then(|inspection| inspection.wire_json())
            .map_err(|_| BoundaryError::Invariant)?;
        encode_result(&result, evidence).map_err(|_| BoundaryError::Invariant)
    }
}
