use std::{env, path::PathBuf};

const RITHMIC_PROTOS: &[&str] = &[
    "request_rithmic_system_info.proto",
    "response_rithmic_system_info.proto",
    "request_login.proto",
    "response_login.proto",
    "request_logout.proto",
    "response_logout.proto",
    "request_heartbeat.proto",
    "response_heartbeat.proto",
    "reject.proto",
    "forced_logout.proto",
    "request_market_data_update.proto",
    "response_market_data_update.proto",
    "request_front_month_contract.proto",
    "response_front_month_contract.proto",
    "last_trade.proto",
    "best_bid_offer.proto",
    "request_time_bar_replay.proto",
    "response_time_bar_replay.proto",
    "time_bar.proto",
    "request_login_info.proto",
    "response_login_info.proto",
    "request_account_list.proto",
    "response_account_list.proto",
    "request_trade_routes.proto",
    "response_trade_routes.proto",
    "request_subscribe_for_order_updates.proto",
    "response_subscribe_for_order_updates.proto",
    "request_new_order.proto",
    "response_new_order.proto",
    "request_bracket_order.proto",
    "response_bracket_order.proto",
    "request_modify_order.proto",
    "response_modify_order.proto",
    "request_cancel_order.proto",
    "response_cancel_order.proto",
    "request_exit_position.proto",
    "response_exit_position.proto",
    "request_show_orders.proto",
    "response_show_orders.proto",
    "request_show_order_history.proto",
    "response_show_order_history.proto",
    "rithmic_order_notification.proto",
    "request_show_fill_history.proto",
    "response_show_fill_history.proto",
    "exchange_order_notification.proto",
    "request_pnl_position_snapshot.proto",
    "response_pnl_position_snapshot.proto",
    "instrument_pnl_position_update.proto",
    "account_pnl_position_update.proto",
];

fn main() {
    println!("cargo:rerun-if-env-changed=RITHMIC_PROTO_DIR");

    if env::var_os("CARGO_FEATURE_RITHMIC").is_none() {
        return;
    }

    let proto_dir = env::var_os("RITHMIC_PROTO_DIR")
        .map(PathBuf::from)
        .filter(|path| path.is_dir())
        .expect("RITHMIC_PROTO_DIR must point to the local Rithmic proto directory");
    let protos: Vec<_> = RITHMIC_PROTOS
        .iter()
        .map(|name| proto_dir.join(name))
        .collect();

    for proto in &protos {
        if !proto.is_file() {
            panic!("missing required Rithmic proto: {}", proto.display());
        }
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("vendored protoc is unavailable for this platform");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config
        .compile_protos(&protos, &[proto_dir])
        .expect("failed to compile local Rithmic protos");
}
