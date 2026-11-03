#[derive(Clone, Copy)]
pub(super) enum FieldKind {
    Integer(&'static str),
    Enum(&'static str),
    Boolean(&'static str),
    Float(&'static str),
    Identity(&'static str),
    SafeText(&'static str),
}

pub(super) fn field(template: i32, number: u32) -> Option<FieldKind> {
    use FieldKind::{Float as F, Identity as I, Integer as N, SafeText as S};
    let new_order_fields = [
        112004, 110306, 149247, 112003, 112005, 112008, 154710, 157063, 157064, 157065, 154487,
        154549, 157085, 157086, 154488, 154453, 154454, 153632,
    ];
    let modify_order_fields = [
        112004, 110306, 149247, 112008, 154710, 157063, 157064, 154453, 154454, 153632,
    ];
    let bracket_order_fields = [
        112004, 110306, 149247, 112003, 112005, 112008, 154710, 157087, 157170, 157172, 154457,
        154456, 154459, 154458, 157124, 157062, 157151, 154857, 157145, 157146, 157147, 157148,
        157149, 154487, 154549, 157085, 157086, 154488, 154453, 154454, 153632,
    ];
    let rithmic_order_update_fields = [
        153625, 110121, 110100, 110101, 112004, 112027, 112003, 112005, 112008, 154770, 154710,
        157087, 154111, 154112, 110306, 149247, 110322,
    ];
    let exchange_order_update_fields = [
        153625, 110121, 110100, 110101, 112004, 112003, 112005, 112008, 154770, 154710, 157087,
        154111, 154112, 110306, 149247, 110322,
    ];
    let account_pnl_fields = [
        110121, 154041, 154042, 154037, 154038, 154260, 154261, 156962, 156964, 156967, 150100,
        150101,
    ];
    let listed = match template {
        351 => {
            [
                154119, 110300, 154497, 110358, 154013, 154014, 131003, 154008, 149238,
            ]
            .contains(&number)
                || rithmic_order_update_fields.contains(&number)
        }
        352 => {
            [
                154119, 110300, 154497, 110358, 154013, 154014, 131003, 154008, 149238, 153647,
            ]
            .contains(&number)
                || exchange_order_update_fields.contains(&number)
        }
        312 => {
            [154119, 154013, 154014, 154008].contains(&number)
                || [110100, 110101].contains(&number)
                || new_order_fields.contains(&number)
        }
        314 => {
            [154013, 154014, 154008].contains(&number)
                || [110100, 110101].contains(&number)
                || modify_order_fields.contains(&number)
        }
        330 => {
            [154119, 154013, 154014, 154008].contains(&number)
                || [110100, 110101].contains(&number)
                || bracket_order_fields.contains(&number)
        }
        316 => [154013, 154014, 154008, 154710].contains(&number),
        3504 => {
            [154013, 154014, 154008].contains(&number) || [110100, 110101, 154710].contains(&number)
        }
        308 => [154008].contains(&number),
        320 => [154008].contains(&number),
        402 => [154008].contains(&number),
        // Template 403's licensed snapshot response has no reviewed payload
        // fields beyond its envelope. Never reuse ORDER/PNL field numbers.
        // Response templates have no content allowlist until their individual
        // licensed declarations are available for review. In particular, do
        // not inherit request/notification symbol or identity fields here.
        313 | 315 | 317 | 321 | 331 | 3505 => false,
        450 => {
            [
                154013, 154014, 154008, 100749, 154434, 157954, 157955, 157956, 157957, 157958,
            ]
            .contains(&number)
                || account_pnl_fields.contains(&number)
        }
        451 => [154013, 154014, 154008].contains(&number) || account_pnl_fields.contains(&number),
        _ => false,
    };
    if !listed {
        return None;
    }
    Some(match number {
        154119 => I("user_tag"),
        110300 => I("basket_id"),
        154497 => I("original_basket_id"),
        110358 => I("linked_basket_ids"),
        154013 => I("fcm_id"),
        154014 => I("ib_id"),
        131003 => I("user_id"),
        154008 => I("account_id"),
        149238 => I("exchange_order_id"),
        153647 => I("tp_exchange_order_id"),
        110100 => S("symbol"),
        110101 => S("exchange"),
        100749 if matches!(template, 450 | 451) => S("product_code"),
        153625 => FieldKind::Enum("notify_type"),
        110121 => FieldKind::Boolean("is_snapshot"),
        112004 => N("quantity"),
        112027 => N("quan_release_pending"),
        112003 => FieldKind::Enum("transaction_type"),
        112005 => FieldKind::Enum("duration"),
        112008 => FieldKind::Enum("price_type"),
        154453 => FieldKind::Enum("if_touched_condition"),
        154454 => FieldKind::Enum("if_touched_price_field"),
        154770 => FieldKind::Enum("orig_price_type"),
        154710 => FieldKind::Enum("manual_or_auto"),
        157087 => FieldKind::Enum("bracket_type"),
        157170 => N("break_even_ticks"),
        157172 => N("break_even_trigger_ticks"),
        154457 => N("target_quantity"),
        154456 => N("target_ticks"),
        154459 => N("stop_quantity"),
        154458 => N("stop_ticks"),
        157124 => N("trailing_stop_trigger_ticks"),
        157062 => FieldKind::Boolean("trailing_stop_by_last_trade_price"),
        157151 => FieldKind::Boolean("target_market_order_if_touched"),
        154857 => FieldKind::Boolean("stop_market_on_reject"),
        157145 => N("target_market_at_ssboe"),
        157146 => N("target_market_at_usecs"),
        157147 => N("stop_market_at_ssboe"),
        157148 => N("stop_market_at_usecs"),
        157149 => N("target_market_order_after_secs"),
        157063 => FieldKind::Boolean("trailing_stop"),
        154111 => N("total_fill_size"),
        154112 => N("total_unfilled_size"),
        157064 => N("trail_by_ticks"),
        157065 => N("trail_by_price_id"),
        154487 => N("release_at_ssboe"),
        154549 => N("release_at_usecs"),
        157085 => N("cancel_at_ssboe"),
        157086 => N("cancel_at_usecs"),
        154041 => N("fill_buy_qty"),
        154042 => N("fill_sell_qty"),
        154037 => N("order_buy_qty"),
        154038 => N("order_sell_qty"),
        154260 => N("buy_qty"),
        154261 => N("sell_qty"),
        156962 => N("open_position_quantity"),
        156964 => N("closed_position_quantity"),
        156967 => N("net_quantity"),
        150100 => N("ssboe"),
        150101 => N("usecs"),
        110306 => F("price"),
        149247 => F("trigger_price"),
        110322 => F("avg_fill_price"),
        154434 => F("avg_open_fill_price"),
        157954 if template == 450 => F("day_open_pnl"),
        157955 if template == 450 => F("day_closed_pnl"),
        157956 if template == 450 => F("day_pnl"),
        157957 if template == 450 => F("day_open_pnl_offset"),
        157958 if template == 450 => F("day_closed_pnl_offset"),
        153632 => F("if_touched_price"),
        _ => return None,
    })
}
