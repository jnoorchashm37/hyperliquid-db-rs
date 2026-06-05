use std::collections::BTreeMap;

use crate::types::{L4BookDiff, L4BookUpdates, L4Order, L4OrderStatus};

pub fn coin_to_book_updates(
    order_statuses: Vec<L4OrderStatus>,
    book_diffs: Vec<L4BookDiff>,
    time: u64,
    height: u64
) -> Vec<L4BookUpdates> {
    let mut updates = BTreeMap::<String, L4BookUpdates>::new();

    for diff in book_diffs {
        updates
            .entry(diff.coin.clone())
            .or_insert_with(|| L4BookUpdates::new(time, height))
            .book_diffs
            .push(diff);
    }

    for status in order_statuses {
        updates
            .entry(status.order.coin.clone())
            .or_insert_with(|| L4BookUpdates::new(time, height))
            .order_statuses
            .push(status);
    }

    updates.into_values().collect()
}

pub fn convert_trigger(order: &mut L4Order, status_time: u64) {
    if order.is_trigger {
        order.trigger_px = 0.0;
        order.trigger_condition = "Triggered".to_string();
        order.is_trigger = false;
        order.timestamp = status_time;
        order.tif = Some("Gtc".to_string());
    }
}
