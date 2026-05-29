use crate::hl_fs::schemas::NodeFillsRow;

pub struct TradeDeriver {}

impl TradeDeriver {
    pub fn new() -> Self {
        Self {}
    }

    pub fn new_fill_data(&mut self, data: NodeFillsRow) {}
}
