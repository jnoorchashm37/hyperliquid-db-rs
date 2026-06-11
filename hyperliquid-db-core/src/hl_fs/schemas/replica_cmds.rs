use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::hl_fs::schemas::NODE_DATA_DATE_TIME_FORMAT;

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsRows {
    pub abci_block: ReplicaCmdsAbciBlock,
    pub resps:      ReplicaCmdsResps
}

impl ReplicaCmdsRows {
    pub fn block_time_unix_ms(&self) -> eyre::Result<u64> {
        Ok(NaiveDateTime::parse_from_str(&self.abci_block.time, NODE_DATA_DATE_TIME_FORMAT)?
            .and_utc()
            .timestamp_millis() as u64)
    }
}

impl<'de> Deserialize<'de> for ReplicaCmdsRows {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsRowsRaw::deserialize(deserializer)?;

        Ok(Self {
            abci_block: ReplicaCmdsAbciBlock {
                time:                  raw.abci_block.time,
                signed_action_bundles: raw.abci_block.signed_action_bundles,
                round:                 raw.abci_block.round,
                parent_round:          raw.abci_block.parent_round,
                hardfork:              raw.abci_block.hardfork,
                proposer:              raw.abci_block.proposer
            },
            resps:      raw.resps
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsAbciBlock {
    pub time:                  String,
    pub signed_action_bundles: Vec<ReplicaCmdsSignedActionBundle>,
    pub round:                 u64,
    pub parent_round:          u64,
    pub hardfork:              ReplicaCmdsHardfork,
    pub proposer:              String
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdsHardfork {
    pub version: u64,
    pub round:   u64
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsSignedActionBundle {
    pub hash:              String,
    pub broadcaster:       String,
    pub broadcaster_nonce: u64,
    pub signed_actions:    Vec<ReplicaCmdsSignedAction>
}

impl<'de> Deserialize<'de> for ReplicaCmdsSignedActionBundle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsSignedActionBundleRaw::deserialize(deserializer)?;

        let (hash, bundle) = (raw.0, raw.1);
        Ok(Self {
            hash,
            broadcaster: bundle.broadcaster,
            broadcaster_nonce: bundle.broadcaster_nonce,
            signed_actions: bundle.signed_actions
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCmdsSignedAction {
    pub signature:     ReplicaCmdsSignature,
    pub action:        ReplicaCmdsAction,
    pub nonce:         u64,
    pub vault_address: Option<String>,
    pub expires_after: Option<u64>,
    pub is_frontend:   Option<bool>
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdsSignature {
    pub r: String,
    pub s: String,
    pub v: u64
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReplicaCmdsAction {
    Order {
        orders:   Vec<ReplicaCmdsOrder>,
        grouping: ReplicaCmdsGrouping,
        builder:  Option<ReplicaCmdsBuilder>
    },
    Cancel {
        cancels: Vec<ReplicaCmdsCancel>
    },
    CancelByCloid {
        cancels: Vec<ReplicaCmdsCancelByCloid>
    },
    BatchModify {
        modifies: Vec<ReplicaCmdsModify>
    },
    Modify {
        oid:   ReplicaCmdsOid,
        order: ReplicaCmdsOrder
    },
    ScheduleCancel {
        time: Option<u64>
    },
    UpdateLeverage {
        asset:    u64,
        is_cross: bool,
        leverage: u64
    },
    UpdateIsolatedMargin {
        asset:  u64,
        is_buy: bool,
        ntli:   i64
    },
    TwapOrder {
        twap: ReplicaCmdsTwap
    },
    TwapCancel {
        a: u64,
        t: u64
    },
    UsdSend {
        amount:             f64,
        destination:        String,
        hyperliquid_chain:  String,
        signature_chain_id: String,
        time:               u64
    },
    SpotSend {
        amount:             f64,
        destination:        String,
        hyperliquid_chain:  String,
        signature_chain_id: String,
        time:               u64,
        token:              String
    },
    UsdClassTransfer {
        amount:             String,
        hyperliquid_chain:  String,
        nonce:              u64,
        signature_chain_id: String,
        to_perp:            bool
    },
    SubAccountTransfer {
        is_deposit:       bool,
        sub_account_user: String,
        usd:              u64
    },
    SubAccountSpotTransfer {
        amount:           f64,
        is_deposit:       bool,
        sub_account_user: String,
        token:            String
    },
    VaultTransfer {
        is_deposit:    bool,
        usd:           u64,
        vault_address: String
    },
    Withdraw3 {
        amount:             f64,
        destination:        String,
        hyperliquid_chain:  String,
        signature_chain_id: String,
        time:               u64
    },
    SendAsset {
        amount:             f64,
        destination:        String,
        destination_dex:    String,
        from_sub_account:   String,
        hyperliquid_chain:  String,
        nonce:              u64,
        signature_chain_id: String,
        source_dex:         String,
        token:              String
    },
    AgentSendAsset {
        amount:           f64,
        destination:      String,
        destination_dex:  String,
        from_sub_account: String,
        nonce:            u64,
        source_dex:       String,
        token:            String
    },
    SendToEvmWithData {
        address_encoding:      String,
        amount:                f64,
        data:                  String,
        destination_chain_id:  u64,
        destination_recipient: String,
        gas_limit:             u64,
        hyperliquid_chain:     String,
        nonce:                 u64,
        signature_chain_id:    String,
        source_dex:            String,
        token:                 String
    },
    BorrowLend {
        amount:    Option<f64>,
        operation: String,
        token:     u64
    },
    UserOutcome {
        merge_outcome:  Option<ReplicaCmdsMergeOutcome>,
        negate_outcome: Option<ReplicaCmdsNegateOutcome>,
        split_outcome:  Option<ReplicaCmdsSplitOutcome>
    },
    SpotUser {
        toggle_spot_dusting: ReplicaCmdsToggleSpotDusting
    },
    ClaimRewards,
    ApproveAgent {
        agent_address:      String,
        agent_name:         Option<String>,
        hyperliquid_chain:  String,
        nonce:              u64,
        signature_chain_id: String
    },
    ApproveBuilderFee {
        builder:            String,
        hyperliquid_chain:  String,
        max_fee_rate:       String,
        nonce:              u64,
        signature_chain_id: String
    },
    SetReferrer {
        code: String
    },
    RegisterReferrer {
        code: String
    },
    AgentSetAbstraction {
        abstraction: String
    },
    UserSetAbstraction {
        abstraction:        String,
        hyperliquid_chain:  String,
        nonce:              u64,
        signature_chain_id: String,
        user:               String
    },
    AgentEnableDexAbstraction,
    UserDexAbstraction {
        enabled:            bool,
        hyperliquid_chain:  String,
        nonce:              u64,
        signature_chain_id: String,
        user:               String
    },
    ReserveRequestWeight {
        weight: u64
    },
    EvmRawTx {
        data: String
    },
    MultiSig {
        payload:            ReplicaCmdsMultiSigPayload,
        signature_chain_id: String,
        signatures:         Vec<ReplicaCmdsSignature>
    },
    Noop,
    GossipPriorityBid {
        ip:      String,
        max_gas: u64,
        slot_id: u64
    },
    PerpDeploy {
        set_oracle:              Option<ReplicaCmdsPerpDeploySetOracle>,
        set_funding_multipliers: Option<Vec<ReplicaCmdsCoinPx>>
    },
    ValidatorL1UpdateReferenceOracle {
        external_perp_pxs: Vec<ReplicaCmdsCoinPx>,
        oracle_pxs:        Vec<ReplicaCmdsCoinPx>
    },
    L1ValidatorVoteBridgeDeposit {
        action: ReplicaCmdsBridgeDepositVote
    },
    VoteL1Hash {
        height:    u64,
        l1_hash:   String,
        signature: ReplicaCmdsSignature
    },
    VoteAppHash {
        app_hash:  String,
        height:    u64,
        signature: ReplicaCmdsSignature
    },
    #[serde(rename = "SetGlobalAction")]
    SetGlobalAction {
        external_perp_pxs: Vec<ReplicaCmdsCoinPx>,
        native_px:         f64,
        pxs:               Vec<ReplicaCmdsSetGlobalPx>,
        usdt_usdc_px:      f64
    },
    #[serde(rename = "VoteEthDepositAction")]
    VoteEthDepositAction {
        eth_id: ReplicaCmdsEthId,
        usd:    u64,
        user:   String
    },
    #[serde(rename = "VoteEthFinalizedWithdrawalAction")]
    VoteEthFinalizedWithdrawalAction {
        destination: String,
        eth_tx_hash: String,
        nonce:       u64,
        usd:         u64,
        user:        String
    },
    #[serde(rename = "NetChildVaultPositionsAction")]
    NetChildVaultPositionsAction {
        assets:                Vec<u64>,
        child_vault_addresses: Vec<String>
    },
    #[serde(rename = "ValidatorSignWithdrawalAction")]
    ValidatorSignWithdrawalAction {
        destination: String,
        nonce:       u64,
        signature:   ReplicaCmdsSignature,
        usd:         u64,
        user:        String
    }
}

impl<'de> Deserialize<'de> for ReplicaCmdsAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsActionRaw::deserialize(deserializer)?;

        match raw {
            _private::ReplicaCmdsActionRaw::Order { orders, grouping, builder } => {
                Ok(ReplicaCmdsAction::Order { orders, grouping, builder })
            }
            _private::ReplicaCmdsActionRaw::Cancel { cancels } => {
                Ok(ReplicaCmdsAction::Cancel { cancels })
            }
            _private::ReplicaCmdsActionRaw::CancelByCloid { cancels } => {
                Ok(ReplicaCmdsAction::CancelByCloid { cancels })
            }
            _private::ReplicaCmdsActionRaw::BatchModify { modifies } => {
                Ok(ReplicaCmdsAction::BatchModify { modifies })
            }
            _private::ReplicaCmdsActionRaw::Modify { oid, order } => {
                Ok(ReplicaCmdsAction::Modify { oid, order })
            }
            _private::ReplicaCmdsActionRaw::ScheduleCancel { time } => {
                Ok(ReplicaCmdsAction::ScheduleCancel { time })
            }
            _private::ReplicaCmdsActionRaw::UpdateLeverage { asset, is_cross, leverage } => {
                Ok(ReplicaCmdsAction::UpdateLeverage { asset, is_cross, leverage })
            }
            _private::ReplicaCmdsActionRaw::UpdateIsolatedMargin { asset, is_buy, ntli } => {
                Ok(ReplicaCmdsAction::UpdateIsolatedMargin { asset, is_buy, ntli })
            }
            _private::ReplicaCmdsActionRaw::TwapOrder { twap } => {
                Ok(ReplicaCmdsAction::TwapOrder { twap })
            }
            _private::ReplicaCmdsActionRaw::TwapCancel { a, t } => {
                Ok(ReplicaCmdsAction::TwapCancel { a, t })
            }
            _private::ReplicaCmdsActionRaw::UsdSend {
                amount,
                destination,
                hyperliquid_chain,
                signature_chain_id,
                time
            } => Ok(ReplicaCmdsAction::UsdSend {
                amount: parse_f64(amount)?,
                destination,
                hyperliquid_chain,
                signature_chain_id,
                time
            }),
            _private::ReplicaCmdsActionRaw::SpotSend {
                amount,
                destination,
                hyperliquid_chain,
                signature_chain_id,
                time,
                token
            } => Ok(ReplicaCmdsAction::SpotSend {
                amount: parse_f64(amount)?,
                destination,
                hyperliquid_chain,
                signature_chain_id,
                time,
                token
            }),
            _private::ReplicaCmdsActionRaw::UsdClassTransfer {
                amount,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                to_perp
            } => Ok(ReplicaCmdsAction::UsdClassTransfer {
                amount,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                to_perp
            }),
            _private::ReplicaCmdsActionRaw::SubAccountTransfer {
                is_deposit,
                sub_account_user,
                usd
            } => Ok(ReplicaCmdsAction::SubAccountTransfer { is_deposit, sub_account_user, usd }),
            _private::ReplicaCmdsActionRaw::SubAccountSpotTransfer {
                amount,
                is_deposit,
                sub_account_user,
                token
            } => Ok(ReplicaCmdsAction::SubAccountSpotTransfer {
                amount: parse_f64(amount)?,
                is_deposit,
                sub_account_user,
                token
            }),
            _private::ReplicaCmdsActionRaw::VaultTransfer { is_deposit, usd, vault_address } => {
                Ok(ReplicaCmdsAction::VaultTransfer { is_deposit, usd, vault_address })
            }
            _private::ReplicaCmdsActionRaw::Withdraw3 {
                amount,
                destination,
                hyperliquid_chain,
                signature_chain_id,
                time
            } => Ok(ReplicaCmdsAction::Withdraw3 {
                amount: parse_f64(amount)?,
                destination,
                hyperliquid_chain,
                signature_chain_id,
                time
            }),
            _private::ReplicaCmdsActionRaw::SendAsset {
                amount,
                destination,
                destination_dex,
                from_sub_account,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                source_dex,
                token
            } => Ok(ReplicaCmdsAction::SendAsset {
                amount: parse_f64(amount)?,
                destination,
                destination_dex,
                from_sub_account,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                source_dex,
                token
            }),
            _private::ReplicaCmdsActionRaw::AgentSendAsset {
                amount,
                destination,
                destination_dex,
                from_sub_account,
                nonce,
                source_dex,
                token
            } => Ok(ReplicaCmdsAction::AgentSendAsset {
                amount: parse_f64(amount)?,
                destination,
                destination_dex,
                from_sub_account,
                nonce,
                source_dex,
                token
            }),
            _private::ReplicaCmdsActionRaw::SendToEvmWithData {
                address_encoding,
                amount,
                data,
                destination_chain_id,
                destination_recipient,
                gas_limit,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                source_dex,
                token
            } => Ok(ReplicaCmdsAction::SendToEvmWithData {
                address_encoding,
                amount: parse_f64(amount)?,
                data,
                destination_chain_id,
                destination_recipient,
                gas_limit,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                source_dex,
                token
            }),
            _private::ReplicaCmdsActionRaw::BorrowLend { amount, operation, token } => {
                Ok(ReplicaCmdsAction::BorrowLend {
                    amount: amount.map(parse_f64::<D::Error>).transpose()?,
                    operation,
                    token
                })
            }
            _private::ReplicaCmdsActionRaw::UserOutcome {
                merge_outcome,
                negate_outcome,
                split_outcome
            } => {
                Ok(ReplicaCmdsAction::UserOutcome { merge_outcome, negate_outcome, split_outcome })
            }
            _private::ReplicaCmdsActionRaw::SpotUser { toggle_spot_dusting } => {
                Ok(ReplicaCmdsAction::SpotUser { toggle_spot_dusting })
            }
            _private::ReplicaCmdsActionRaw::ClaimRewards => Ok(ReplicaCmdsAction::ClaimRewards),
            _private::ReplicaCmdsActionRaw::ApproveAgent {
                agent_address,
                agent_name,
                hyperliquid_chain,
                nonce,
                signature_chain_id
            } => Ok(ReplicaCmdsAction::ApproveAgent {
                agent_address,
                agent_name,
                hyperliquid_chain,
                nonce,
                signature_chain_id
            }),
            _private::ReplicaCmdsActionRaw::ApproveBuilderFee {
                builder,
                hyperliquid_chain,
                max_fee_rate,
                nonce,
                signature_chain_id
            } => Ok(ReplicaCmdsAction::ApproveBuilderFee {
                builder,
                hyperliquid_chain,
                max_fee_rate,
                nonce,
                signature_chain_id
            }),
            _private::ReplicaCmdsActionRaw::SetReferrer { code } => {
                Ok(ReplicaCmdsAction::SetReferrer { code })
            }
            _private::ReplicaCmdsActionRaw::RegisterReferrer { code } => {
                Ok(ReplicaCmdsAction::RegisterReferrer { code })
            }
            _private::ReplicaCmdsActionRaw::AgentSetAbstraction { abstraction } => {
                Ok(ReplicaCmdsAction::AgentSetAbstraction { abstraction })
            }
            _private::ReplicaCmdsActionRaw::UserSetAbstraction {
                abstraction,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                user
            } => Ok(ReplicaCmdsAction::UserSetAbstraction {
                abstraction,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                user
            }),
            _private::ReplicaCmdsActionRaw::AgentEnableDexAbstraction => {
                Ok(ReplicaCmdsAction::AgentEnableDexAbstraction)
            }
            _private::ReplicaCmdsActionRaw::UserDexAbstraction {
                enabled,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                user
            } => Ok(ReplicaCmdsAction::UserDexAbstraction {
                enabled,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                user
            }),
            _private::ReplicaCmdsActionRaw::ReserveRequestWeight { weight } => {
                Ok(ReplicaCmdsAction::ReserveRequestWeight { weight })
            }
            _private::ReplicaCmdsActionRaw::EvmRawTx { data } => {
                Ok(ReplicaCmdsAction::EvmRawTx { data })
            }
            _private::ReplicaCmdsActionRaw::MultiSig {
                payload,
                signature_chain_id,
                signatures
            } => Ok(ReplicaCmdsAction::MultiSig { payload, signature_chain_id, signatures }),
            _private::ReplicaCmdsActionRaw::Noop => Ok(ReplicaCmdsAction::Noop),
            _private::ReplicaCmdsActionRaw::GossipPriorityBid { ip, max_gas, slot_id } => {
                Ok(ReplicaCmdsAction::GossipPriorityBid { ip, max_gas, slot_id })
            }
            _private::ReplicaCmdsActionRaw::PerpDeploy { set_oracle, set_funding_multipliers } => {
                Ok(ReplicaCmdsAction::PerpDeploy { set_oracle, set_funding_multipliers })
            }
            _private::ReplicaCmdsActionRaw::ValidatorL1UpdateReferenceOracle {
                external_perp_pxs,
                oracle_pxs
            } => Ok(ReplicaCmdsAction::ValidatorL1UpdateReferenceOracle {
                external_perp_pxs,
                oracle_pxs
            }),
            _private::ReplicaCmdsActionRaw::L1ValidatorVoteBridgeDeposit { action } => {
                Ok(ReplicaCmdsAction::L1ValidatorVoteBridgeDeposit { action })
            }
            _private::ReplicaCmdsActionRaw::VoteL1Hash { height, l1_hash, signature } => {
                Ok(ReplicaCmdsAction::VoteL1Hash { height, l1_hash, signature })
            }
            _private::ReplicaCmdsActionRaw::VoteAppHash { app_hash, height, signature } => {
                Ok(ReplicaCmdsAction::VoteAppHash { app_hash, height, signature })
            }
            _private::ReplicaCmdsActionRaw::SetGlobalAction {
                external_perp_pxs,
                native_px,
                pxs,
                usdt_usdc_px
            } => Ok(ReplicaCmdsAction::SetGlobalAction {
                external_perp_pxs,
                native_px: parse_f64(native_px)?,
                pxs,
                usdt_usdc_px: parse_f64(usdt_usdc_px)?
            }),
            _private::ReplicaCmdsActionRaw::VoteEthDepositAction { eth_id, usd, user } => {
                Ok(ReplicaCmdsAction::VoteEthDepositAction { eth_id, usd, user })
            }
            _private::ReplicaCmdsActionRaw::VoteEthFinalizedWithdrawalAction {
                destination,
                eth_tx_hash,
                nonce,
                usd,
                user
            } => Ok(ReplicaCmdsAction::VoteEthFinalizedWithdrawalAction {
                destination,
                eth_tx_hash,
                nonce,
                usd,
                user
            }),
            _private::ReplicaCmdsActionRaw::NetChildVaultPositionsAction {
                assets,
                child_vault_addresses
            } => Ok(ReplicaCmdsAction::NetChildVaultPositionsAction {
                assets,
                child_vault_addresses
            }),
            _private::ReplicaCmdsActionRaw::ValidatorSignWithdrawalAction {
                destination,
                nonce,
                signature,
                usd,
                user
            } => Ok(ReplicaCmdsAction::ValidatorSignWithdrawalAction {
                destination,
                nonce,
                signature,
                usd,
                user
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsOrder {
    pub a: u64,
    pub b: bool,
    pub p: f64,
    pub s: f64,
    pub r: bool,
    pub t: ReplicaCmdsOrderType,
    pub c: Option<String>
}

impl<'de> Deserialize<'de> for ReplicaCmdsOrder {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsOrderRaw::deserialize(deserializer)?;

        Ok(Self {
            a: raw.a,
            b: raw.b,
            p: parse_f64(raw.p)?,
            s: parse_f64(raw.s)?,
            r: raw.r,
            t: raw.t,
            c: raw.c
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReplicaCmdsOrderType {
    Limit { tif: String },
    Trigger { is_market: bool, tpsl: String, trigger_px: f64 }
}

impl<'de> Deserialize<'de> for ReplicaCmdsOrderType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsOrderTypeRaw::deserialize(deserializer)?;

        match raw {
            _private::ReplicaCmdsOrderTypeRaw::Limit { tif } => {
                Ok(ReplicaCmdsOrderType::Limit { tif })
            }
            _private::ReplicaCmdsOrderTypeRaw::Trigger { is_market, tpsl, trigger_px } => {
                Ok(ReplicaCmdsOrderType::Trigger {
                    is_market,
                    tpsl,
                    trigger_px: parse_f64(trigger_px)?
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReplicaCmdsGrouping {
    Named(String),
    P { p: u64 }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdsBuilder {
    pub b: String,
    pub f: u64
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdsCancel {
    pub a: u64,
    pub o: u64
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdsCancelByCloid {
    pub asset: u64,
    pub cloid: String
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdsModify {
    pub oid:   ReplicaCmdsOid,
    pub order: ReplicaCmdsOrder
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReplicaCmdsOid {
    Oid(u64),
    Cloid(String)
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsTwap {
    pub a: u64,
    pub b: bool,
    pub m: u64,
    pub r: bool,
    pub s: f64,
    pub t: bool
}

impl<'de> Deserialize<'de> for ReplicaCmdsTwap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsTwapRaw::deserialize(deserializer)?;

        Ok(Self { a: raw.a, b: raw.b, m: raw.m, r: raw.r, s: parse_f64(raw.s)?, t: raw.t })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsMergeOutcome {
    pub amount:  Option<f64>,
    pub outcome: u64
}

impl<'de> Deserialize<'de> for ReplicaCmdsMergeOutcome {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsMergeOutcomeRaw::deserialize(deserializer)?;

        Ok(Self {
            amount:  raw.amount.map(parse_f64::<D::Error>).transpose()?,
            outcome: raw.outcome
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsNegateOutcome {
    pub amount:   f64,
    pub outcome:  u64,
    pub question: u64
}

impl<'de> Deserialize<'de> for ReplicaCmdsNegateOutcome {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsNegateOutcomeRaw::deserialize(deserializer)?;

        Ok(Self { amount: parse_f64(raw.amount)?, outcome: raw.outcome, question: raw.question })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsSplitOutcome {
    pub amount:  f64,
    pub outcome: u64
}

impl<'de> Deserialize<'de> for ReplicaCmdsSplitOutcome {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsSplitOutcomeRaw::deserialize(deserializer)?;

        Ok(Self { amount: parse_f64(raw.amount)?, outcome: raw.outcome })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCmdsToggleSpotDusting {
    pub opt_out: bool
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCmdsMultiSigPayload {
    pub multi_sig_user: String,
    pub outer_signer:   String,
    pub action:         Box<ReplicaCmdsAction>
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCmdsPerpDeploySetOracle {
    pub dex:               String,
    pub oracle_pxs:        Vec<ReplicaCmdsCoinPx>,
    pub mark_pxs:          Vec<Vec<ReplicaCmdsCoinPx>>,
    pub external_perp_pxs: Vec<ReplicaCmdsCoinPx>
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsCoinPx {
    pub coin: String,
    pub px:   f64
}

impl<'de> Deserialize<'de> for ReplicaCmdsCoinPx {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsCoinPxRaw::deserialize(deserializer)?;

        Ok(Self { coin: raw.0, px: parse_f64(raw.1)? })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsSetGlobalPx {
    pub px_a: Option<f64>,
    pub px_b: f64
}

impl<'de> Deserialize<'de> for ReplicaCmdsSetGlobalPx {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsSetGlobalPxRaw::deserialize(deserializer)?;

        Ok(Self { px_a: raw.0.map(parse_f64::<D::Error>).transpose()?, px_b: parse_f64(raw.1)? })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCmdsBridgeDepositVote {
    pub eth_id: ReplicaCmdsEthId,
    pub usd:    u64,
    pub user:   String
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdsEthId {
    pub b: u64,
    pub e: String,
    pub h: String,
    pub i: u64,
    pub t: u64
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ReplicaCmdsResps {
    Full(Vec<ReplicaCmdsRespBundle>)
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct ReplicaCmdsRespBundle {
    pub hash:  String,
    pub resps: Vec<ReplicaCmdsUserResp>
}

impl<'de> Deserialize<'de> for ReplicaCmdsRespBundle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsRespBundleRaw::deserialize(deserializer)?;

        Ok(Self { hash: raw.0, resps: raw.1 })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdsUserResp {
    pub user: Option<String>,
    pub res:  ReplicaCmdsRes
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum ReplicaCmdsRes {
    Ok { response: ReplicaCmdsResponse },
    Err { response: String }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ReplicaCmdsResponse {
    Cancel { data: ReplicaCmdsStatusesData },
    Default,
    Order { data: ReplicaCmdsStatusesData },
    SetGlobal { data: Vec<String> },
    TwapCancel { data: ReplicaCmdsTwapData },
    TwapOrder { data: ReplicaCmdsTwapData },
    ValidatorVote { data: bool }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdsStatusesData {
    pub statuses: Vec<ReplicaCmdsOrderStatus>
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReplicaCmdsOrderStatus {
    Error(String),
    Filled { avg_px: f64, cloid: Option<String>, oid: u64, total_sz: f64 },
    Resting { cloid: Option<String>, oid: u64 },
    Success,
    WaitingForFill,
    WaitingForTrigger
}

impl<'de> Deserialize<'de> for ReplicaCmdsOrderStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::ReplicaCmdsOrderStatusRaw::deserialize(deserializer)?;

        match raw {
            _private::ReplicaCmdsOrderStatusRaw::Error(error) => {
                Ok(ReplicaCmdsOrderStatus::Error(error))
            }
            _private::ReplicaCmdsOrderStatusRaw::Filled { avg_px, cloid, oid, total_sz } => {
                Ok(ReplicaCmdsOrderStatus::Filled {
                    avg_px: parse_f64(avg_px)?,
                    cloid,
                    oid,
                    total_sz: parse_f64(total_sz)?
                })
            }
            _private::ReplicaCmdsOrderStatusRaw::Resting { cloid, oid } => {
                Ok(ReplicaCmdsOrderStatus::Resting { cloid, oid })
            }
            _private::ReplicaCmdsOrderStatusRaw::Success => Ok(ReplicaCmdsOrderStatus::Success),
            _private::ReplicaCmdsOrderStatusRaw::WaitingForFill => {
                Ok(ReplicaCmdsOrderStatus::WaitingForFill)
            }
            _private::ReplicaCmdsOrderStatusRaw::WaitingForTrigger => {
                Ok(ReplicaCmdsOrderStatus::WaitingForTrigger)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdsTwapData {
    pub status: ReplicaCmdsTwapStatus
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReplicaCmdsTwapStatus {
    Error(String),
    Running { twap_id: u64 },
    Success
}

fn parse_f64<E>(value: String) -> Result<f64, E>
where
    E: serde::de::Error
{
    value.parse().map_err(serde::de::Error::custom)
}

mod _private {
    use serde::{Deserialize, Serialize};

    use super::{
        ReplicaCmdsBridgeDepositVote, ReplicaCmdsBuilder, ReplicaCmdsCancel,
        ReplicaCmdsCancelByCloid, ReplicaCmdsCoinPx, ReplicaCmdsEthId, ReplicaCmdsGrouping,
        ReplicaCmdsHardfork, ReplicaCmdsMergeOutcome, ReplicaCmdsModify,
        ReplicaCmdsMultiSigPayload, ReplicaCmdsNegateOutcome, ReplicaCmdsOid, ReplicaCmdsOrder,
        ReplicaCmdsOrderType, ReplicaCmdsPerpDeploySetOracle, ReplicaCmdsResps,
        ReplicaCmdsSetGlobalPx, ReplicaCmdsSignature, ReplicaCmdsSignedAction,
        ReplicaCmdsSignedActionBundle, ReplicaCmdsSplitOutcome, ReplicaCmdsToggleSpotDusting,
        ReplicaCmdsTwap, ReplicaCmdsUserResp
    };

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsRowsRaw {
        pub abci_block: ReplicaCmdsAbciBlockRaw,
        pub resps:      ReplicaCmdsResps
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsAbciBlockRaw {
        pub time:                  String,
        pub signed_action_bundles: Vec<ReplicaCmdsSignedActionBundle>,
        pub round:                 u64,
        pub parent_round:          u64,
        pub hardfork:              ReplicaCmdsHardfork,
        pub proposer:              String
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsSignedActionBundleRaw(pub String, pub ReplicaCmdsActionBundleRaw);

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsActionBundleRaw {
        pub signed_actions:    Vec<ReplicaCmdsSignedAction>,
        pub broadcaster:       String,
        pub broadcaster_nonce: u64
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum ReplicaCmdsActionRaw {
        Order {
            orders:   Vec<ReplicaCmdsOrder>,
            grouping: ReplicaCmdsGrouping,
            builder:  Option<ReplicaCmdsBuilder>
        },
        Cancel {
            cancels: Vec<ReplicaCmdsCancel>
        },
        CancelByCloid {
            cancels: Vec<ReplicaCmdsCancelByCloid>
        },
        BatchModify {
            modifies: Vec<ReplicaCmdsModify>
        },
        Modify {
            oid:   ReplicaCmdsOid,
            order: ReplicaCmdsOrder
        },
        ScheduleCancel {
            time: Option<u64>
        },
        UpdateLeverage {
            asset:    u64,
            is_cross: bool,
            leverage: u64
        },
        UpdateIsolatedMargin {
            asset:  u64,
            is_buy: bool,
            ntli:   i64
        },
        TwapOrder {
            twap: ReplicaCmdsTwap
        },
        TwapCancel {
            a: u64,
            t: u64
        },
        UsdSend {
            amount:             String,
            destination:        String,
            hyperliquid_chain:  String,
            signature_chain_id: String,
            time:               u64
        },
        SpotSend {
            amount:             String,
            destination:        String,
            hyperliquid_chain:  String,
            signature_chain_id: String,
            time:               u64,
            token:              String
        },
        UsdClassTransfer {
            amount:             String,
            hyperliquid_chain:  String,
            nonce:              u64,
            signature_chain_id: String,
            to_perp:            bool
        },
        SubAccountTransfer {
            is_deposit:       bool,
            sub_account_user: String,
            usd:              u64
        },
        SubAccountSpotTransfer {
            amount:           String,
            is_deposit:       bool,
            sub_account_user: String,
            token:            String
        },
        VaultTransfer {
            is_deposit:    bool,
            usd:           u64,
            vault_address: String
        },
        Withdraw3 {
            amount:             String,
            destination:        String,
            hyperliquid_chain:  String,
            signature_chain_id: String,
            time:               u64
        },
        SendAsset {
            amount:             String,
            destination:        String,
            destination_dex:    String,
            from_sub_account:   String,
            hyperliquid_chain:  String,
            nonce:              u64,
            signature_chain_id: String,
            source_dex:         String,
            token:              String
        },
        AgentSendAsset {
            amount:           String,
            destination:      String,
            destination_dex:  String,
            from_sub_account: String,
            nonce:            u64,
            source_dex:       String,
            token:            String
        },
        SendToEvmWithData {
            address_encoding:      String,
            amount:                String,
            data:                  String,
            destination_chain_id:  u64,
            destination_recipient: String,
            gas_limit:             u64,
            hyperliquid_chain:     String,
            nonce:                 u64,
            signature_chain_id:    String,
            source_dex:            String,
            token:                 String
        },
        BorrowLend {
            amount:    Option<String>,
            operation: String,
            token:     u64
        },
        UserOutcome {
            merge_outcome:  Option<ReplicaCmdsMergeOutcome>,
            negate_outcome: Option<ReplicaCmdsNegateOutcome>,
            split_outcome:  Option<ReplicaCmdsSplitOutcome>
        },
        SpotUser {
            toggle_spot_dusting: ReplicaCmdsToggleSpotDusting
        },
        ClaimRewards,
        ApproveAgent {
            agent_address:      String,
            agent_name:         Option<String>,
            hyperliquid_chain:  String,
            nonce:              u64,
            signature_chain_id: String
        },
        ApproveBuilderFee {
            builder:            String,
            hyperliquid_chain:  String,
            max_fee_rate:       String,
            nonce:              u64,
            signature_chain_id: String
        },
        SetReferrer {
            code: String
        },
        RegisterReferrer {
            code: String
        },
        AgentSetAbstraction {
            abstraction: String
        },
        UserSetAbstraction {
            abstraction:        String,
            hyperliquid_chain:  String,
            nonce:              u64,
            signature_chain_id: String,
            user:               String
        },
        AgentEnableDexAbstraction,
        UserDexAbstraction {
            enabled:            bool,
            hyperliquid_chain:  String,
            nonce:              u64,
            signature_chain_id: String,
            user:               String
        },
        ReserveRequestWeight {
            weight: u64
        },
        EvmRawTx {
            data: String
        },
        MultiSig {
            payload:            ReplicaCmdsMultiSigPayload,
            signature_chain_id: String,
            signatures:         Vec<ReplicaCmdsSignature>
        },
        Noop,
        GossipPriorityBid {
            ip:      String,
            max_gas: u64,
            slot_id: u64
        },
        PerpDeploy {
            set_oracle:              Option<ReplicaCmdsPerpDeploySetOracle>,
            set_funding_multipliers: Option<Vec<ReplicaCmdsCoinPx>>
        },
        ValidatorL1UpdateReferenceOracle {
            external_perp_pxs: Vec<ReplicaCmdsCoinPx>,
            oracle_pxs:        Vec<ReplicaCmdsCoinPx>
        },
        L1ValidatorVoteBridgeDeposit {
            action: ReplicaCmdsBridgeDepositVote
        },
        VoteL1Hash {
            height:    u64,
            l1_hash:   String,
            signature: ReplicaCmdsSignature
        },
        VoteAppHash {
            app_hash:  String,
            height:    u64,
            signature: ReplicaCmdsSignature
        },
        #[serde(rename = "SetGlobalAction")]
        SetGlobalAction {
            external_perp_pxs: Vec<ReplicaCmdsCoinPx>,
            native_px:         String,
            pxs:               Vec<ReplicaCmdsSetGlobalPx>,
            usdt_usdc_px:      String
        },
        #[serde(rename = "VoteEthDepositAction")]
        VoteEthDepositAction {
            eth_id: ReplicaCmdsEthId,
            usd:    u64,
            user:   String
        },
        #[serde(rename = "VoteEthFinalizedWithdrawalAction")]
        VoteEthFinalizedWithdrawalAction {
            destination: String,
            eth_tx_hash: String,
            nonce:       u64,
            usd:         u64,
            user:        String
        },
        #[serde(rename = "NetChildVaultPositionsAction")]
        NetChildVaultPositionsAction {
            assets:                Vec<u64>,
            child_vault_addresses: Vec<String>
        },
        #[serde(rename = "ValidatorSignWithdrawalAction")]
        ValidatorSignWithdrawalAction {
            destination: String,
            nonce:       u64,
            signature:   ReplicaCmdsSignature,
            usd:         u64,
            user:        String
        }
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsOrderRaw {
        pub a: u64,
        pub b: bool,
        pub p: String,
        pub s: String,
        pub r: bool,
        pub t: ReplicaCmdsOrderType,
        pub c: Option<String>
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum ReplicaCmdsOrderTypeRaw {
        Limit { tif: String },
        Trigger { is_market: bool, tpsl: String, trigger_px: String }
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsTwapRaw {
        pub a: u64,
        pub b: bool,
        pub m: u64,
        pub r: bool,
        pub s: String,
        pub t: bool
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsMergeOutcomeRaw {
        pub amount:  Option<String>,
        pub outcome: u64
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsNegateOutcomeRaw {
        pub amount:   String,
        pub outcome:  u64,
        pub question: u64
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsSplitOutcomeRaw {
        pub amount:  String,
        pub outcome: u64
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsCoinPxRaw(pub String, pub String);

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsSetGlobalPxRaw(pub Option<String>, pub String);

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct ReplicaCmdsRespBundleRaw(pub String, pub Vec<ReplicaCmdsUserResp>);

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum ReplicaCmdsOrderStatusRaw {
        Error(String),
        Filled { avg_px: String, cloid: Option<String>, oid: u64, total_sz: String },
        Resting { cloid: Option<String>, oid: u64 },
        Success,
        WaitingForFill,
        WaitingForTrigger
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ReplicaCmdsAction, ReplicaCmdsGrouping, ReplicaCmdsOid, ReplicaCmdsOrderStatus,
        ReplicaCmdsOrderType, ReplicaCmdsRes, ReplicaCmdsResponse, ReplicaCmdsResps,
        ReplicaCmdsRows, ReplicaCmdsTwapStatus
    };

    #[test]
    fn deserializes_order_action_and_statuses_sample_shapes() {
        let row: ReplicaCmdsRows = serde_json::from_str(
            r#"{
                "abci_block":{
                    "time":"2026-06-11T17:27:48.584957873",
                    "signed_action_bundles":[
                        ["0xd8a0c8434c44bf6bc8f619a523091d0727ef29acba5f89078702167175b1935e",{
                            "signed_actions":[
                                {
                                    "signature":{"r":"0xb6fd0a84550f94c90ff19c33f51880849e94005cc69e498948cbe5a9cc12ba7","s":"0x6555258e4de8749a2151ca834ea67fa9bf44891012b71e51f55c49d2e5d94c48","v":28},
                                    "action":{"type":"order","orders":[{"a":110049,"b":false,"p":"91.545","s":"4.37","r":false,"t":{"limit":{"tif":"Ioc"}},"c":"0xbbd0a82e4ab3713bc3607bb9f369a91f"}],"grouping":"na"},
                                    "nonce":1781198704899
                                },
                                {
                                    "signature":{"r":"0x75ab253011428dcc6cee7778799c9ce45628dc37c900e0320aaed482721da4d8","s":"0x184f6b20599ad176c004e4c084febfa0274892212e5c13a043901c27cadd9b88","v":27},
                                    "vaultAddress":"0x4bcc092a156905788bf2a1a59e5968623957210d",
                                    "expiresAfter":1781198899999,
                                    "action":{"type":"batchModify","modifies":[{"oid":"0x0000000000000000226d6bd9d20320de","order":{"a":5,"b":true,"p":"65.045","s":"128.49","r":false,"t":{"limit":{"tif":"Ioc"}},"c":"0x0000000000000000226d6bd9d20320de"}},{"oid":465972521426,"order":{"a":5,"b":true,"p":"65.048","s":"189.39","r":true,"t":{"trigger":{"isMarket":true,"tpsl":"sl","triggerPx":"62836"}},"c":null}}]},
                                    "nonce":1781198704900
                                }
                            ],
                            "broadcaster":"0xc9ba414780c1b75e9806e3469300993fd2bfa883",
                            "broadcaster_nonce":1781198798932
                        }]
                    ],
                    "round":1326518535,
                    "parent_round":1326518534,
                    "hardfork":{"version":90,"round":1319594786},
                    "proposer":"0x5ac99df645f3414876c816caa18b2d234024b487"
                },
                "resps":{"Full":[
                    ["0xd8a0c8434c44bf6bc8f619a523091d0727ef29acba5f89078702167175b1935e",[
                        {"user":"0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00","res":{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":465972521426,"cloid":"0x19eb7b922ece43e00000000000000000"}}]}}}},
                        {"user":"0x4bcc092a156905788bf2a1a59e5968623957210d","res":{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Cannot modify canceled or filled order"},{"filled":{"totalSz":"0.129","avgPx":"385.58","oid":465972522923}}]}}}}
                    ]]
                ]}
            }"#
        )
        .unwrap();

        assert_eq!(row.abci_block.time, "2026-06-11T17:27:48.584957873");
        assert_eq!(row.abci_block.round, 1326518535);
        assert_eq!(row.abci_block.parent_round, 1326518534);
        assert_eq!(row.abci_block.hardfork.version, 90);
        assert_eq!(row.abci_block.proposer, "0x5ac99df645f3414876c816caa18b2d234024b487");
        assert_eq!(row.abci_block.signed_action_bundles.len(), 1);

        let bundle = &row.abci_block.signed_action_bundles[0];
        assert_eq!(
            bundle.hash,
            "0xd8a0c8434c44bf6bc8f619a523091d0727ef29acba5f89078702167175b1935e"
        );
        assert_eq!(bundle.broadcaster, "0xc9ba414780c1b75e9806e3469300993fd2bfa883");
        assert_eq!(bundle.broadcaster_nonce, 1781198798932);
        assert_eq!(bundle.signed_actions.len(), 2);

        let order_action = &bundle.signed_actions[0];
        assert_eq!(order_action.signature.v, 28);
        assert_eq!(order_action.nonce, 1781198704899);
        assert_eq!(order_action.vault_address, None);
        assert_eq!(order_action.expires_after, None);

        match &order_action.action {
            ReplicaCmdsAction::Order { orders, grouping, builder } => {
                assert_eq!(orders.len(), 1);
                assert_eq!(orders[0].a, 110049);
                assert!(!orders[0].b);
                assert!((orders[0].p - 91.545).abs() < f64::EPSILON);
                assert!((orders[0].s - 4.37).abs() < f64::EPSILON);
                assert_eq!(orders[0].t, ReplicaCmdsOrderType::Limit { tif: "Ioc".to_string() });
                assert_eq!(orders[0].c.as_deref(), Some("0xbbd0a82e4ab3713bc3607bb9f369a91f"));
                assert_eq!(grouping, &ReplicaCmdsGrouping::Named("na".to_string()));
                assert!(builder.is_none());
            }
            _ => panic!("expected order action")
        }

        let batch_modify_action = &bundle.signed_actions[1];
        assert_eq!(
            batch_modify_action.vault_address.as_deref(),
            Some("0x4bcc092a156905788bf2a1a59e5968623957210d")
        );
        assert_eq!(batch_modify_action.expires_after, Some(1781198899999));

        match &batch_modify_action.action {
            ReplicaCmdsAction::BatchModify { modifies } => {
                assert_eq!(modifies.len(), 2);
                assert_eq!(
                    modifies[0].oid,
                    ReplicaCmdsOid::Cloid("0x0000000000000000226d6bd9d20320de".to_string())
                );
                assert_eq!(modifies[1].oid, ReplicaCmdsOid::Oid(465972521426));
                match &modifies[1].order.t {
                    ReplicaCmdsOrderType::Trigger { is_market, tpsl, trigger_px } => {
                        assert!(is_market);
                        assert_eq!(tpsl, "sl");
                        assert!((trigger_px - 62836.0).abs() < f64::EPSILON);
                    }
                    _ => panic!("expected trigger order type")
                }
                assert_eq!(modifies[1].order.c, None);
            }
            _ => panic!("expected batch modify action")
        }

        let ReplicaCmdsResps::Full(resp_bundles) = &row.resps;
        assert_eq!(resp_bundles.len(), 1);
        assert_eq!(resp_bundles[0].hash, bundle.hash);
        assert_eq!(resp_bundles[0].resps.len(), 2);

        match &resp_bundles[0].resps[0].res {
            ReplicaCmdsRes::Ok { response: ReplicaCmdsResponse::Order { data } } => {
                assert_eq!(data.statuses.len(), 1);
                match &data.statuses[0] {
                    ReplicaCmdsOrderStatus::Resting { oid, cloid } => {
                        assert_eq!(*oid, 465972521426);
                        assert_eq!(cloid.as_deref(), Some("0x19eb7b922ece43e00000000000000000"));
                    }
                    _ => panic!("expected resting status")
                }
            }
            _ => panic!("expected ok order response")
        }

        match &resp_bundles[0].resps[1].res {
            ReplicaCmdsRes::Ok { response: ReplicaCmdsResponse::Order { data } } => {
                assert_eq!(data.statuses.len(), 2);
                assert_eq!(
                    data.statuses[0],
                    ReplicaCmdsOrderStatus::Error(
                        "Cannot modify canceled or filled order".to_string()
                    )
                );
                match &data.statuses[1] {
                    ReplicaCmdsOrderStatus::Filled { avg_px, cloid, oid, total_sz } => {
                        assert!((avg_px - 385.58).abs() < f64::EPSILON);
                        assert_eq!(*cloid, None);
                        assert_eq!(*oid, 465972522923);
                        assert!((total_sz - 0.129).abs() < f64::EPSILON);
                    }
                    _ => panic!("expected filled status")
                }
            }
            _ => panic!("expected ok order response")
        }
    }

    #[test]
    fn deserializes_cancel_actions_and_err_res_sample_shapes() {
        let row: ReplicaCmdsRows = serde_json::from_str(
            r#"{
                "abci_block":{
                    "time":"2026-06-11T17:27:49.951335540",
                    "signed_action_bundles":[
                        ["0x276f0cb3242485d7d0201ad0bc6b7ad3b1c812673cf30854f7f77802cacda688",{
                            "signed_actions":[
                                {
                                    "signature":{"r":"0x1","s":"0x2","v":27},
                                    "action":{"type":"cancel","cancels":[{"a":5,"o":465988686977}]},
                                    "nonce":1781198704901
                                },
                                {
                                    "signature":{"r":"0x3","s":"0x4","v":28},
                                    "action":{"type":"cancelByCloid","cancels":[{"asset":159,"cloid":"0xf6703236bb0acd72435dcfe8efdf8501"}]},
                                    "nonce":1781198704902,
                                    "isFrontend":true
                                },
                                {
                                    "signature":{"r":"0x5","s":"0x6","v":27},
                                    "action":{"type":"order","orders":[{"a":110037,"b":true,"p":"92.018","s":"2.21","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x00000000000000000000019eb6f2a8a2"}],"grouping":{"p":80000}},
                                    "nonce":1781198704903
                                },
                                {
                                    "signature":{"r":"0x7","s":"0x8","v":28},
                                    "action":{"type":"scheduleCancel","time":1781200007878},
                                    "nonce":1781198704904
                                },
                                {
                                    "signature":{"r":"0x9","s":"0xa","v":27},
                                    "action":{"type":"updateIsolatedMargin","asset":1,"isBuy":true,"ntli":-136986993412},
                                    "nonce":1781198704905
                                },
                                {
                                    "signature":{"r":"0xb","s":"0xc","v":28},
                                    "action":{"type":"noop"},
                                    "nonce":1781198704906
                                },
                                {
                                    "signature":{"r":"0xd","s":"0xe","v":27},
                                    "action":{"type":"usdClassTransfer","signatureChainId":"0x66eee","hyperliquidChain":"Mainnet","amount":"8 subaccount:0x4d76d9da7c68ed6e8f63a125f177f22781d5de79","toPerp":false,"nonce":1781199106056},
                                    "nonce":1781199106056
                                }
                            ],
                            "broadcaster":"0xffbb4dfc9455f0df2e973d7a371d8ad994264aa6",
                            "broadcaster_nonce":1781198798933
                        }]
                    ],
                    "round":1326518536,
                    "parent_round":1326518535,
                    "hardfork":{"version":90,"round":1319594786},
                    "proposer":"0x5ac99df645f3414876c816caa18b2d234024b487"
                },
                "resps":{"Full":[
                    ["0x276f0cb3242485d7d0201ad0bc6b7ad3b1c812673cf30854f7f77802cacda688",[
                        {"user":"0x31ca8395cf837de08b24da3f660e77761dfb974b","res":{"status":"ok","response":{"type":"cancel","data":{"statuses":["success",{"error":"Order was never placed, already canceled, or filled. asset=0"}]}}}},
                        {"user":null,"res":{"status":"err","response":"User or API Wallet 0xab9c34e99e77ec2974e76c56fde158a8ea1637be does not exist."}},
                        {"user":"0x7064c7abd02a8481a33620e485e75df41b8a5fa5","res":{"status":"ok","response":{"type":"default"}}},
                        {"user":"0x8b1c0722a978599bb254c2bdd75992fa605073f6","res":{"status":"ok","response":{"type":"setGlobal","data":[]}}},
                        {"user":"0x163014fed389c392a0a214d2c4a28a73ec804741","res":{"status":"ok","response":{"type":"validatorVote","data":true}}},
                        {"user":"0x033fb97dacc654b928de7129bf205f98eb15b1b1","res":{"status":"ok","response":{"type":"twapOrder","data":{"status":{"running":{"twapId":1924567}}}}}}
                    ]]
                ]}
            }"#
        )
        .unwrap();

        let bundle = &row.abci_block.signed_action_bundles[0];
        assert_eq!(bundle.signed_actions.len(), 7);

        match &bundle.signed_actions[0].action {
            ReplicaCmdsAction::Cancel { cancels } => {
                assert_eq!(cancels.len(), 1);
                assert_eq!(cancels[0].a, 5);
                assert_eq!(cancels[0].o, 465988686977);
            }
            _ => panic!("expected cancel action")
        }

        assert_eq!(bundle.signed_actions[1].is_frontend, Some(true));
        match &bundle.signed_actions[1].action {
            ReplicaCmdsAction::CancelByCloid { cancels } => {
                assert_eq!(cancels.len(), 1);
                assert_eq!(cancels[0].asset, 159);
                assert_eq!(cancels[0].cloid, "0xf6703236bb0acd72435dcfe8efdf8501");
            }
            _ => panic!("expected cancel by cloid action")
        }

        match &bundle.signed_actions[2].action {
            ReplicaCmdsAction::Order { grouping, .. } => {
                assert_eq!(grouping, &ReplicaCmdsGrouping::P { p: 80000 });
            }
            _ => panic!("expected order action")
        }

        match &bundle.signed_actions[3].action {
            ReplicaCmdsAction::ScheduleCancel { time } => assert_eq!(*time, Some(1781200007878)),
            _ => panic!("expected schedule cancel action")
        }

        match &bundle.signed_actions[4].action {
            ReplicaCmdsAction::UpdateIsolatedMargin { asset, is_buy, ntli } => {
                assert_eq!(*asset, 1);
                assert!(is_buy);
                assert_eq!(*ntli, -136986993412);
            }
            _ => panic!("expected update isolated margin action")
        }

        assert!(matches!(bundle.signed_actions[5].action, ReplicaCmdsAction::Noop));

        match &bundle.signed_actions[6].action {
            ReplicaCmdsAction::UsdClassTransfer { amount, to_perp, .. } => {
                assert_eq!(amount, "8 subaccount:0x4d76d9da7c68ed6e8f63a125f177f22781d5de79");
                assert!(!to_perp);
            }
            _ => panic!("expected usd class transfer action")
        }

        let ReplicaCmdsResps::Full(resp_bundles) = &row.resps;
        let resps = &resp_bundles[0].resps;

        match &resps[0].res {
            ReplicaCmdsRes::Ok { response: ReplicaCmdsResponse::Cancel { data } } => {
                assert_eq!(data.statuses.len(), 2);
                assert_eq!(data.statuses[0], ReplicaCmdsOrderStatus::Success);
                assert!(matches!(data.statuses[1], ReplicaCmdsOrderStatus::Error(_)));
            }
            _ => panic!("expected ok cancel response")
        }

        assert_eq!(resps[1].user, None);
        match &resps[1].res {
            ReplicaCmdsRes::Err { response } => {
                assert_eq!(
                    response,
                    "User or API Wallet 0xab9c34e99e77ec2974e76c56fde158a8ea1637be does not exist."
                );
            }
            _ => panic!("expected err res")
        }

        assert!(matches!(
            resps[2].res,
            ReplicaCmdsRes::Ok { response: ReplicaCmdsResponse::Default }
        ));

        match &resps[3].res {
            ReplicaCmdsRes::Ok { response: ReplicaCmdsResponse::SetGlobal { data } } => {
                assert!(data.is_empty());
            }
            _ => panic!("expected ok set global response")
        }

        assert!(matches!(
            resps[4].res,
            ReplicaCmdsRes::Ok { response: ReplicaCmdsResponse::ValidatorVote { data: true } }
        ));

        match &resps[5].res {
            ReplicaCmdsRes::Ok { response: ReplicaCmdsResponse::TwapOrder { data } } => {
                assert_eq!(data.status, ReplicaCmdsTwapStatus::Running { twap_id: 1924567 });
            }
            _ => panic!("expected ok twap order response")
        }
    }

    #[test]
    fn deserializes_multi_sig_and_validator_action_sample_shapes() {
        let row: ReplicaCmdsRows = serde_json::from_str(
            r#"{
                "abci_block":{
                    "time":"2026-06-11T17:27:51.286481322",
                    "signed_action_bundles":[
                        ["0x4fcd996b2f4faf410a0d939c3b38d220aefbc9d535e2b8e6d685dd98430cb277",{
                            "signed_actions":[
                                {
                                    "signature":{"r":"0xcc5","s":"0x219","v":27},
                                    "action":{
                                        "type":"multiSig",
                                        "signatureChainId":"0x66eee",
                                        "signatures":[{"r":"0xcc5ab","s":"0x219f9","v":27}],
                                        "payload":{
                                            "multiSigUser":"0x1234567890545d1df9ee64b35fdd16966e08acec",
                                            "outerSigner":"0xd2d8bc14967d340c1eebde6eec6358200278da91",
                                            "action":{
                                                "type":"perpDeploy",
                                                "setOracle":{
                                                    "dex":"xyz",
                                                    "oraclePxs":[["xyz:AAPL","401.74"],["xyz:AMD","3.695"]],
                                                    "markPxs":[[["xyz:AAPL","401.74"]],[["xyz:AAPL","1.1511"]]],
                                                    "externalPerpPxs":[["xyz:AAPL","400.45"]]
                                                }
                                            }
                                        }
                                    },
                                    "nonce":1781198704907
                                },
                                {
                                    "signature":{"r":"0xd","s":"0xe","v":28},
                                    "action":{
                                        "type":"SetGlobalAction",
                                        "externalPerpPxs":[["AAVE","0.29025"]],
                                        "nativePx":"56.3765",
                                        "pxs":[[null,"0.37621"],["0.24525","6.8952"]],
                                        "usdtUsdcPx":"0.9990509016434387"
                                    },
                                    "nonce":1781198704908
                                },
                                {
                                    "signature":{"r":"0xf","s":"0x10","v":27},
                                    "action":{
                                        "type":"VoteEthDepositAction",
                                        "ethId":{"b":472446433,"e":"Deposit","h":"0xd5f7beb288e667d12eebfbac436329ede1f78876d8192a36fe4334aad508976c","i":0,"t":1},
                                        "usd":15147247,
                                        "user":"0xf032648dfd22c4d81588bd83e09ab89cfe816705"
                                    },
                                    "nonce":1781198704909
                                },
                                {
                                    "signature":{"r":"0x11","s":"0x12","v":28},
                                    "action":{
                                        "type":"voteAppHash",
                                        "appHash":"0xdebba56cf147ad8852c2d879ea3ae73cf82f1f6898b790a028370f1af08585b9",
                                        "height":1031640000,
                                        "signature":{"r":"0x451e","s":"0x6189","v":27}
                                    },
                                    "nonce":1781198704910
                                },
                                {
                                    "signature":{"r":"0x13","s":"0x14","v":27},
                                    "action":{"type":"userOutcome","mergeOutcome":{"amount":null,"outcome":240}},
                                    "nonce":1781198704911
                                },
                                {
                                    "signature":{"r":"0x15","s":"0x16","v":28},
                                    "action":{"type":"borrowLend","amount":"29.326138","operation":"supply","token":360},
                                    "nonce":1781198704912
                                }
                            ],
                            "broadcaster":"0x67e451964e0421f6e7d07be784f35c530667c2b3",
                            "broadcaster_nonce":1781198798934
                        }]
                    ],
                    "round":1326518537,
                    "parent_round":1326518536,
                    "hardfork":{"version":90,"round":1319594786},
                    "proposer":"0x5ac99df645f3414876c816caa18b2d234024b487"
                },
                "resps":{"Full":[
                    ["0x4fcd996b2f4faf410a0d939c3b38d220aefbc9d535e2b8e6d685dd98430cb277",[
                        {"user":"0x1234567890545d1df9ee64b35fdd16966e08acec","res":{"status":"ok","response":{"type":"setGlobal","data":[]}}},
                        {"user":"0x58e1b0e63c905d5982324fcd9108582623b8132e","res":{"status":"ok","response":{"type":"setGlobal","data":[]}}},
                        {"user":"0xda6816df552c3f9e0fb64979fb357800d690d79b","res":{"status":"ok","response":{"type":"default"}}},
                        {"user":"0xef2364db5db6f5539aa0bc111771a94ee47637fc","res":{"status":"ok","response":{"type":"validatorVote","data":true}}},
                        {"user":"0x1e532f0cb079be639b86073d78e0f4006cecfeac","res":{"status":"ok","response":{"type":"default"}}},
                        {"user":"0xfa08822df3d81a57230eae82960f12293d487ee7","res":{"status":"ok","response":{"type":"default"}}}
                    ]]
                ]}
            }"#
        )
        .unwrap();

        let bundle = &row.abci_block.signed_action_bundles[0];

        match &bundle.signed_actions[0].action {
            ReplicaCmdsAction::MultiSig { payload, signature_chain_id, signatures } => {
                assert_eq!(payload.multi_sig_user, "0x1234567890545d1df9ee64b35fdd16966e08acec");
                assert_eq!(payload.outer_signer, "0xd2d8bc14967d340c1eebde6eec6358200278da91");
                assert_eq!(signature_chain_id, "0x66eee");
                assert_eq!(signatures.len(), 1);

                match payload.action.as_ref() {
                    ReplicaCmdsAction::PerpDeploy { set_oracle, set_funding_multipliers } => {
                        let oracle = set_oracle.as_ref().unwrap();
                        assert_eq!(oracle.dex, "xyz");
                        assert_eq!(oracle.oracle_pxs.len(), 2);
                        assert_eq!(oracle.oracle_pxs[0].coin, "xyz:AAPL");
                        assert!((oracle.oracle_pxs[0].px - 401.74).abs() < f64::EPSILON);
                        assert_eq!(oracle.mark_pxs.len(), 2);
                        assert_eq!(oracle.mark_pxs[0].len(), 1);
                        assert!((oracle.mark_pxs[1][0].px - 1.1511).abs() < f64::EPSILON);
                        assert_eq!(oracle.external_perp_pxs.len(), 1);
                        assert!(set_funding_multipliers.is_none());
                    }
                    _ => panic!("expected perp deploy action in multi sig payload")
                }
            }
            _ => panic!("expected multi sig action")
        }

        match &bundle.signed_actions[1].action {
            ReplicaCmdsAction::SetGlobalAction {
                external_perp_pxs,
                native_px,
                pxs,
                usdt_usdc_px
            } => {
                assert_eq!(external_perp_pxs.len(), 1);
                assert_eq!(external_perp_pxs[0].coin, "AAVE");
                assert!((native_px - 56.3765).abs() < f64::EPSILON);
                assert_eq!(pxs.len(), 2);
                assert_eq!(pxs[0].px_a, None);
                assert!((pxs[0].px_b - 0.37621).abs() < f64::EPSILON);
                assert!((pxs[1].px_a.unwrap() - 0.24525).abs() < f64::EPSILON);
                assert!((usdt_usdc_px - 0.9990509016434387).abs() < f64::EPSILON);
            }
            _ => panic!("expected set global action")
        }

        match &bundle.signed_actions[2].action {
            ReplicaCmdsAction::VoteEthDepositAction { eth_id, usd, user } => {
                assert_eq!(eth_id.b, 472446433);
                assert_eq!(eth_id.e, "Deposit");
                assert_eq!(eth_id.i, 0);
                assert_eq!(eth_id.t, 1);
                assert_eq!(*usd, 15147247);
                assert_eq!(user, "0xf032648dfd22c4d81588bd83e09ab89cfe816705");
            }
            _ => panic!("expected vote eth deposit action")
        }

        match &bundle.signed_actions[3].action {
            ReplicaCmdsAction::VoteAppHash { app_hash, height, signature } => {
                assert_eq!(
                    app_hash,
                    "0xdebba56cf147ad8852c2d879ea3ae73cf82f1f6898b790a028370f1af08585b9"
                );
                assert_eq!(*height, 1031640000);
                assert_eq!(signature.v, 27);
            }
            _ => panic!("expected vote app hash action")
        }

        match &bundle.signed_actions[4].action {
            ReplicaCmdsAction::UserOutcome { merge_outcome, negate_outcome, split_outcome } => {
                let merge = merge_outcome.as_ref().unwrap();
                assert_eq!(merge.amount, None);
                assert_eq!(merge.outcome, 240);
                assert!(negate_outcome.is_none());
                assert!(split_outcome.is_none());
            }
            _ => panic!("expected user outcome action")
        }

        match &bundle.signed_actions[5].action {
            ReplicaCmdsAction::BorrowLend { amount, operation, token } => {
                assert!((amount.unwrap() - 29.326138).abs() < f64::EPSILON);
                assert_eq!(operation, "supply");
                assert_eq!(*token, 360);
            }
            _ => panic!("expected borrow lend action")
        }
    }

    #[test]
    fn parses_timestrings_to_unix_timestamps() {
        let row: ReplicaCmdsRows = serde_json::from_str(
            r#"{
                "abci_block":{
                    "time":"2026-06-11T17:27:48.584957873",
                    "signed_action_bundles":[],
                    "round":1326518535,
                    "parent_round":1326518534,
                    "hardfork":{"version":90,"round":1319594786},
                    "proposer":"0x5ac99df645f3414876c816caa18b2d234024b487"
                },
                "resps":{"Full":[]}
            }"#
        )
        .unwrap();

        assert_eq!(row.block_time_unix_ms().unwrap(), 1_781_198_868_584);
    }
}
