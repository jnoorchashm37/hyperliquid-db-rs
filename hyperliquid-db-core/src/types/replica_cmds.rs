use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::hl_fs::schemas::*;

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmd {
    pub time:              u64,
    pub round:             u64,
    pub parent_round:      u64,
    pub proposer:          String,
    pub hash:              String,
    pub broadcaster:       String,
    pub broadcaster_nonce: u64,
    pub signature:         ReplicaCmdSignature,
    pub action:            ReplicaCmdAction,
    pub nonce:             u64,
    pub vault_address:     Option<String>,
    pub expires_after:     Option<u64>,
    pub is_frontend:       Option<bool>,
    pub user:              Option<String>,
    pub res:               Option<ReplicaCmdRes>
}

impl ReplicaCmd {
    pub fn from_row(time: u64, row: ReplicaCmdsRows) -> Vec<Self> {
        let ReplicaCmdsResps::Full(resp_bundles) = row.resps;
        let mut resps_by_hash = resp_bundles
            .into_iter()
            .map(|bundle| (bundle.hash, bundle.resps))
            .collect::<HashMap<_, _>>();

        let abci_block = row.abci_block;
        let mut cmds = Vec::new();
        for bundle in abci_block.signed_action_bundles {
            let mut resps = resps_by_hash
                .remove(&bundle.hash)
                .unwrap_or_default()
                .into_iter();

            for signed_action in bundle.signed_actions {
                let (user, res) = match resps.next() {
                    Some(resp) => (resp.user, Some(resp.res.into())),
                    None => (None, None)
                };

                cmds.push(Self {
                    time,
                    round: abci_block.round,
                    parent_round: abci_block.parent_round,
                    proposer: abci_block.proposer.clone(),
                    hash: bundle.hash.clone(),
                    broadcaster: bundle.broadcaster.clone(),
                    broadcaster_nonce: bundle.broadcaster_nonce,
                    signature: signed_action.signature.into(),
                    action: signed_action.action.into(),
                    nonce: signed_action.nonce,
                    vault_address: signed_action.vault_address,
                    expires_after: signed_action.expires_after,
                    is_frontend: signed_action.is_frontend,
                    user,
                    res
                });
            }
        }

        cmds
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdSignature {
    pub r: String,
    pub s: String,
    pub v: u64
}

impl From<ReplicaCmdsSignature> for ReplicaCmdSignature {
    fn from(value: ReplicaCmdsSignature) -> Self {
        Self { r: value.r, s: value.s, v: value.v }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReplicaCmdAction {
    Order {
        orders:   Vec<ReplicaCmdOrder>,
        grouping: ReplicaCmdGrouping,
        builder:  Option<ReplicaCmdBuilder>
    },
    Cancel {
        cancels: Vec<ReplicaCmdCancel>
    },
    CancelByCloid {
        cancels: Vec<ReplicaCmdCancelByCloid>
    },
    BatchModify {
        modifies: Vec<ReplicaCmdModify>
    },
    Modify {
        oid:   ReplicaCmdOid,
        order: ReplicaCmdOrder
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
        twap: ReplicaCmdTwap
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
        merge_outcome:  Option<ReplicaCmdMergeOutcome>,
        negate_outcome: Option<ReplicaCmdNegateOutcome>,
        split_outcome:  Option<ReplicaCmdSplitOutcome>
    },
    SpotUser {
        toggle_spot_dusting: ReplicaCmdToggleSpotDusting
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
        payload:            ReplicaCmdMultiSigPayload,
        signature_chain_id: String,
        signatures:         Vec<ReplicaCmdSignature>
    },
    Noop,
    GossipPriorityBid {
        ip:      String,
        max_gas: u64,
        slot_id: u64
    },
    PerpDeploy {
        set_oracle:              Option<ReplicaCmdPerpDeploySetOracle>,
        set_funding_multipliers: Option<Vec<ReplicaCmdCoinPx>>
    },
    ValidatorL1UpdateReferenceOracle {
        external_perp_pxs: Vec<ReplicaCmdCoinPx>,
        oracle_pxs:        Vec<ReplicaCmdCoinPx>
    },
    L1ValidatorVoteBridgeDeposit {
        action: ReplicaCmdBridgeDepositVote
    },
    VoteL1Hash {
        height:    u64,
        l1_hash:   String,
        signature: ReplicaCmdSignature
    },
    VoteAppHash {
        app_hash:  String,
        height:    u64,
        signature: ReplicaCmdSignature
    },
    #[serde(rename = "SetGlobalAction")]
    SetGlobalAction {
        external_perp_pxs: Vec<ReplicaCmdCoinPx>,
        native_px:         f64,
        pxs:               Vec<ReplicaCmdSetGlobalPx>,
        usdt_usdc_px:      f64
    },
    #[serde(rename = "VoteEthDepositAction")]
    VoteEthDepositAction {
        eth_id: ReplicaCmdEthId,
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
        signature:   ReplicaCmdSignature,
        usd:         u64,
        user:        String
    }
}

impl From<ReplicaCmdsAction> for ReplicaCmdAction {
    fn from(value: ReplicaCmdsAction) -> Self {
        match value {
            ReplicaCmdsAction::Order { orders, grouping, builder } => Self::Order {
                orders:   orders.into_iter().map(Into::into).collect(),
                grouping: grouping.into(),
                builder:  builder.map(Into::into)
            },
            ReplicaCmdsAction::Cancel { cancels } => {
                Self::Cancel { cancels: cancels.into_iter().map(Into::into).collect() }
            }
            ReplicaCmdsAction::CancelByCloid { cancels } => {
                Self::CancelByCloid { cancels: cancels.into_iter().map(Into::into).collect() }
            }
            ReplicaCmdsAction::BatchModify { modifies } => {
                Self::BatchModify { modifies: modifies.into_iter().map(Into::into).collect() }
            }
            ReplicaCmdsAction::Modify { oid, order } => {
                Self::Modify { oid: oid.into(), order: order.into() }
            }
            ReplicaCmdsAction::ScheduleCancel { time } => Self::ScheduleCancel { time },
            ReplicaCmdsAction::UpdateLeverage { asset, is_cross, leverage } => {
                Self::UpdateLeverage { asset, is_cross, leverage }
            }
            ReplicaCmdsAction::UpdateIsolatedMargin { asset, is_buy, ntli } => {
                Self::UpdateIsolatedMargin { asset, is_buy, ntli }
            }
            ReplicaCmdsAction::TwapOrder { twap } => Self::TwapOrder { twap: twap.into() },
            ReplicaCmdsAction::TwapCancel { a, t } => Self::TwapCancel { a, t },
            ReplicaCmdsAction::UsdSend {
                amount,
                destination,
                hyperliquid_chain,
                signature_chain_id,
                time
            } => Self::UsdSend { amount, destination, hyperliquid_chain, signature_chain_id, time },
            ReplicaCmdsAction::SpotSend {
                amount,
                destination,
                hyperliquid_chain,
                signature_chain_id,
                time,
                token
            } => Self::SpotSend {
                amount,
                destination,
                hyperliquid_chain,
                signature_chain_id,
                time,
                token
            },
            ReplicaCmdsAction::UsdClassTransfer {
                amount,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                to_perp
            } => Self::UsdClassTransfer {
                amount,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                to_perp
            },
            ReplicaCmdsAction::SubAccountTransfer { is_deposit, sub_account_user, usd } => {
                Self::SubAccountTransfer { is_deposit, sub_account_user, usd }
            }
            ReplicaCmdsAction::SubAccountSpotTransfer {
                amount,
                is_deposit,
                sub_account_user,
                token
            } => Self::SubAccountSpotTransfer { amount, is_deposit, sub_account_user, token },
            ReplicaCmdsAction::VaultTransfer { is_deposit, usd, vault_address } => {
                Self::VaultTransfer { is_deposit, usd, vault_address }
            }
            ReplicaCmdsAction::Withdraw3 {
                amount,
                destination,
                hyperliquid_chain,
                signature_chain_id,
                time
            } => {
                Self::Withdraw3 { amount, destination, hyperliquid_chain, signature_chain_id, time }
            }
            ReplicaCmdsAction::SendAsset {
                amount,
                destination,
                destination_dex,
                from_sub_account,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                source_dex,
                token
            } => Self::SendAsset {
                amount,
                destination,
                destination_dex,
                from_sub_account,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                source_dex,
                token
            },
            ReplicaCmdsAction::AgentSendAsset {
                amount,
                destination,
                destination_dex,
                from_sub_account,
                nonce,
                source_dex,
                token
            } => Self::AgentSendAsset {
                amount,
                destination,
                destination_dex,
                from_sub_account,
                nonce,
                source_dex,
                token
            },
            ReplicaCmdsAction::SendToEvmWithData {
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
            } => Self::SendToEvmWithData {
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
            },
            ReplicaCmdsAction::BorrowLend { amount, operation, token } => {
                Self::BorrowLend { amount, operation, token }
            }
            ReplicaCmdsAction::UserOutcome { merge_outcome, negate_outcome, split_outcome } => {
                Self::UserOutcome {
                    merge_outcome:  merge_outcome.map(Into::into),
                    negate_outcome: negate_outcome.map(Into::into),
                    split_outcome:  split_outcome.map(Into::into)
                }
            }
            ReplicaCmdsAction::SpotUser { toggle_spot_dusting } => {
                Self::SpotUser { toggle_spot_dusting: toggle_spot_dusting.into() }
            }
            ReplicaCmdsAction::ClaimRewards => Self::ClaimRewards,
            ReplicaCmdsAction::ApproveAgent {
                agent_address,
                agent_name,
                hyperliquid_chain,
                nonce,
                signature_chain_id
            } => Self::ApproveAgent {
                agent_address,
                agent_name,
                hyperliquid_chain,
                nonce,
                signature_chain_id
            },
            ReplicaCmdsAction::ApproveBuilderFee {
                builder,
                hyperliquid_chain,
                max_fee_rate,
                nonce,
                signature_chain_id
            } => Self::ApproveBuilderFee {
                builder,
                hyperliquid_chain,
                max_fee_rate,
                nonce,
                signature_chain_id
            },
            ReplicaCmdsAction::SetReferrer { code } => Self::SetReferrer { code },
            ReplicaCmdsAction::RegisterReferrer { code } => Self::RegisterReferrer { code },
            ReplicaCmdsAction::AgentSetAbstraction { abstraction } => {
                Self::AgentSetAbstraction { abstraction }
            }
            ReplicaCmdsAction::UserSetAbstraction {
                abstraction,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                user
            } => Self::UserSetAbstraction {
                abstraction,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                user
            },
            ReplicaCmdsAction::AgentEnableDexAbstraction => Self::AgentEnableDexAbstraction,
            ReplicaCmdsAction::UserDexAbstraction {
                enabled,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                user
            } => Self::UserDexAbstraction {
                enabled,
                hyperliquid_chain,
                nonce,
                signature_chain_id,
                user
            },
            ReplicaCmdsAction::ReserveRequestWeight { weight } => {
                Self::ReserveRequestWeight { weight }
            }
            ReplicaCmdsAction::EvmRawTx { data } => Self::EvmRawTx { data },
            ReplicaCmdsAction::MultiSig { payload, signature_chain_id, signatures } => {
                Self::MultiSig {
                    payload: payload.into(),
                    signature_chain_id,
                    signatures: signatures.into_iter().map(Into::into).collect()
                }
            }
            ReplicaCmdsAction::Noop => Self::Noop,
            ReplicaCmdsAction::GossipPriorityBid { ip, max_gas, slot_id } => {
                Self::GossipPriorityBid { ip, max_gas, slot_id }
            }
            ReplicaCmdsAction::PerpDeploy { set_oracle, set_funding_multipliers } => {
                Self::PerpDeploy {
                    set_oracle:              set_oracle.map(Into::into),
                    set_funding_multipliers: set_funding_multipliers
                        .map(|pxs| pxs.into_iter().map(Into::into).collect())
                }
            }
            ReplicaCmdsAction::ValidatorL1UpdateReferenceOracle {
                external_perp_pxs,
                oracle_pxs
            } => Self::ValidatorL1UpdateReferenceOracle {
                external_perp_pxs: external_perp_pxs.into_iter().map(Into::into).collect(),
                oracle_pxs:        oracle_pxs.into_iter().map(Into::into).collect()
            },
            ReplicaCmdsAction::L1ValidatorVoteBridgeDeposit { action } => {
                Self::L1ValidatorVoteBridgeDeposit { action: action.into() }
            }
            ReplicaCmdsAction::VoteL1Hash { height, l1_hash, signature } => {
                Self::VoteL1Hash { height, l1_hash, signature: signature.into() }
            }
            ReplicaCmdsAction::VoteAppHash { app_hash, height, signature } => {
                Self::VoteAppHash { app_hash, height, signature: signature.into() }
            }
            ReplicaCmdsAction::SetGlobalAction {
                external_perp_pxs,
                native_px,
                pxs,
                usdt_usdc_px
            } => Self::SetGlobalAction {
                external_perp_pxs: external_perp_pxs.into_iter().map(Into::into).collect(),
                native_px,
                pxs: pxs.into_iter().map(Into::into).collect(),
                usdt_usdc_px
            },
            ReplicaCmdsAction::VoteEthDepositAction { eth_id, usd, user } => {
                Self::VoteEthDepositAction { eth_id: eth_id.into(), usd, user }
            }
            ReplicaCmdsAction::VoteEthFinalizedWithdrawalAction {
                destination,
                eth_tx_hash,
                nonce,
                usd,
                user
            } => Self::VoteEthFinalizedWithdrawalAction {
                destination,
                eth_tx_hash,
                nonce,
                usd,
                user
            },
            ReplicaCmdsAction::NetChildVaultPositionsAction { assets, child_vault_addresses } => {
                Self::NetChildVaultPositionsAction { assets, child_vault_addresses }
            }
            ReplicaCmdsAction::ValidatorSignWithdrawalAction {
                destination,
                nonce,
                signature,
                usd,
                user
            } => Self::ValidatorSignWithdrawalAction {
                destination,
                nonce,
                signature: signature.into(),
                usd,
                user
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdOrder {
    pub a: u64,
    pub b: bool,
    pub p: f64,
    pub s: f64,
    pub r: bool,
    pub t: ReplicaCmdOrderType,
    pub c: Option<String>
}

impl From<ReplicaCmdsOrder> for ReplicaCmdOrder {
    fn from(value: ReplicaCmdsOrder) -> Self {
        Self {
            a: value.a,
            b: value.b,
            p: value.p,
            s: value.s,
            r: value.r,
            t: value.t.into(),
            c: value.c
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReplicaCmdOrderType {
    Limit { tif: String },
    Trigger { is_market: bool, tpsl: String, trigger_px: f64 }
}

impl From<ReplicaCmdsOrderType> for ReplicaCmdOrderType {
    fn from(value: ReplicaCmdsOrderType) -> Self {
        match value {
            ReplicaCmdsOrderType::Limit { tif } => Self::Limit { tif },
            ReplicaCmdsOrderType::Trigger { is_market, tpsl, trigger_px } => {
                Self::Trigger { is_market, tpsl, trigger_px }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReplicaCmdGrouping {
    Named(String),
    P { p: u64 }
}

impl From<ReplicaCmdsGrouping> for ReplicaCmdGrouping {
    fn from(value: ReplicaCmdsGrouping) -> Self {
        match value {
            ReplicaCmdsGrouping::Named(name) => Self::Named(name),
            ReplicaCmdsGrouping::P { p } => Self::P { p }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdBuilder {
    pub b: String,
    pub f: u64
}

impl From<ReplicaCmdsBuilder> for ReplicaCmdBuilder {
    fn from(value: ReplicaCmdsBuilder) -> Self {
        Self { b: value.b, f: value.f }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdCancel {
    pub a: u64,
    pub o: u64
}

impl From<ReplicaCmdsCancel> for ReplicaCmdCancel {
    fn from(value: ReplicaCmdsCancel) -> Self {
        Self { a: value.a, o: value.o }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdCancelByCloid {
    pub asset: u64,
    pub cloid: String
}

impl From<ReplicaCmdsCancelByCloid> for ReplicaCmdCancelByCloid {
    fn from(value: ReplicaCmdsCancelByCloid) -> Self {
        Self { asset: value.asset, cloid: value.cloid }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdModify {
    pub oid:   ReplicaCmdOid,
    pub order: ReplicaCmdOrder
}

impl From<ReplicaCmdsModify> for ReplicaCmdModify {
    fn from(value: ReplicaCmdsModify) -> Self {
        Self { oid: value.oid.into(), order: value.order.into() }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReplicaCmdOid {
    Oid(u64),
    Cloid(String)
}

impl From<ReplicaCmdsOid> for ReplicaCmdOid {
    fn from(value: ReplicaCmdsOid) -> Self {
        match value {
            ReplicaCmdsOid::Oid(oid) => Self::Oid(oid),
            ReplicaCmdsOid::Cloid(cloid) => Self::Cloid(cloid)
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdTwap {
    pub a: u64,
    pub b: bool,
    pub m: u64,
    pub r: bool,
    pub s: f64,
    pub t: bool
}

impl From<ReplicaCmdsTwap> for ReplicaCmdTwap {
    fn from(value: ReplicaCmdsTwap) -> Self {
        Self { a: value.a, b: value.b, m: value.m, r: value.r, s: value.s, t: value.t }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdMergeOutcome {
    pub amount:  Option<f64>,
    pub outcome: u64
}

impl From<ReplicaCmdsMergeOutcome> for ReplicaCmdMergeOutcome {
    fn from(value: ReplicaCmdsMergeOutcome) -> Self {
        Self { amount: value.amount, outcome: value.outcome }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdNegateOutcome {
    pub amount:   f64,
    pub outcome:  u64,
    pub question: u64
}

impl From<ReplicaCmdsNegateOutcome> for ReplicaCmdNegateOutcome {
    fn from(value: ReplicaCmdsNegateOutcome) -> Self {
        Self { amount: value.amount, outcome: value.outcome, question: value.question }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdSplitOutcome {
    pub amount:  f64,
    pub outcome: u64
}

impl From<ReplicaCmdsSplitOutcome> for ReplicaCmdSplitOutcome {
    fn from(value: ReplicaCmdsSplitOutcome) -> Self {
        Self { amount: value.amount, outcome: value.outcome }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCmdToggleSpotDusting {
    pub opt_out: bool
}

impl From<ReplicaCmdsToggleSpotDusting> for ReplicaCmdToggleSpotDusting {
    fn from(value: ReplicaCmdsToggleSpotDusting) -> Self {
        Self { opt_out: value.opt_out }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCmdMultiSigPayload {
    pub multi_sig_user: String,
    pub outer_signer:   String,
    pub action:         Box<ReplicaCmdAction>
}

impl From<ReplicaCmdsMultiSigPayload> for ReplicaCmdMultiSigPayload {
    fn from(value: ReplicaCmdsMultiSigPayload) -> Self {
        Self {
            multi_sig_user: value.multi_sig_user,
            outer_signer:   value.outer_signer,
            action:         Box::new((*value.action).into())
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCmdPerpDeploySetOracle {
    pub dex:               String,
    pub oracle_pxs:        Vec<ReplicaCmdCoinPx>,
    pub mark_pxs:          Vec<Vec<ReplicaCmdCoinPx>>,
    pub external_perp_pxs: Vec<ReplicaCmdCoinPx>
}

impl From<ReplicaCmdsPerpDeploySetOracle> for ReplicaCmdPerpDeploySetOracle {
    fn from(value: ReplicaCmdsPerpDeploySetOracle) -> Self {
        Self {
            dex:               value.dex,
            oracle_pxs:        value.oracle_pxs.into_iter().map(Into::into).collect(),
            mark_pxs:          value
                .mark_pxs
                .into_iter()
                .map(|pxs| pxs.into_iter().map(Into::into).collect())
                .collect(),
            external_perp_pxs: value
                .external_perp_pxs
                .into_iter()
                .map(Into::into)
                .collect()
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdCoinPx {
    pub coin: String,
    pub px:   f64
}

impl From<ReplicaCmdsCoinPx> for ReplicaCmdCoinPx {
    fn from(value: ReplicaCmdsCoinPx) -> Self {
        Self { coin: value.coin, px: value.px }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdSetGlobalPx {
    pub px_a: Option<f64>,
    pub px_b: f64
}

impl From<ReplicaCmdsSetGlobalPx> for ReplicaCmdSetGlobalPx {
    fn from(value: ReplicaCmdsSetGlobalPx) -> Self {
        Self { px_a: value.px_a, px_b: value.px_b }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCmdBridgeDepositVote {
    pub eth_id: ReplicaCmdEthId,
    pub usd:    u64,
    pub user:   String
}

impl From<ReplicaCmdsBridgeDepositVote> for ReplicaCmdBridgeDepositVote {
    fn from(value: ReplicaCmdsBridgeDepositVote) -> Self {
        Self { eth_id: value.eth_id.into(), usd: value.usd, user: value.user }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdEthId {
    pub b: u64,
    pub e: String,
    pub h: String,
    pub i: u64,
    pub t: u64
}

impl From<ReplicaCmdsEthId> for ReplicaCmdEthId {
    fn from(value: ReplicaCmdsEthId) -> Self {
        Self { b: value.b, e: value.e, h: value.h, i: value.i, t: value.t }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum ReplicaCmdRes {
    Ok { response: ReplicaCmdResponse },
    Err { response: String }
}

impl From<ReplicaCmdsRes> for ReplicaCmdRes {
    fn from(value: ReplicaCmdsRes) -> Self {
        match value {
            ReplicaCmdsRes::Ok { response } => Self::Ok { response: response.into() },
            ReplicaCmdsRes::Err { response } => Self::Err { response }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ReplicaCmdResponse {
    Cancel { data: ReplicaCmdStatusesData },
    Default,
    Order { data: ReplicaCmdStatusesData },
    SetGlobal { data: Vec<String> },
    TwapCancel { data: ReplicaCmdTwapData },
    TwapOrder { data: ReplicaCmdTwapData },
    ValidatorVote { data: bool }
}

impl From<ReplicaCmdsResponse> for ReplicaCmdResponse {
    fn from(value: ReplicaCmdsResponse) -> Self {
        match value {
            ReplicaCmdsResponse::Cancel { data } => Self::Cancel { data: data.into() },
            ReplicaCmdsResponse::Default => Self::Default,
            ReplicaCmdsResponse::Order { data } => Self::Order { data: data.into() },
            ReplicaCmdsResponse::SetGlobal { data } => Self::SetGlobal { data },
            ReplicaCmdsResponse::TwapCancel { data } => Self::TwapCancel { data: data.into() },
            ReplicaCmdsResponse::TwapOrder { data } => Self::TwapOrder { data: data.into() },
            ReplicaCmdsResponse::ValidatorVote { data } => Self::ValidatorVote { data }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdStatusesData {
    pub statuses: Vec<ReplicaCmdOrderStatus>
}

impl From<ReplicaCmdsStatusesData> for ReplicaCmdStatusesData {
    fn from(value: ReplicaCmdsStatusesData) -> Self {
        Self { statuses: value.statuses.into_iter().map(Into::into).collect() }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReplicaCmdOrderStatus {
    Error(String),
    Filled { avg_px: f64, cloid: Option<String>, oid: u64, total_sz: f64 },
    Resting { cloid: Option<String>, oid: u64 },
    Success,
    WaitingForFill,
    WaitingForTrigger
}

impl From<ReplicaCmdsOrderStatus> for ReplicaCmdOrderStatus {
    fn from(value: ReplicaCmdsOrderStatus) -> Self {
        match value {
            ReplicaCmdsOrderStatus::Error(error) => Self::Error(error),
            ReplicaCmdsOrderStatus::Filled { avg_px, cloid, oid, total_sz } => {
                Self::Filled { avg_px, cloid, oid, total_sz }
            }
            ReplicaCmdsOrderStatus::Resting { cloid, oid } => Self::Resting { cloid, oid },
            ReplicaCmdsOrderStatus::Success => Self::Success,
            ReplicaCmdsOrderStatus::WaitingForFill => Self::WaitingForFill,
            ReplicaCmdsOrderStatus::WaitingForTrigger => Self::WaitingForTrigger
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReplicaCmdTwapData {
    pub status: ReplicaCmdTwapStatus
}

impl From<ReplicaCmdsTwapData> for ReplicaCmdTwapData {
    fn from(value: ReplicaCmdsTwapData) -> Self {
        Self { status: value.status.into() }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReplicaCmdTwapStatus {
    Error(String),
    Running { twap_id: u64 },
    Success
}

impl From<ReplicaCmdsTwapStatus> for ReplicaCmdTwapStatus {
    fn from(value: ReplicaCmdsTwapStatus) -> Self {
        match value {
            ReplicaCmdsTwapStatus::Error(error) => Self::Error(error),
            ReplicaCmdsTwapStatus::Running { twap_id } => Self::Running { twap_id },
            ReplicaCmdsTwapStatus::Success => Self::Success
        }
    }
}
