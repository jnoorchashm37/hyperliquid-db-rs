use serde::{Deserialize, Serialize};

use crate::hl_fs::schemas::{
    MiscEventsEvent, MiscEventsFundingDelta, MiscEventsInner, MiscEventsLedgerDelta,
    MiscEventsLiquidatedPosition, MiscEventsPreviousWinner, MiscEventsValidatorReward
};

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MiscEvent {
    pub time:   u64,
    pub height: u64,
    pub hash:   String,
    pub inner:  MiscEventInner
}

impl MiscEvent {
    pub fn from_event(time: u64, height: u64, event: MiscEventsEvent) -> Self {
        Self { time, height, hash: event.hash, inner: event.inner.into() }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MiscEventInner {
    CDeposit {
        user:   String,
        amount: f64
    },
    Delegation {
        user:          String,
        validator:     String,
        amount:        f64,
        is_undelegate: bool
    },
    CWithdrawal {
        user:         String,
        amount:       f64,
        is_finalized: bool
    },
    ValidatorRewards {
        validator_to_reward: Vec<MiscEventValidatorReward>
    },
    Funding {
        deltas: Vec<MiscEventFundingDelta>
    },
    LedgerUpdate {
        users: Vec<String>,
        delta: MiscEventLedgerDelta
    },
    GossipPriorityAuctionRestart {
        slot_id:         u64,
        previous_winner: MiscEventPreviousWinner,
        end_gas:         f64
    }
}

impl From<MiscEventsInner> for MiscEventInner {
    fn from(value: MiscEventsInner) -> Self {
        match value {
            MiscEventsInner::CDeposit { user, amount } => Self::CDeposit { user, amount },
            MiscEventsInner::Delegation { user, validator, amount, is_undelegate } => {
                Self::Delegation { user, validator, amount, is_undelegate }
            }
            MiscEventsInner::CWithdrawal { user, amount, is_finalized } => {
                Self::CWithdrawal { user, amount, is_finalized }
            }
            MiscEventsInner::ValidatorRewards { validator_to_reward } => Self::ValidatorRewards {
                validator_to_reward: validator_to_reward.into_iter().map(Into::into).collect()
            },
            MiscEventsInner::Funding { deltas } => {
                Self::Funding { deltas: deltas.into_iter().map(Into::into).collect() }
            }
            MiscEventsInner::LedgerUpdate { users, delta } => {
                Self::LedgerUpdate { users, delta: delta.into() }
            }
            MiscEventsInner::GossipPriorityAuctionRestart { slot_id, previous_winner, end_gas } => {
                Self::GossipPriorityAuctionRestart {
                    slot_id,
                    previous_winner: previous_winner.into(),
                    end_gas
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MiscEventValidatorReward {
    pub validator: String,
    pub reward:    f64
}

impl From<MiscEventsValidatorReward> for MiscEventValidatorReward {
    fn from(value: MiscEventsValidatorReward) -> Self {
        Self { validator: value.validator, reward: value.reward }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MiscEventFundingDelta {
    pub user:           String,
    pub coin:           String,
    pub funding_amount: f64,
    pub szi:            f64,
    pub funding_rate:   f64
}

impl From<MiscEventsFundingDelta> for MiscEventFundingDelta {
    fn from(value: MiscEventsFundingDelta) -> Self {
        Self {
            user:           value.user,
            coin:           value.coin,
            funding_amount: value.funding_amount,
            szi:            value.szi,
            funding_rate:   value.funding_rate
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MiscEventPreviousWinner {
    Ip(String)
}

impl From<MiscEventsPreviousWinner> for MiscEventPreviousWinner {
    fn from(value: MiscEventsPreviousWinner) -> Self {
        match value {
            MiscEventsPreviousWinner::Ip(ip) => Self::Ip(ip)
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum MiscEventLedgerDelta {
    Send {
        user:             String,
        destination:      String,
        source_dex:       String,
        destination_dex:  String,
        token:            String,
        amount:           f64,
        usdc_value:       f64,
        fee:              f64,
        native_token_fee: f64,
        nonce:            u64,
        fee_token:        String
    },
    SpotTransfer {
        token:            String,
        amount:           f64,
        usdc_value:       f64,
        user:             String,
        destination:      String,
        fee:              f64,
        native_token_fee: f64,
        nonce:            u64,
        fee_token:        String
    },
    AccountClassTransfer {
        usdc:    f64,
        to_perp: bool
    },
    SubAccountTransfer {
        usdc:        f64,
        user:        String,
        destination: String
    },
    InternalTransfer {
        usdc:        f64,
        user:        String,
        destination: String,
        fee:         f64
    },
    Deposit {
        usdc: f64
    },
    Withdraw {
        usdc:  f64,
        nonce: u64,
        fee:   f64
    },
    CStakingTransfer {
        token:      String,
        amount:     f64,
        is_deposit: bool
    },
    GossipPriorityGasAuction {
        token:  String,
        amount: f64
    },
    BorrowLend {
        token:           String,
        operation:       String,
        amount:          f64,
        interest_amount: f64
    },
    RewardsClaim {
        amount: f64,
        token:  String
    },
    VaultCreate {
        vault: String,
        usdc:  f64,
        fee:   f64
    },
    VaultDeposit {
        vault: String,
        usdc:  f64
    },
    VaultWithdraw {
        vault:             String,
        user:              String,
        requested_usd:     f64,
        commission:        f64,
        closing_cost:      f64,
        basis:             f64,
        net_withdrawn_usd: f64
    },
    VaultDistribution {
        vault: String,
        usdc:  f64
    },
    VaultLeaderCommission {
        user: String,
        usdc: f64
    },
    Liquidation {
        liquidated_ntl_pos:   f64,
        account_value:        f64,
        leverage_type:        String,
        liquidated_positions: Vec<MiscEventLiquidatedPosition>
    },
    SpotGenesis {
        token:  String,
        amount: f64
    },
    AccountActivationGas {
        amount: f64,
        token:  String
    },
    PerpDexClassTransfer {
        amount:  f64,
        token:   String,
        dex:     String,
        to_perp: bool
    },
    DeployGasAuction {
        token:  String,
        amount: f64
    }
}

impl From<MiscEventsLedgerDelta> for MiscEventLedgerDelta {
    fn from(value: MiscEventsLedgerDelta) -> Self {
        match value {
            MiscEventsLedgerDelta::Send {
                user,
                destination,
                source_dex,
                destination_dex,
                token,
                amount,
                usdc_value,
                fee,
                native_token_fee,
                nonce,
                fee_token
            } => Self::Send {
                user,
                destination,
                source_dex,
                destination_dex,
                token,
                amount,
                usdc_value,
                fee,
                native_token_fee,
                nonce,
                fee_token
            },
            MiscEventsLedgerDelta::SpotTransfer {
                token,
                amount,
                usdc_value,
                user,
                destination,
                fee,
                native_token_fee,
                nonce,
                fee_token
            } => Self::SpotTransfer {
                token,
                amount,
                usdc_value,
                user,
                destination,
                fee,
                native_token_fee,
                nonce,
                fee_token
            },
            MiscEventsLedgerDelta::AccountClassTransfer { usdc, to_perp } => {
                Self::AccountClassTransfer { usdc, to_perp }
            }
            MiscEventsLedgerDelta::SubAccountTransfer { usdc, user, destination } => {
                Self::SubAccountTransfer { usdc, user, destination }
            }
            MiscEventsLedgerDelta::InternalTransfer { usdc, user, destination, fee } => {
                Self::InternalTransfer { usdc, user, destination, fee }
            }
            MiscEventsLedgerDelta::Deposit { usdc } => Self::Deposit { usdc },
            MiscEventsLedgerDelta::Withdraw { usdc, nonce, fee } => {
                Self::Withdraw { usdc, nonce, fee }
            }
            MiscEventsLedgerDelta::CStakingTransfer { token, amount, is_deposit } => {
                Self::CStakingTransfer { token, amount, is_deposit }
            }
            MiscEventsLedgerDelta::GossipPriorityGasAuction { token, amount } => {
                Self::GossipPriorityGasAuction { token, amount }
            }
            MiscEventsLedgerDelta::BorrowLend { token, operation, amount, interest_amount } => {
                Self::BorrowLend { token, operation, amount, interest_amount }
            }
            MiscEventsLedgerDelta::RewardsClaim { amount, token } => {
                Self::RewardsClaim { amount, token }
            }
            MiscEventsLedgerDelta::VaultCreate { vault, usdc, fee } => {
                Self::VaultCreate { vault, usdc, fee }
            }
            MiscEventsLedgerDelta::VaultDeposit { vault, usdc } => {
                Self::VaultDeposit { vault, usdc }
            }
            MiscEventsLedgerDelta::VaultWithdraw {
                vault,
                user,
                requested_usd,
                commission,
                closing_cost,
                basis,
                net_withdrawn_usd
            } => Self::VaultWithdraw {
                vault,
                user,
                requested_usd,
                commission,
                closing_cost,
                basis,
                net_withdrawn_usd
            },
            MiscEventsLedgerDelta::VaultDistribution { vault, usdc } => {
                Self::VaultDistribution { vault, usdc }
            }
            MiscEventsLedgerDelta::VaultLeaderCommission { user, usdc } => {
                Self::VaultLeaderCommission { user, usdc }
            }
            MiscEventsLedgerDelta::Liquidation {
                liquidated_ntl_pos,
                account_value,
                leverage_type,
                liquidated_positions
            } => Self::Liquidation {
                liquidated_ntl_pos,
                account_value,
                leverage_type,
                liquidated_positions: liquidated_positions.into_iter().map(Into::into).collect()
            },
            MiscEventsLedgerDelta::SpotGenesis { token, amount } => {
                Self::SpotGenesis { token, amount }
            }
            MiscEventsLedgerDelta::AccountActivationGas { amount, token } => {
                Self::AccountActivationGas { amount, token }
            }
            MiscEventsLedgerDelta::PerpDexClassTransfer { amount, token, dex, to_perp } => {
                Self::PerpDexClassTransfer { amount, token, dex, to_perp }
            }
            MiscEventsLedgerDelta::DeployGasAuction { token, amount } => {
                Self::DeployGasAuction { token, amount }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MiscEventLiquidatedPosition {
    pub coin: String,
    pub szi:  f64
}

impl From<MiscEventsLiquidatedPosition> for MiscEventLiquidatedPosition {
    fn from(value: MiscEventsLiquidatedPosition) -> Self {
        Self { coin: value.coin, szi: value.szi }
    }
}
