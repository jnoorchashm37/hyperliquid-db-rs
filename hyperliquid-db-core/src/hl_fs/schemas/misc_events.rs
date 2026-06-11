use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::hl_fs::schemas::NODE_DATA_DATE_TIME_FORMAT;

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct MiscEventsRows {
    pub local_time:   String,
    pub block_time:   String,
    pub block_number: u64,
    pub events:       Vec<MiscEventsEvent>
}

impl MiscEventsRows {
    pub fn local_time_unix_ms(&self) -> eyre::Result<u64> {
        Ok(NaiveDateTime::parse_from_str(&self.local_time, NODE_DATA_DATE_TIME_FORMAT)?
            .and_utc()
            .timestamp_millis() as u64)
    }

    pub fn block_time_unix_ms(&self) -> eyre::Result<u64> {
        Ok(NaiveDateTime::parse_from_str(&self.block_time, NODE_DATA_DATE_TIME_FORMAT)?
            .and_utc()
            .timestamp_millis() as u64)
    }
}

impl<'de> Deserialize<'de> for MiscEventsRows {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::MiscEventsRowsRaw::deserialize(deserializer)?;

        let events = raw
            .events
            .into_iter()
            .map(|raw_event| MiscEventsEvent {
                time:  raw_event.time,
                hash:  raw_event.hash,
                inner: raw_event.inner
            })
            .collect();

        Ok(Self {
            local_time: raw.local_time,
            block_time: raw.block_time,
            block_number: raw.block_number,
            events
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct MiscEventsEvent {
    pub time:  String,
    pub hash:  String,
    pub inner: MiscEventsInner
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub enum MiscEventsInner {
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
        validator_to_reward: Vec<MiscEventsValidatorReward>
    },
    Funding {
        deltas: Vec<MiscEventsFundingDelta>
    },
    LedgerUpdate {
        users: Vec<String>,
        delta: MiscEventsLedgerDelta
    },
    GossipPriorityAuctionRestart {
        slot_id:         u64,
        previous_winner: MiscEventsPreviousWinner,
        end_gas:         f64
    }
}

impl<'de> Deserialize<'de> for MiscEventsInner {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::MiscEventsInnerRaw::deserialize(deserializer)?;

        let parse_f64 = |value: String| -> Result<f64, D::Error> {
            value.parse().map_err(serde::de::Error::custom)
        };

        match raw {
            _private::MiscEventsInnerRaw::CDeposit { user, amount } => {
                Ok(MiscEventsInner::CDeposit { user, amount: parse_f64(amount)? })
            }
            _private::MiscEventsInnerRaw::Delegation { user, validator, amount, is_undelegate } => {
                Ok(MiscEventsInner::Delegation {
                    user,
                    validator,
                    amount: parse_f64(amount)?,
                    is_undelegate
                })
            }
            _private::MiscEventsInnerRaw::CWithdrawal { user, amount, is_finalized } => {
                Ok(MiscEventsInner::CWithdrawal { user, amount: parse_f64(amount)?, is_finalized })
            }
            _private::MiscEventsInnerRaw::ValidatorRewards { validator_to_reward } => {
                Ok(MiscEventsInner::ValidatorRewards {
                    validator_to_reward: validator_to_reward
                        .into_iter()
                        .map(|raw_reward| {
                            Ok(MiscEventsValidatorReward {
                                validator: raw_reward.0,
                                reward:    parse_f64(raw_reward.1)?
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?
                })
            }
            _private::MiscEventsInnerRaw::Funding { deltas } => Ok(MiscEventsInner::Funding {
                deltas: deltas
                    .into_iter()
                    .map(|raw_delta| {
                        Ok(MiscEventsFundingDelta {
                            user:           raw_delta.user,
                            coin:           raw_delta.coin,
                            funding_amount: parse_f64(raw_delta.funding_amount)?,
                            szi:            parse_f64(raw_delta.szi)?,
                            funding_rate:   parse_f64(raw_delta.funding_rate)?
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }),
            _private::MiscEventsInnerRaw::LedgerUpdate { users, delta } => {
                Ok(MiscEventsInner::LedgerUpdate { users, delta })
            }
            _private::MiscEventsInnerRaw::GossipPriorityAuctionRestart {
                slot_id,
                previous_winner,
                end_gas
            } => Ok(MiscEventsInner::GossipPriorityAuctionRestart {
                slot_id,
                previous_winner,
                end_gas: parse_f64(end_gas)?
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct MiscEventsValidatorReward {
    pub validator: String,
    pub reward:    f64
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct MiscEventsFundingDelta {
    pub user:           String,
    pub coin:           String,
    pub funding_amount: f64,
    pub szi:            f64,
    pub funding_rate:   f64
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MiscEventsPreviousWinner {
    Ip(String)
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum MiscEventsLedgerDelta {
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
        liquidated_positions: Vec<MiscEventsLiquidatedPosition>
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

impl<'de> Deserialize<'de> for MiscEventsLedgerDelta {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::MiscEventsLedgerDeltaRaw::deserialize(deserializer)?;

        let parse_f64 = |value: String| -> Result<f64, D::Error> {
            value.parse().map_err(serde::de::Error::custom)
        };

        match raw {
            _private::MiscEventsLedgerDeltaRaw::Send {
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
            } => Ok(MiscEventsLedgerDelta::Send {
                user,
                destination,
                source_dex,
                destination_dex,
                token,
                amount: parse_f64(amount)?,
                usdc_value: parse_f64(usdc_value)?,
                fee: parse_f64(fee)?,
                native_token_fee: parse_f64(native_token_fee)?,
                nonce,
                fee_token
            }),
            _private::MiscEventsLedgerDeltaRaw::SpotTransfer {
                token,
                amount,
                usdc_value,
                user,
                destination,
                fee,
                native_token_fee,
                nonce,
                fee_token
            } => Ok(MiscEventsLedgerDelta::SpotTransfer {
                token,
                amount: parse_f64(amount)?,
                usdc_value: parse_f64(usdc_value)?,
                user,
                destination,
                fee: parse_f64(fee)?,
                native_token_fee: parse_f64(native_token_fee)?,
                nonce,
                fee_token
            }),
            _private::MiscEventsLedgerDeltaRaw::AccountClassTransfer { usdc, to_perp } => {
                Ok(MiscEventsLedgerDelta::AccountClassTransfer { usdc: parse_f64(usdc)?, to_perp })
            }
            _private::MiscEventsLedgerDeltaRaw::SubAccountTransfer { usdc, user, destination } => {
                Ok(MiscEventsLedgerDelta::SubAccountTransfer {
                    usdc: parse_f64(usdc)?,
                    user,
                    destination
                })
            }
            _private::MiscEventsLedgerDeltaRaw::InternalTransfer {
                usdc,
                user,
                destination,
                fee
            } => Ok(MiscEventsLedgerDelta::InternalTransfer {
                usdc: parse_f64(usdc)?,
                user,
                destination,
                fee: parse_f64(fee)?
            }),
            _private::MiscEventsLedgerDeltaRaw::Deposit { usdc } => {
                Ok(MiscEventsLedgerDelta::Deposit { usdc: parse_f64(usdc)? })
            }
            _private::MiscEventsLedgerDeltaRaw::Withdraw { usdc, nonce, fee } => {
                Ok(MiscEventsLedgerDelta::Withdraw {
                    usdc: parse_f64(usdc)?,
                    nonce,
                    fee: parse_f64(fee)?
                })
            }
            _private::MiscEventsLedgerDeltaRaw::CStakingTransfer { token, amount, is_deposit } => {
                Ok(MiscEventsLedgerDelta::CStakingTransfer {
                    token,
                    amount: parse_f64(amount)?,
                    is_deposit
                })
            }
            _private::MiscEventsLedgerDeltaRaw::GossipPriorityGasAuction { token, amount } => {
                Ok(MiscEventsLedgerDelta::GossipPriorityGasAuction {
                    token,
                    amount: parse_f64(amount)?
                })
            }
            _private::MiscEventsLedgerDeltaRaw::BorrowLend {
                token,
                operation,
                amount,
                interest_amount
            } => Ok(MiscEventsLedgerDelta::BorrowLend {
                token,
                operation,
                amount: parse_f64(amount)?,
                interest_amount: parse_f64(interest_amount)?
            }),
            _private::MiscEventsLedgerDeltaRaw::RewardsClaim { amount, token } => {
                Ok(MiscEventsLedgerDelta::RewardsClaim { amount: parse_f64(amount)?, token })
            }
            _private::MiscEventsLedgerDeltaRaw::VaultCreate { vault, usdc, fee } => {
                Ok(MiscEventsLedgerDelta::VaultCreate {
                    vault,
                    usdc: parse_f64(usdc)?,
                    fee: parse_f64(fee)?
                })
            }
            _private::MiscEventsLedgerDeltaRaw::VaultDeposit { vault, usdc } => {
                Ok(MiscEventsLedgerDelta::VaultDeposit { vault, usdc: parse_f64(usdc)? })
            }
            _private::MiscEventsLedgerDeltaRaw::VaultWithdraw {
                vault,
                user,
                requested_usd,
                commission,
                closing_cost,
                basis,
                net_withdrawn_usd
            } => Ok(MiscEventsLedgerDelta::VaultWithdraw {
                vault,
                user,
                requested_usd: parse_f64(requested_usd)?,
                commission: parse_f64(commission)?,
                closing_cost: parse_f64(closing_cost)?,
                basis: parse_f64(basis)?,
                net_withdrawn_usd: parse_f64(net_withdrawn_usd)?
            }),
            _private::MiscEventsLedgerDeltaRaw::VaultDistribution { vault, usdc } => {
                Ok(MiscEventsLedgerDelta::VaultDistribution { vault, usdc: parse_f64(usdc)? })
            }
            _private::MiscEventsLedgerDeltaRaw::VaultLeaderCommission { user, usdc } => {
                Ok(MiscEventsLedgerDelta::VaultLeaderCommission { user, usdc: parse_f64(usdc)? })
            }
            _private::MiscEventsLedgerDeltaRaw::Liquidation {
                liquidated_ntl_pos,
                account_value,
                leverage_type,
                liquidated_positions
            } => Ok(MiscEventsLedgerDelta::Liquidation {
                liquidated_ntl_pos: parse_f64(liquidated_ntl_pos)?,
                account_value: parse_f64(account_value)?,
                leverage_type,
                liquidated_positions: liquidated_positions
                    .into_iter()
                    .map(|raw_position| {
                        Ok(MiscEventsLiquidatedPosition {
                            coin: raw_position.coin,
                            szi:  parse_f64(raw_position.szi)?
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }),
            _private::MiscEventsLedgerDeltaRaw::SpotGenesis { token, amount } => {
                Ok(MiscEventsLedgerDelta::SpotGenesis { token, amount: parse_f64(amount)? })
            }
            _private::MiscEventsLedgerDeltaRaw::AccountActivationGas { amount, token } => {
                Ok(MiscEventsLedgerDelta::AccountActivationGas {
                    amount: parse_f64(amount)?,
                    token
                })
            }
            _private::MiscEventsLedgerDeltaRaw::PerpDexClassTransfer {
                amount,
                token,
                dex,
                to_perp
            } => Ok(MiscEventsLedgerDelta::PerpDexClassTransfer {
                amount: parse_f64(amount)?,
                token,
                dex,
                to_perp
            }),
            _private::MiscEventsLedgerDeltaRaw::DeployGasAuction { token, amount } => {
                Ok(MiscEventsLedgerDelta::DeployGasAuction { token, amount: parse_f64(amount)? })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct MiscEventsLiquidatedPosition {
    pub coin: String,
    pub szi:  f64
}

mod _private {
    use serde::{Deserialize, Serialize};

    use super::{MiscEventsInner, MiscEventsLedgerDelta, MiscEventsPreviousWinner};

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct MiscEventsRowsRaw {
        pub local_time:   String,
        pub block_time:   String,
        pub block_number: u64,
        pub events:       Vec<MiscEventsEventRaw>
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct MiscEventsEventRaw {
        pub time:  String,
        pub hash:  String,
        pub inner: MiscEventsInner
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub enum MiscEventsInnerRaw {
        CDeposit {
            user:   String,
            amount: String
        },
        Delegation {
            user:          String,
            validator:     String,
            amount:        String,
            is_undelegate: bool
        },
        CWithdrawal {
            user:         String,
            amount:       String,
            is_finalized: bool
        },
        ValidatorRewards {
            validator_to_reward: Vec<MiscEventsValidatorRewardRaw>
        },
        Funding {
            deltas: Vec<MiscEventsFundingDeltaRaw>
        },
        LedgerUpdate {
            users: Vec<String>,
            delta: MiscEventsLedgerDelta
        },
        GossipPriorityAuctionRestart {
            slot_id:         u64,
            previous_winner: MiscEventsPreviousWinner,
            end_gas:         String
        }
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct MiscEventsValidatorRewardRaw(pub String, pub String);

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct MiscEventsFundingDeltaRaw {
        pub user:           String,
        pub coin:           String,
        pub funding_amount: String,
        pub szi:            String,
        pub funding_rate:   String
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum MiscEventsLedgerDeltaRaw {
        Send {
            user:             String,
            destination:      String,
            source_dex:       String,
            destination_dex:  String,
            token:            String,
            amount:           String,
            usdc_value:       String,
            fee:              String,
            native_token_fee: String,
            nonce:            u64,
            fee_token:        String
        },
        SpotTransfer {
            token:            String,
            amount:           String,
            usdc_value:       String,
            user:             String,
            destination:      String,
            fee:              String,
            native_token_fee: String,
            nonce:            u64,
            fee_token:        String
        },
        AccountClassTransfer {
            usdc:    String,
            to_perp: bool
        },
        SubAccountTransfer {
            usdc:        String,
            user:        String,
            destination: String
        },
        InternalTransfer {
            usdc:        String,
            user:        String,
            destination: String,
            fee:         String
        },
        Deposit {
            usdc: String
        },
        Withdraw {
            usdc:  String,
            nonce: u64,
            fee:   String
        },
        CStakingTransfer {
            token:      String,
            amount:     String,
            is_deposit: bool
        },
        GossipPriorityGasAuction {
            token:  String,
            amount: String
        },
        BorrowLend {
            token:           String,
            operation:       String,
            amount:          String,
            interest_amount: String
        },
        RewardsClaim {
            amount: String,
            token:  String
        },
        VaultCreate {
            vault: String,
            usdc:  String,
            fee:   String
        },
        VaultDeposit {
            vault: String,
            usdc:  String
        },
        VaultWithdraw {
            vault:             String,
            user:              String,
            requested_usd:     String,
            commission:        String,
            closing_cost:      String,
            basis:             String,
            net_withdrawn_usd: String
        },
        VaultDistribution {
            vault: String,
            usdc:  String
        },
        VaultLeaderCommission {
            user: String,
            usdc: String
        },
        Liquidation {
            liquidated_ntl_pos:   String,
            account_value:        String,
            leverage_type:        String,
            liquidated_positions: Vec<MiscEventsLiquidatedPositionRaw>
        },
        SpotGenesis {
            token:  String,
            amount: String
        },
        AccountActivationGas {
            amount: String,
            token:  String
        },
        PerpDexClassTransfer {
            amount:  String,
            token:   String,
            dex:     String,
            to_perp: bool
        },
        DeployGasAuction {
            token:  String,
            amount: String
        }
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct MiscEventsLiquidatedPositionRaw {
        pub coin: String,
        pub szi:  String
    }
}

#[cfg(test)]
mod tests {
    use super::{MiscEventsInner, MiscEventsLedgerDelta, MiscEventsPreviousWinner, MiscEventsRows};

    #[test]
    fn deserializes_ledger_update_send_sample_shape() {
        let row: MiscEventsRows = serde_json::from_str(
            r#"{
                "local_time":"2026-06-11T01:00:03.104726053",
                "block_time":"2026-06-11T01:00:02.896899363",
                "block_number":1030773868,
                "events":[{
                    "time":"2026-06-11T01:00:02.896899363",
                    "hash":"0x5a44d83dc07d168a5bbe043d705c6c02055b00235b70355cfe0d83907f70f074",
                    "inner":{"LedgerUpdate":{
                        "users":["0x8b1c0722a978599bb254c2bdd75992fa605073f6","0x2000000000000000000000000000000000000000"],
                        "delta":{
                            "type":"send",
                            "user":"0x8b1c0722a978599bb254c2bdd75992fa605073f6",
                            "destination":"0x2000000000000000000000000000000000000000",
                            "sourceDex":"spot",
                            "destinationDex":"spot",
                            "token":"USDC",
                            "amount":"500.008953",
                            "usdcValue":"500.008953",
                            "fee":"0.0",
                            "nativeTokenFee":"0.00002",
                            "nonce":1781139594371,
                            "feeToken":""
                        }
                    }}
                }]
            }"#
        )
        .unwrap();

        assert_eq!(row.local_time, "2026-06-11T01:00:03.104726053");
        assert_eq!(row.block_time, "2026-06-11T01:00:02.896899363");
        assert_eq!(row.block_number, 1030773868);
        assert_eq!(row.events.len(), 1);

        let event = &row.events[0];
        assert_eq!(event.time, "2026-06-11T01:00:02.896899363");
        assert_eq!(
            event.hash,
            "0x5a44d83dc07d168a5bbe043d705c6c02055b00235b70355cfe0d83907f70f074"
        );

        match &event.inner {
            MiscEventsInner::LedgerUpdate { users, delta } => {
                assert_eq!(users.len(), 2);
                assert_eq!(users[0], "0x8b1c0722a978599bb254c2bdd75992fa605073f6");

                match delta {
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
                    } => {
                        assert_eq!(user, "0x8b1c0722a978599bb254c2bdd75992fa605073f6");
                        assert_eq!(destination, "0x2000000000000000000000000000000000000000");
                        assert_eq!(source_dex, "spot");
                        assert_eq!(destination_dex, "spot");
                        assert_eq!(token, "USDC");
                        assert!((amount - 500.008953).abs() < f64::EPSILON);
                        assert!((usdc_value - 500.008953).abs() < f64::EPSILON);
                        assert!((fee - 0.0).abs() < f64::EPSILON);
                        assert!((native_token_fee - 0.00002).abs() < f64::EPSILON);
                        assert_eq!(*nonce, 1781139594371);
                        assert_eq!(fee_token, "");
                    }
                    _ => panic!("expected send ledger delta")
                }
            }
            _ => panic!("expected ledger update inner event")
        }
    }

    #[test]
    fn deserializes_staking_sample_shapes() {
        let row: MiscEventsRows = serde_json::from_str(
            r#"{
                "local_time":"2026-06-11T01:01:29.720408825",
                "block_time":"2026-06-11T01:01:29.513722441",
                "block_number":1030775110,
                "events":[
                    {
                        "time":"2026-06-11T01:01:29.513722441",
                        "hash":"0x7fe9ff9e443d22a38163043d7061460207350083df30417523b2aaf10330fc8e",
                        "inner":{"Delegation":{
                            "user":"0xde399799d4abda687280ad4512554f80f8df9666",
                            "validator":"0xa23b4556090260828ff3f939d2dbdd4f318b5f1f",
                            "amount":"4.0",
                            "is_undelegate":false
                        }}
                    },
                    {
                        "time":"2026-06-11T01:01:29.513722441",
                        "hash":"0x7fe9ff9e443d22a38163043d7061460207350083df30417523b2aaf10330fc8e",
                        "inner":{"CDeposit":{
                            "user":"0x163014fed389c392a0a214d2c4a28a73ec804741",
                            "amount":"0.00982529"
                        }}
                    },
                    {
                        "time":"2026-06-11T01:01:29.513722441",
                        "hash":"0x7fe9ff9e443d22a38163043d7061460207350083df30417523b2aaf10330fc8e",
                        "inner":{"CWithdrawal":{
                            "user":"0x033fb97dacc654b928de7129bf205f98eb15b1b1",
                            "amount":"11.38254596",
                            "is_finalized":false
                        }}
                    }
                ]
            }"#
        )
        .unwrap();

        assert_eq!(row.events.len(), 3);

        match &row.events[0].inner {
            MiscEventsInner::Delegation { user, validator, amount, is_undelegate } => {
                assert_eq!(user, "0xde399799d4abda687280ad4512554f80f8df9666");
                assert_eq!(validator, "0xa23b4556090260828ff3f939d2dbdd4f318b5f1f");
                assert!((amount - 4.0).abs() < f64::EPSILON);
                assert!(!is_undelegate);
            }
            _ => panic!("expected delegation inner event")
        }

        match &row.events[1].inner {
            MiscEventsInner::CDeposit { user, amount } => {
                assert_eq!(user, "0x163014fed389c392a0a214d2c4a28a73ec804741");
                assert!((amount - 0.00982529).abs() < f64::EPSILON);
            }
            _ => panic!("expected c deposit inner event")
        }

        match &row.events[2].inner {
            MiscEventsInner::CWithdrawal { user, amount, is_finalized } => {
                assert_eq!(user, "0x033fb97dacc654b928de7129bf205f98eb15b1b1");
                assert!((amount - 11.38254596).abs() < f64::EPSILON);
                assert!(!is_finalized);
            }
            _ => panic!("expected c withdrawal inner event")
        }
    }

    #[test]
    fn deserializes_funding_validator_rewards_and_gossip_sample_shapes() {
        let row: MiscEventsRows = serde_json::from_str(
            r#"{
                "local_time":"2026-06-11T01:00:00.312725258",
                "block_time":"2026-06-11T01:00:00.039772125",
                "block_number":1030773830,
                "events":[
                    {
                        "time":"2026-06-11T01:00:00.039772125",
                        "hash":"0x0000000000000000000000000000000000000000000000000000000000000000",
                        "inner":{"Funding":{"deltas":[
                            {"user":"0x00000000000dfd47d200dc26e4e5c79b7e6de984","coin":"ZEC","funding_amount":"9.330246","szi":"280.0","funding_rate":"-0.0000810249"},
                            {"user":"0x00000000002f3d928030df28cb15b27b56888b9b","coin":"HYPE","funding_amount":"4.506878","szi":"-2744.84","funding_rate":"0.0000308526"}
                        ]}}
                    },
                    {
                        "time":"2026-06-11T01:00:00.039772125",
                        "hash":"0x0000000000000000000000000000000000000000000000000000000000000000",
                        "inner":{"ValidatorRewards":{"validator_to_reward":[
                            ["0x000000000056f99d36b6f2e0c51fd41496bbacb8","0.28221779"],
                            ["0x15458aed3c7a49b215fbfa863c6ff550c31e1a31","0.22258446"]
                        ]}}
                    },
                    {
                        "time":"2026-06-11T01:00:00.039772125",
                        "hash":"0x0000000000000000000000000000000000000000000000000000000000000000",
                        "inner":{"GossipPriorityAuctionRestart":{
                            "slot_id":0,
                            "previous_winner":{"Ip":"18.180.228.50"},
                            "end_gas":"0.39989171"
                        }}
                    }
                ]
            }"#
        )
        .unwrap();

        assert_eq!(row.events.len(), 3);

        match &row.events[0].inner {
            MiscEventsInner::Funding { deltas } => {
                assert_eq!(deltas.len(), 2);
                assert_eq!(deltas[0].user, "0x00000000000dfd47d200dc26e4e5c79b7e6de984");
                assert_eq!(deltas[0].coin, "ZEC");
                assert!((deltas[0].funding_amount - 9.330246).abs() < f64::EPSILON);
                assert!((deltas[0].szi - 280.0).abs() < f64::EPSILON);
                assert!((deltas[0].funding_rate - -0.0000810249).abs() < f64::EPSILON);
                assert_eq!(deltas[1].coin, "HYPE");
                assert!((deltas[1].szi - -2744.84).abs() < f64::EPSILON);
            }
            _ => panic!("expected funding inner event")
        }

        match &row.events[1].inner {
            MiscEventsInner::ValidatorRewards { validator_to_reward } => {
                assert_eq!(validator_to_reward.len(), 2);
                assert_eq!(
                    validator_to_reward[0].validator,
                    "0x000000000056f99d36b6f2e0c51fd41496bbacb8"
                );
                assert!((validator_to_reward[0].reward - 0.28221779).abs() < f64::EPSILON);
            }
            _ => panic!("expected validator rewards inner event")
        }

        match &row.events[2].inner {
            MiscEventsInner::GossipPriorityAuctionRestart { slot_id, previous_winner, end_gas } => {
                assert_eq!(*slot_id, 0);
                assert_eq!(
                    previous_winner,
                    &MiscEventsPreviousWinner::Ip("18.180.228.50".to_string())
                );
                assert!((end_gas - 0.39989171).abs() < f64::EPSILON);
            }
            _ => panic!("expected gossip priority auction restart inner event")
        }
    }

    #[test]
    fn parses_timestrings_to_unix_timestamps() {
        let row = MiscEventsRows {
            local_time:   "2026-06-11T01:00:03.104726053".to_string(),
            block_time:   "2026-06-11T01:00:02.896899363".to_string(),
            block_number: 0,
            events:       Vec::new()
        };

        assert_eq!(row.local_time_unix_ms().unwrap(), 1_781_139_603_104);
        assert_eq!(row.block_time_unix_ms().unwrap(), 1_781_139_602_896);
    }
}
