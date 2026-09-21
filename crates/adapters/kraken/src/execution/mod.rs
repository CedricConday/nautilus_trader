// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live execution client implementations for the Kraken adapter.
//!
//! This module provides separate execution clients for Kraken Spot and Futures markets:
//!
//! - [`KrakenSpotExecutionClient`] - For Spot markets using WebSocket v2
//! - [`KrakenFuturesExecutionClient`] - For Futures markets
//!
//! # Supported Operations
//!
//! ## Common
//! - Order submission (market, limit, stop)
//! - Order modification
//! - Order cancellation (single, batch, cancel-all)
//! - Account state and balance queries
//!
//! ## Futures Only
//! - Position management

mod futures;
mod spot;

pub use futures::KrakenFuturesExecutionClient;
use nautilus_common::{
    cache::Cache,
    messages::execution::{BatchCancelOrders, CancelAllOrders, CancelOrder},
};
use nautilus_live::execution::failure::CommandFailure;
use nautilus_model::{
    identifiers::{AccountId, ClientOrderId, StrategyId, VenueOrderId},
    orders::Order,
};
pub use spot::KrakenSpotExecutionClient;

use crate::{
    common::enums::KrakenApiResult,
    http::{
        error::{
            KrakenBatchOrderError, KrakenHttpError, KrakenModifyOrderError, KrakenSubmitOrderError,
        },
        futures::client::{FuturesBatchSubmitItem, is_futures_submit_rejection},
        spot::models::SpotBatchOrderResponse,
    },
};

/// Builds the explicit per-order cancels a [`CancelAllOrders`] command selects.
///
/// Kraken's account-wide cancel-all reaches orders on instruments the command never named, and
/// its symbol-scoped form cannot express a side, so both clients resolve the command against the
/// cache and cancel the selected orders by ID. Each cancel keeps its own order's strategy and
/// venue order ID so a per-order result can be attributed, and carries the command's tracing
/// identifiers.
///
/// Orders belonging to another account are skipped. The instrument already scopes the selection
/// to this venue, but a second client on the same venue would otherwise have its orders swept up.
fn cancels_for_cancel_all(
    cache: &Cache,
    cmd: &CancelAllOrders,
    account_id: AccountId,
) -> Vec<CancelOrder> {
    cache
        .orders_open(None, Some(&cmd.instrument_id), None, None, cmd.order_side)
        .into_iter()
        .filter(|order| {
            order
                .account_id()
                .is_none_or(|order_account_id| order_account_id == account_id)
        })
        .map(|order| {
            cancel_from_cancel_all(
                cmd,
                order.client_order_id(),
                order.venue_order_id(),
                order.strategy_id(),
            )
        })
        .collect()
}

/// Builds one [`CancelOrder`] for `client_order_id` from a [`CancelAllOrders`] command.
fn cancel_from_cancel_all(
    cmd: &CancelAllOrders,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
    strategy_id: StrategyId,
) -> CancelOrder {
    CancelOrder {
        trader_id: cmd.trader_id,
        client_id: cmd.client_id,
        strategy_id,
        instrument_id: cmd.instrument_id,
        client_order_id,
        venue_order_id,
        command_id: cmd.command_id,
        ts_init: cmd.ts_init,
        params: cmd.params.clone(),
        correlation_id: cmd.correlation_id,
        causation_id: cmd.causation_id,
    }
}

/// Builds the [`BatchCancelOrders`] command that cancels `cancels` through the client's own
/// batch-cancel path, so a cancel-all reaches the venue by the same transport, and honours the
/// same per-call options, as an explicit batch cancel.
fn batch_cancel_from_cancel_all(
    cmd: &CancelAllOrders,
    cancels: Vec<CancelOrder>,
) -> BatchCancelOrders {
    BatchCancelOrders {
        trader_id: cmd.trader_id,
        client_id: cmd.client_id,
        strategy_id: cmd.strategy_id,
        instrument_id: cmd.instrument_id,
        cancels,
        command_id: cmd.command_id,
        ts_init: cmd.ts_init,
        params: cmd.params.clone(),
        correlation_id: cmd.correlation_id.or(Some(cmd.command_id)),
        causation_id: Some(cmd.command_id),
    }
}

fn command_failure_from_submit_error(error: &anyhow::Error) -> CommandFailure {
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<KrakenSubmitOrderError>() {
            return match error {
                KrakenSubmitOrderError::Rejected { reason } => {
                    CommandFailure::venue_rejected(reason)
                }
                KrakenSubmitOrderError::MissingStatus
                | KrakenSubmitOrderError::UnknownStatus { .. }
                | KrakenSubmitOrderError::MissingOrderId { .. }
                | KrakenSubmitOrderError::PostSubmitLookup { .. } => {
                    CommandFailure::ambiguous(error.to_string())
                }
            };
        }
    }

    command_failure_from_order_error(error, true)
}

fn command_failure_from_modify_error(error: &anyhow::Error) -> CommandFailure {
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<KrakenModifyOrderError>() {
            return match error {
                KrakenModifyOrderError::Rejected { reason } => {
                    CommandFailure::venue_rejected(reason)
                }
                KrakenModifyOrderError::UnknownStatus { .. }
                | KrakenModifyOrderError::MissingOrderId => {
                    CommandFailure::ambiguous(error.to_string())
                }
            };
        }
    }

    command_failure_from_order_error(error, true)
}

fn command_failure_from_spot_batch_error(error: &anyhow::Error) -> CommandFailure {
    command_failure_from_batch_error(error, true)
}

fn command_failure_from_futures_batch_error(error: &anyhow::Error) -> CommandFailure {
    command_failure_from_batch_error(error, false)
}

fn command_failure_from_batch_error(
    error: &anyhow::Error,
    whole_order_rejection: bool,
) -> CommandFailure {
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<KrakenBatchOrderError>() {
            return match error {
                KrakenBatchOrderError::Validation { .. } | KrakenBatchOrderError::NotAttempted => {
                    CommandFailure::not_sent(error.to_string())
                }
                KrakenBatchOrderError::ResponseCount { .. }
                | KrakenBatchOrderError::MissingResponse { .. }
                | KrakenBatchOrderError::DuplicateResponse { .. } => {
                    CommandFailure::ambiguous(error.to_string())
                }
            };
        }
    }

    command_failure_from_order_error(error, whole_order_rejection)
}

fn command_failure_from_order_error(
    error: &anyhow::Error,
    order_api_rejection: bool,
) -> CommandFailure {
    let reason = error.to_string();

    for cause in error.chain() {
        let Some(error) = cause.downcast_ref::<KrakenHttpError>() else {
            continue;
        };

        return match error {
            KrakenHttpError::RequestNotStarted(_) | KrakenHttpError::MissingCredentials => {
                CommandFailure::not_sent(reason)
            }
            KrakenHttpError::ApiError(errors)
                if order_api_rejection && contains_spot_order_rejection(errors) =>
            {
                CommandFailure::venue_rejected(format_api_errors(errors))
            }
            KrakenHttpError::NetworkError(_)
            | KrakenHttpError::ApiError(_)
            | KrakenHttpError::ParseError(_)
            | KrakenHttpError::AuthenticationError(_) => CommandFailure::ambiguous(reason),
        };
    }

    CommandFailure::not_sent(reason)
}

fn command_failure_from_spot_batch_item(
    item: SpotBatchOrderResponse,
) -> Result<(), CommandFailure> {
    match (item.txid, item.error) {
        (Some(_), None) => Ok(()),
        (None, Some(reason)) => Err(CommandFailure::venue_rejected(reason)),
        (Some(_), Some(_)) => Err(CommandFailure::ambiguous(
            "Batch item response contained both a transaction ID and an error",
        )),
        (None, None) => Err(CommandFailure::ambiguous(
            "Batch item response had no transaction ID or error",
        )),
    }
}

fn command_failure_from_futures_batch_item(
    item: FuturesBatchSubmitItem,
) -> Result<(), CommandFailure> {
    let status = item.status.status;

    if item.result != KrakenApiResult::Success {
        return if is_futures_submit_rejection(&status) {
            Err(CommandFailure::venue_rejected(status))
        } else {
            Err(CommandFailure::ambiguous(format!(
                "Batch response reported an error with item status: {status}"
            )))
        };
    }

    match status.as_str() {
        "placed" | "filled" => Ok(()),
        reason if is_futures_submit_rejection(reason) => {
            Err(CommandFailure::venue_rejected(reason))
        }
        "" => Err(CommandFailure::ambiguous("Empty batch item status")),
        reason => Err(CommandFailure::ambiguous(format!(
            "Unknown batch item status: {reason}"
        ))),
    }
}

fn command_failure_from_cancel_error(error: KrakenHttpError) -> CommandFailure {
    match error {
        KrakenHttpError::RequestNotStarted(message) => CommandFailure::not_sent(message),
        KrakenHttpError::AuthenticationError(message) => CommandFailure::not_sent(message),
        KrakenHttpError::MissingCredentials => CommandFailure::not_sent("Missing credentials"),
        KrakenHttpError::NetworkError(message) | KrakenHttpError::ParseError(message) => {
            CommandFailure::ambiguous(message)
        }
        KrakenHttpError::ApiError(message) => {
            CommandFailure::ambiguous(format_cancel_api_errors(&message))
        }
    }
}

fn command_failure_from_spot_cancel_error(error: KrakenHttpError) -> CommandFailure {
    match error {
        KrakenHttpError::ApiError(message) if contains_spot_cancel_rejection(&message) => {
            CommandFailure::venue_rejected(format_cancel_api_errors(&message))
        }
        KrakenHttpError::ApiError(message) => {
            CommandFailure::ambiguous(format_cancel_api_errors(&message))
        }
        other => command_failure_from_cancel_error(other),
    }
}

fn contains_spot_order_rejection(errors: &[String]) -> bool {
    errors.iter().any(|e| e.trim_start().starts_with("EOrder:"))
}

fn contains_spot_cancel_rejection(errors: &[String]) -> bool {
    contains_spot_order_rejection(errors)
}

fn format_api_errors(errors: &[String]) -> String {
    if errors.is_empty() {
        "unknown error (empty error list)".to_string()
    } else {
        errors.join(", ")
    }
}

fn format_cancel_api_errors(errors: &[String]) -> String {
    format_api_errors(errors)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_post_submit_phase_overrides_nested_not_sent_error() {
        let error = anyhow::Error::new(KrakenSubmitOrderError::PostSubmitLookup {
            source: KrakenHttpError::RequestNotStarted("lookup was not sent".to_string()).into(),
        });

        assert_eq!(
            command_failure_from_submit_error(&error),
            CommandFailure::Ambiguous(
                "Order lookup failed after submission: Request not started: lookup was not sent"
                    .to_string()
            )
        );
    }

    #[rstest]
    fn test_request_not_started_is_not_sent() {
        let error = anyhow::Error::new(KrakenHttpError::RequestNotStarted(
            "request encoding failed".to_string(),
        ));

        assert_eq!(
            command_failure_from_submit_error(&error),
            CommandFailure::NotSent("Request not started: request encoding failed".to_string())
        );
    }
}
