// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// SPDX-License-Identifier: MIT

use super::BrokerManager;
use super::SubscriptionMessage;
use crate::auth_impl::{AuthorizationType, Security};
use crate::retained_store::RetainedStore;
use crate::session_store::{SessionRef, SessionStore};
use crate::subscription_store::SubscriptionStore;
use mqtt_endpoint_tokio::mqtt_ep;
use mqtt_endpoint_tokio::mqtt_ep::prelude::*;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, trace};

impl BrokerManager {
    /// Handle SUBSCRIBE in endpoint task (unified for both v3.1.1 and v5.0)
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_subscribe(
        &self,
        packet_id: u16,
        entries: &[mqtt_ep::packet::SubEntry],
        props: Vec<mqtt_ep::packet::Property>,
        security: &Option<Arc<Security>>,
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        session_ref: &SessionRef,
        _subscription_store: &Arc<SubscriptionStore>,
        retained_store: &Arc<RetainedStore>,
        session_store: &Arc<SessionStore>,
    ) -> anyhow::Result<()> {
        // Get endpoint version to determine if features should be checked
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        // Extract SubscriptionIdentifier from properties
        let sub_id = props.iter().find_map(|prop| match prop {
            mqtt_ep::packet::Property::SubscriptionIdentifier(_) => prop.as_u32(),
            _ => None,
        });

        // Check subscription identifier support (MQTT v5.0 only)
        if endpoint_version == mqtt_ep::Version::V5_0 && sub_id.is_some() && !self.sub_id_support {
            error!("Subscription identifiers not supported but client provided one");
            // Send SUBACK with failure for all entries
            let return_codes = vec![
                mqtt_ep::result_code::SubackReturnCode::Failure;
                entries.len()
            ];
            Self::send_suback(endpoint, packet_id, return_codes, Vec::new()).await?;
            return Ok(());
        }

        let mut topic_filters = Vec::new();
        let mut entry_info = Vec::new(); // Store (topic_filter, qos, rh, rap, nl, is_shared, failure_reason)

        for entry in entries {
            let topic_filter = entry.topic_filter().to_string();
            let sub_opts = entry.sub_opts();
            let qos = sub_opts.qos();
            // Get Retain Handling from sub_opts
            let rh = sub_opts.rh();
            // Get Retain As Published from sub_opts
            let rap = sub_opts.rap();
            // Get No Local from sub_opts (v5.0 only, always false for v3.1.1)
            let nl = if endpoint_version == mqtt_ep::Version::V5_0 {
                sub_opts.nl()
            } else {
                false
            };
            let is_shared = topic_filter.starts_with("$share/");

            // MQTT v5.0 spec [MQTT-3.8.3-4]: Protocol Error if SharedSubscription + nl=true
            if is_shared && nl {
                use tracing::error;
                error!(
                    "Protocol Error: Client sent SUBSCRIBE with SharedSubscription ('{topic_filter}') and NoLocal=true"
                );
                // Send DISCONNECT with ProtocolError reason code
                let disconnect = mqtt_ep::packet::v5_0::Disconnect::builder()
                    .reason_code(mqtt_ep::result_code::DisconnectReasonCode::ProtocolError)
                    .build()
                    .unwrap();
                let _ = endpoint.send(disconnect).await;
                return Err(anyhow::anyhow!(
                    "Protocol Error: SharedSubscription with NoLocal=true"
                ));
            }

            // Feature support checks (MQTT v5.0 only)
            let mut feature_check_failed = None;

            if endpoint_version == mqtt_ep::Version::V5_0 {
                // Check shared subscription support
                if is_shared && !self.shared_sub_support {
                    error!("Shared subscriptions not supported but client tried to use: '{topic_filter}'");
                    feature_check_failed = Some(mqtt_ep::result_code::SubackReasonCode::SharedSubscriptionsNotSupported);
                }

                // Check wildcard subscription support
                if feature_check_failed.is_none()
                    && (topic_filter.contains('+') || topic_filter.contains('#'))
                    && !self.wc_support {
                    error!("Wildcard subscriptions not supported but client tried to use: '{topic_filter}'");
                    feature_check_failed = Some(mqtt_ep::result_code::SubackReasonCode::WildcardSubscriptionsNotSupported);
                }
            }

            // Check authorization if security is configured
            // Special case: Response Topics - only allow exact match to own response topic
            // Check if topic_filter is a Response Topic (no wildcards allowed)
            let is_response_topic_filter = session_store.is_response_topic(&topic_filter).await;

            let authorized = if is_response_topic_filter {
                // This is a Response Topic: check if it matches this session's response topic
                if let Some(session) = session_store.get_session(&session_ref.session_id).await {
                    let session_guard = session.read().await;
                    if let Some(session_response_topic) = session_guard.response_topic() {
                        // Only allow exact match (no wildcards, only own Response Topic)
                        if topic_filter == session_response_topic {
                            trace!(
                                "Authorization: Response Topic '{topic_filter}' - subscribe allowed (matches session's response topic)"
                            );
                            true
                        } else {
                            error!(
                                "Authorization: Response Topic '{topic_filter}' - subscribe denied (does not match session's response topic '{session_response_topic}')"
                            );
                            false
                        }
                    } else {
                        error!(
                            "Authorization: Response Topic '{topic_filter}' - subscribe denied (no response topic for this session)"
                        );
                        false
                    }
                } else {
                    error!(
                        "Authorization: Response Topic '{topic_filter}' - subscribe denied (session not found)"
                    );
                    false
                }
            } else if let Some(ref sec) = security {
                // Normal topic: use regular authorization
                if let Some(ref username) = session_ref.session_id.user_name {
                    let auth_result = sec.auth_sub(&topic_filter, username);
                    match auth_result {
                        AuthorizationType::Allow => {
                            trace!(
                                "Authorization: user '{username}' allowed to subscribe to '{topic_filter}'"
                            );
                            true
                        }
                        AuthorizationType::Deny => {
                            error!(
                                "Authorization: user '{username}' denied to subscribe to '{topic_filter}'"
                            );
                            false
                        }
                        AuthorizationType::None => {
                            error!(
                                "Authorization: no rule found for user '{username}' on '{topic_filter}', denying by default"
                            );
                            false
                        }
                    }
                } else {
                    error!("Authorization: no username in session, denying subscription to '{topic_filter}'");
                    false
                }
            } else {
                // No security configured, allow all
                true
            };

            // Determine if this subscription should be processed
            let should_process = feature_check_failed.is_none() && authorized;

            if should_process {
                topic_filters.push((topic_filter.clone(), qos, rap, nl));
                trace!(
                    "SUBSCRIBE: endpoint wants to subscribe to '{topic_filter}' with QoS {qos:?}, RH={rh:?}, RAP={rap}, NL={nl}"
                );
            } else if feature_check_failed.is_some() {
                // Feature check failed - specific reason code will be returned
                trace!("SUBSCRIBE: subscription to '{topic_filter}' denied due to unsupported feature");
            } else {
                // Authorization failed
                trace!("SUBSCRIBE: subscription to '{topic_filter}' denied by authorization");
            }

            // Store failure reason: Some(reason_code) if failed, None if authorized
            let failure_reason = if let Some(reason_code) = feature_check_failed {
                Some(reason_code)
            } else if !authorized {
                // For v5.0 use NotAuthorized, for v3.1.1 use generic Failure
                if endpoint_version == mqtt_ep::Version::V5_0 {
                    Some(mqtt_ep::result_code::SubackReasonCode::NotAuthorized)
                } else {
                    None // Will be mapped to Failure in v3.1.1
                }
            } else {
                None // Success - will be determined by subscription manager
            };

            entry_info.push((topic_filter, qos, rh, rap, nl, is_shared, failure_reason));
        }

        // Send to subscription manager and wait for response (only for authorized subscriptions)
        let return_codes_with_is_new = if !topic_filters.is_empty() {
            let (response_tx, response_rx) = oneshot::channel();
            subscription_tx
                .send(SubscriptionMessage::Subscribe {
                    session_ref: session_ref.clone(),
                    topics: topic_filters,
                    sub_id,
                    response_tx,
                })
                .await?;
            response_rx.await?
        } else {
            Vec::new()
        };

        // Build return codes for SUBACK based on feature support, authorization and subscription results
        if endpoint_version == mqtt_ep::Version::V5_0 {
            // For v5.0, build SubackReasonCode vec directly
            let mut reason_codes = Vec::new();
            let mut subscription_results_iter = return_codes_with_is_new.iter();

            for (_topic_filter, _qos, _rh, _rap, _nl, _is_shared, failure_reason) in &entry_info {
                if let Some(reason_code) = failure_reason {
                    // Failed due to feature support or authorization
                    reason_codes.push(*reason_code);
                } else {
                    // Success - get result from subscription manager
                    if let Some((rc, _is_new)) = subscription_results_iter.next() {
                        // Convert SubackReturnCode to SubackReasonCode
                        let reason_code = match rc {
                            mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos0 => {
                                mqtt_ep::result_code::SubackReasonCode::GrantedQos0
                            }
                            mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos1 => {
                                mqtt_ep::result_code::SubackReasonCode::GrantedQos1
                            }
                            mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos2 => {
                                mqtt_ep::result_code::SubackReasonCode::GrantedQos2
                            }
                            mqtt_ep::result_code::SubackReturnCode::Failure => {
                                mqtt_ep::result_code::SubackReasonCode::UnspecifiedError
                            }
                        };
                        reason_codes.push(reason_code);
                    } else {
                        // Should not happen, but handle gracefully
                        reason_codes.push(mqtt_ep::result_code::SubackReasonCode::UnspecifiedError);
                    }
                }
            }

            // Send SUBACK for v5.0
            let suback = mqtt_ep::packet::v5_0::Suback::builder()
                .packet_id(packet_id)
                .reason_codes(reason_codes)
                .build()
                .unwrap();
            endpoint.send(suback).await?;
        } else {
            // For v3.1.1, build SubackReturnCode vec
            let mut return_codes = Vec::new();
            let mut subscription_results_iter = return_codes_with_is_new.iter();

            for (_topic_filter, _qos, _rh, _rap, _nl, _is_shared, failure_reason) in &entry_info {
                if failure_reason.is_some() {
                    // Failed - return Failure
                    return_codes.push(mqtt_ep::result_code::SubackReturnCode::Failure);
                } else {
                    // Get result from subscription manager
                    if let Some((rc, _is_new)) = subscription_results_iter.next() {
                        return_codes.push(*rc);
                    } else {
                        // Should not happen, but handle gracefully
                        return_codes.push(mqtt_ep::result_code::SubackReturnCode::Failure);
                    }
                }
            }

            // Send SUBACK for v3.1.1
            let suback = mqtt_ep::packet::v3_1_1::Suback::builder()
                .packet_id(packet_id)
                .return_codes(return_codes)
                .build()
                .unwrap();
            endpoint.send(suback).await?;
        }

        // Send retained messages based on Retain Handling (RH)
        let mut subscription_results_iter = return_codes_with_is_new.iter();
        for (topic_filter, sub_qos, rh, _rap, _nl, is_shared, failure_reason) in &entry_info {
            // Skip if failed (not authorized or feature not supported)
            if failure_reason.is_some() {
                continue;
            }

            // Get subscription result
            let (_rc, is_new) = if let Some(result) = subscription_results_iter.next() {
                result
            } else {
                // Should not happen
                continue;
            };
            // Skip if shared subscription
            if *is_shared {
                continue;
            }

            // Check RH value
            let rh_value = *rh as u8;
            let should_send = match rh_value {
                0 => {
                    // RH=0 (SendRetained): Always send retained messages
                    true
                }
                1 => {
                    // RH=1 (SendRetainedIfNotExists): Send only if new subscription
                    *is_new
                }
                2 => {
                    // RH=2 (DoNotSendRetained): Never send retained messages
                    false
                }
                _ => false,
            };

            if should_send {
                // Get matching retained messages
                let retained_messages = retained_store.get_matching(topic_filter).await;

                for retained_msg in retained_messages {
                    // Check MessageExpiryInterval and update props
                    let (updated_props, is_expired) = BrokerManager::update_message_expiry_interval(
                        &retained_msg.props,
                        retained_msg.stored_at,
                    );

                    if is_expired {
                        trace!(
                            "Retained message for topic '{}' has expired, skipping delivery",
                            retained_msg.topic_name
                        );
                        continue;
                    }

                    // QoS arbitration
                    let effective_qos = retained_msg.qos.min(*sub_qos);

                    // Prepare properties
                    let mut msg_props = updated_props;
                    if let Some(id) = sub_id {
                        msg_props.push(mqtt_ep::packet::Property::SubscriptionIdentifier(
                            mqtt_ep::packet::SubscriptionIdentifier::new(id).unwrap(),
                        ));
                    }

                    // Send retained message as PUBLISH via Session
                    // MQTT spec: retained messages must always have retain=true when delivered
                    // RAP only affects forwarding of new messages, not retained message delivery
                    if let Some(session_arc) =
                        session_store.get_session(&session_ref.session_id).await
                    {
                        let mut session_guard = session_arc.write().await;
                        session_guard
                            .send_publish(
                                retained_msg.topic_name.clone(),
                                effective_qos,
                                true, // retain flag must be true for retained messages (MQTT spec)
                                retained_msg.payload.clone(),
                                msg_props,
                            )
                            .await;
                    } else {
                        trace!(
                            "Session not found for retained message delivery: {:?}",
                            session_ref
                        );
                    }
                }
            }
        }

        trace!("✅ SUBSCRIBE processing completed successfully");
        Ok(())
    }

    /// Handle UNSUBSCRIBE in endpoint task (unified for both v3.1.1 and v5.0)
    pub(super) async fn handle_unsubscribe(
        packet_id: u16,
        entries: &[impl AsRef<str>],
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        session_ref: &SessionRef,
    ) -> anyhow::Result<()> {
        let mut topics = Vec::new();

        for topic_filter in entries {
            topics.push(topic_filter.as_ref().to_string());
        }

        // Send to subscription manager and wait for response
        let (response_tx, response_rx) = oneshot::channel();
        subscription_tx
            .send(SubscriptionMessage::Unsubscribe {
                session_ref: session_ref.clone(),
                topics,
                response_tx,
            })
            .await?;

        let return_codes = response_rx.await?;

        // Send UNSUBACK using the unified function
        Self::send_unsuback(endpoint, packet_id, return_codes).await?;

        trace!("✅ UNSUBSCRIBE processing completed successfully");
        Ok(())
    }

    /// Send SUBACK packet to endpoint (supports both v3.1.1 and v5.0)
    pub(super) async fn send_suback(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        packet_id: u16,
        return_codes: Vec<mqtt_ep::result_code::SubackReturnCode>,
        props: Vec<mqtt_ep::packet::Property>,
    ) -> anyhow::Result<()> {
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                let suback = mqtt_ep::packet::v3_1_1::Suback::builder()
                    .packet_id(packet_id)
                    .return_codes(return_codes)
                    .build()
                    .unwrap();
                endpoint.send(suback).await?;
            }
            mqtt_ep::Version::V5_0 => {
                // Convert SubAckReturnCode to SubackReasonCode for v5.0
                let reason_codes: Vec<mqtt_ep::result_code::SubackReasonCode> = return_codes
                    .into_iter()
                    .map(|rc| match rc {
                        mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos0 => {
                            mqtt_ep::result_code::SubackReasonCode::GrantedQos0
                        }
                        mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos1 => {
                            mqtt_ep::result_code::SubackReasonCode::GrantedQos1
                        }
                        mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos2 => {
                            mqtt_ep::result_code::SubackReasonCode::GrantedQos2
                        }
                        mqtt_ep::result_code::SubackReturnCode::Failure => {
                            mqtt_ep::result_code::SubackReasonCode::UnspecifiedError
                        }
                    })
                    .collect();

                let mut builder = mqtt_ep::packet::v5_0::Suback::builder()
                    .packet_id(packet_id)
                    .reason_codes(reason_codes);

                if !props.is_empty() {
                    builder = builder.props(props);
                }

                let suback = builder.build().unwrap();
                endpoint.send(suback).await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported MQTT version"));
            }
        }

        Ok(())
    }

    /// Send UNSUBACK packet to endpoint (supports both v3.1.1 and v5.0)
    pub(super) async fn send_unsuback(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        packet_id: u16,
        return_codes: Vec<mqtt_ep::result_code::UnsubackReasonCode>,
    ) -> anyhow::Result<()> {
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                // v3.1.1 doesn't have return codes in UNSUBACK
                let unsuback = mqtt_ep::packet::v3_1_1::Unsuback::builder()
                    .packet_id(packet_id)
                    .build()
                    .unwrap();
                endpoint.send(unsuback).await?;
            }
            mqtt_ep::Version::V5_0 => {
                let unsuback = mqtt_ep::packet::v5_0::Unsuback::builder()
                    .packet_id(packet_id)
                    .reason_codes(return_codes)
                    .build()
                    .unwrap();
                endpoint.send(unsuback).await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported MQTT version"));
            }
        }

        Ok(())
    }
}
