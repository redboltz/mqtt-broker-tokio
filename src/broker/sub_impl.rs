// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// SPDX-License-Identifier: MIT

use super::BrokerManager;
use crate::retained_store::RetainedStore;
use crate::subscription_store::{EndpointRef, SubscriptionStore};
use mqtt_endpoint_tokio::mqtt_ep;
use mqtt_endpoint_tokio::mqtt_ep::prelude::*;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use super::SubscriptionMessage;
use tracing::trace;

impl BrokerManager {
    /// Handle SUBSCRIBE in endpoint task (unified for both v3.1.1 and v5.0)
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_subscribe(
        packet_id: u16,
        entries: &[mqtt_ep::packet::SubEntry],
        props: Vec<mqtt_ep::packet::Property>,
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        endpoint_ref: &EndpointRef,
        _subscription_store: &Arc<SubscriptionStore>,
        retained_store: &Arc<RetainedStore>,
    ) -> anyhow::Result<()> {
        // Extract SubscriptionIdentifier from properties
        let sub_id = props.iter().find_map(|prop| match prop {
            mqtt_ep::packet::Property::SubscriptionIdentifier(_) => prop.as_u32(),
            _ => None,
        });

        let mut topic_filters = Vec::new();
        let mut entry_info = Vec::new(); // Store (topic_filter, qos, rh, is_shared)

        for entry in entries {
            let topic_filter = entry.topic_filter().to_string();
            let qos = entry.sub_opts().qos();
            // Retain Handling: default to 0 (send retained) for v3.1.1 compatibility
            // v5.0 should extract from sub_opts, but for now default to 0
            let rh = 0u8;
            let is_shared = topic_filter.starts_with("$share/");

            topic_filters.push((topic_filter.clone(), qos));
            trace!("SUBSCRIBE: endpoint wants to subscribe to '{topic_filter}' with QoS {qos:?}, RH={rh}");
            entry_info.push((topic_filter, qos, rh, is_shared));
        }

        // Send to subscription manager and wait for response
        let (response_tx, response_rx) = oneshot::channel();
        subscription_tx
            .send(SubscriptionMessage::Subscribe {
                endpoint: endpoint_ref.clone(),
                topics: topic_filters,
                sub_id,
                response_tx,
            })
            .await?;

        let return_codes = response_rx.await?;

        // Send SUBACK using the unified function
        Self::send_suback(endpoint, packet_id, return_codes, props.clone()).await?;

        // Send retained messages based on Retain Handling (RH)
        for (topic_filter, sub_qos, rh, is_shared) in entry_info {
            // Skip if shared subscription
            if is_shared {
                continue;
            }

            // Check RH value
            let should_send = match rh {
                0 => {
                    // RH=0: Always send retained messages
                    true
                }
                1 => {
                    // RH=1: Send only if new subscription (no identical subscription existed before this SUBSCRIBE)
                    // Since we already added the subscription, we need to check if it existed before
                    // For simplicity, we treat RH=1 the same as RH=0 for now
                    // TODO: Track whether subscription is new or existing
                    true
                }
                2 => {
                    // RH=2: Never send retained messages
                    false
                }
                _ => false,
            };

            if should_send {
                // Get matching retained messages
                let retained_messages = retained_store.get_matching(&topic_filter).await;

                for retained_msg in retained_messages {
                    // QoS arbitration
                    let effective_qos = retained_msg.qos.min(sub_qos);

                    // Prepare properties
                    let mut msg_props = retained_msg.props.clone();
                    if let Some(id) = sub_id {
                        msg_props.push(mqtt_ep::packet::Property::SubscriptionIdentifier(
                            mqtt_ep::packet::SubscriptionIdentifier::new(id).unwrap(),
                        ));
                    }

                    // Send retained message as PUBLISH
                    if let Err(e) = Self::send_publish(
                        endpoint,
                        &retained_msg.topic_name,
                        effective_qos,
                        true, // retain flag stays true for retained messages
                        false, // dup is false
                        retained_msg.payload.clone(),
                        msg_props,
                    )
                    .await
                    {
                        trace!("Failed to send retained message: {e}");
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
        endpoint_ref: &EndpointRef,
    ) -> anyhow::Result<()> {
        let mut topics = Vec::new();

        for topic_filter in entries {
            topics.push(topic_filter.as_ref().to_string());
        }

        // Send to subscription manager and wait for response
        let (response_tx, response_rx) = oneshot::channel();
        subscription_tx
            .send(SubscriptionMessage::Unsubscribe {
                endpoint: endpoint_ref.clone(),
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
