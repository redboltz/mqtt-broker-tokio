// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// SPDX-License-Identifier: MIT

use super::BrokerManager;
use crate::retained_store::RetainedStore;
use crate::session_store::SessionStore;
use crate::subscription_store::SubscriptionStore;
use mqtt_endpoint_tokio::mqtt_ep;
use mqtt_endpoint_tokio::mqtt_ep::prelude::*;
use std::sync::Arc;
use tracing::trace;

impl BrokerManager {
    pub(super) async fn send_publish(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        topic: &str,
        qos: mqtt_ep::packet::Qos,
        retain: bool,
        dup: bool,
        payload: impl IntoPayload,
        props: Vec<mqtt_ep::packet::Property>,
    ) -> anyhow::Result<()> {
        // Determine MQTT version from endpoint
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                // Create v3.1.1 PUBLISH packet
                let mut builder = mqtt_ep::packet::v3_1_1::Publish::builder()
                    .topic_name(topic)
                    .unwrap()
                    .qos(qos)
                    .retain(retain)
                    .dup(dup)
                    .payload(payload);

                let publish_packet = if qos != mqtt_ep::packet::Qos::AtMostOnce {
                    // Acquire proper packet ID for QoS > 0
                    let packet_id = endpoint.acquire_packet_id().await.unwrap();
                    builder = builder.packet_id(packet_id);
                    builder.build().unwrap()
                } else {
                    builder.build().unwrap()
                };

                endpoint.send(publish_packet).await?;
            }
            mqtt_ep::Version::V5_0 => {
                // Create v5.0 PUBLISH packet
                let mut builder = mqtt_ep::packet::v5_0::Publish::builder()
                    .topic_name(topic)
                    .unwrap()
                    .qos(qos)
                    .retain(retain)
                    .dup(dup)
                    .payload(payload);

                // Add properties if present
                if !props.is_empty() {
                    builder = builder.props(props);
                }

                let publish_packet = if qos != mqtt_ep::packet::Qos::AtMostOnce {
                    // Acquire proper packet ID for QoS > 0
                    let packet_id = endpoint.acquire_packet_id().await.unwrap_or(1);
                    builder = builder.packet_id(packet_id);
                    builder.build().unwrap()
                } else {
                    builder.build().unwrap()
                };

                endpoint.send(publish_packet).await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported MQTT version"));
            }
        }

        Ok(())
    }

    /// Send PUBACK packet to endpoint (supports both v3.1.1 and v5.0)

    pub(super) async fn send_puback(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        packet_id: u16,
        reason_code: mqtt_ep::result_code::PubackReasonCode,
        props: Vec<mqtt_ep::packet::Property>,
    ) -> anyhow::Result<()> {
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                let puback = mqtt_ep::packet::v3_1_1::Puback::builder()
                    .packet_id(packet_id)
                    .build()
                    .unwrap();
                endpoint.send(puback).await?;
            }
            mqtt_ep::Version::V5_0 => {
                let mut builder = mqtt_ep::packet::v5_0::Puback::builder()
                    .packet_id(packet_id)
                    .reason_code(reason_code);

                if !props.is_empty() {
                    builder = builder.props(props);
                }

                let puback = builder.build().unwrap();
                endpoint.send(puback).await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported MQTT version"));
            }
        }

        Ok(())
    }

    /// Send PUBREC packet to endpoint (supports both v3.1.1 and v5.0)
    pub(super) async fn send_pubrec(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        packet_id: u16,
        reason_code: mqtt_ep::result_code::PubrecReasonCode,
        props: Vec<mqtt_ep::packet::Property>,
    ) -> anyhow::Result<()> {
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                let pubrec = mqtt_ep::packet::v3_1_1::Pubrec::builder()
                    .packet_id(packet_id)
                    .build()
                    .unwrap();
                endpoint.send(pubrec).await?;
            }
            mqtt_ep::Version::V5_0 => {
                let mut builder = mqtt_ep::packet::v5_0::Pubrec::builder()
                    .packet_id(packet_id)
                    .reason_code(reason_code);

                if !props.is_empty() {
                    builder = builder.props(props);
                }

                let pubrec = builder.build().unwrap();
                endpoint.send(pubrec).await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported MQTT version"));
            }
        }

        Ok(())
    }

    /// Send PUBREL packet to endpoint (supports both v3.1.1 and v5.0)
    pub(super) async fn send_pubrel(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        packet_id: u16,
        reason_code: mqtt_ep::result_code::PubrelReasonCode,
        props: Vec<mqtt_ep::packet::Property>,
    ) -> anyhow::Result<()> {
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                let pubrel = mqtt_ep::packet::v3_1_1::Pubrel::builder()
                    .packet_id(packet_id)
                    .build()
                    .unwrap();
                endpoint.send(pubrel).await?;
            }
            mqtt_ep::Version::V5_0 => {
                let mut builder = mqtt_ep::packet::v5_0::Pubrel::builder()
                    .packet_id(packet_id)
                    .reason_code(reason_code);

                if !props.is_empty() {
                    builder = builder.props(props);
                }

                let pubrel = builder.build().unwrap();
                endpoint.send(pubrel).await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported MQTT version"));
            }
        }

        Ok(())
    }

    /// Send PUBCOMP packet to endpoint (supports both v3.1.1 and v5.0)
    pub(super) async fn send_pubcomp(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        packet_id: u16,
        reason_code: mqtt_ep::result_code::PubcompReasonCode,
        props: Vec<mqtt_ep::packet::Property>,
    ) -> anyhow::Result<()> {
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                let pubcomp = mqtt_ep::packet::v3_1_1::Pubcomp::builder()
                    .packet_id(packet_id)
                    .build()
                    .unwrap();
                endpoint.send(pubcomp).await?;
            }
            mqtt_ep::Version::V5_0 => {
                let mut builder = mqtt_ep::packet::v5_0::Pubcomp::builder()
                    .packet_id(packet_id)
                    .reason_code(reason_code);

                if !props.is_empty() {
                    builder = builder.props(props);
                }

                let pubcomp = builder.build().unwrap();
                endpoint.send(pubcomp).await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported MQTT version"));
            }
        }

        Ok(())
    }

    /// Send SUBACK packet to endpoint (supports both v3.1.1 and v5.0)

    /// Handle PUBLISH packet (unified for both v3.1.1 and v5.0)
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_publish(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        packet_id: u16,
        topic: &str,
        qos: mqtt_ep::packet::Qos,
        retain: bool,
        _dup: bool,
        payload: impl IntoPayload,
        publisher_props: Vec<mqtt_ep::packet::Property>,
        subscription_store: &Arc<SubscriptionStore>,
        retained_store: &Arc<RetainedStore>,
        session_store: &Arc<SessionStore>,
    ) -> anyhow::Result<()> {
        // Convert to ArcPayload early for retained message storage
        let arc_payload = payload.into_payload();

        // Handle retained message
        if retain {
            if arc_payload.is_empty() {
                // Empty payload with retain flag: remove retained message
                retained_store.remove(topic).await;
                trace!("Removed retained message for topic '{topic}'");
            } else {
                // Non-empty payload with retain flag: store/update retained message
                retained_store
                    .store(topic, qos, arc_payload.clone(), publisher_props.clone())
                    .await;
                trace!("Stored retained message for topic '{topic}' with QoS {qos:?}");
            }
        }

        let subscriptions = subscription_store.find_subscribers(topic).await;
        let has_subscribers = !subscriptions.is_empty();

        if !has_subscribers {
            trace!("No subscribers found for topic '{topic}'");
        }

        // Send to subscribers BEFORE sending QoS response
        // This ensures retained message storage and distribution complete before PUBACK/PUBREC
        if has_subscribers {
            // Send to subscribers sequentially (each endpoint.send() queues via mpsc)
            for subscription in subscriptions {
                // QoS arbitration: use the lower of publish QoS and subscription QoS
                let effective_qos = qos.min(subscription.qos);

                // RAP (Retain As Published): if rap is false, always send with retain=false
                // if rap is true, send with the original retain flag
                let effective_retain = if subscription.rap { retain } else { false };

                // Prepare properties for v5.0
                let mut props = publisher_props.clone();
                if let Some(sub_id) = subscription.sub_id {
                    props.push(mqtt_ep::packet::Property::SubscriptionIdentifier(
                        mqtt_ep::packet::SubscriptionIdentifier::new(sub_id).unwrap(),
                    ));
                }

                // Get session and send PUBLISH
                if let Some(session_arc) = session_store
                    .get_session(&subscription.session_ref.session_id)
                    .await
                {
                    let mut session_guard = session_arc.write().await;
                    session_guard
                        .send_publish(
                            topic.to_string(),
                            effective_qos,
                            effective_retain,
                            arc_payload.clone(),
                            props,
                        )
                        .await;
                } else {
                    trace!(
                        "Session not found for subscription: {:?}",
                        subscription.session_ref
                    );
                }
            }
        }

        // Send QoS response AFTER distribution completes
        match qos {
            mqtt_ep::packet::Qos::AtMostOnce => {
                // QoS 0: No response needed
            }
            mqtt_ep::packet::Qos::AtLeastOnce => {
                // QoS 1: Send PUBACK
                let endpoint_version = endpoint
                    .get_protocol_version()
                    .await
                    .unwrap_or(mqtt_ep::Version::V5_0);

                let (reason_code, props) =
                    if endpoint_version == mqtt_ep::Version::V5_0 && !has_subscribers {
                        (
                            mqtt_ep::result_code::PubackReasonCode::NoMatchingSubscribers,
                            Vec::new(),
                        )
                    } else {
                        (mqtt_ep::result_code::PubackReasonCode::Success, Vec::new())
                    };

                Self::send_puback(endpoint, packet_id, reason_code, props).await?;
            }
            mqtt_ep::packet::Qos::ExactlyOnce => {
                // QoS 2: Send PUBREC
                let endpoint_version = endpoint
                    .get_protocol_version()
                    .await
                    .unwrap_or(mqtt_ep::Version::V5_0);

                let (reason_code, props) =
                    if endpoint_version == mqtt_ep::Version::V5_0 && !has_subscribers {
                        (
                            mqtt_ep::result_code::PubrecReasonCode::NoMatchingSubscribers,
                            Vec::new(),
                        )
                    } else {
                        (mqtt_ep::result_code::PubrecReasonCode::Success, Vec::new())
                    };

                Self::send_pubrec(endpoint, packet_id, reason_code, props).await?;
            }
        }

        Ok(())
    }
}
