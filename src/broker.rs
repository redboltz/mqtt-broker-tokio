// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use mqtt_endpoint_tokio::mqtt_ep;
use mqtt_endpoint_tokio::mqtt_ep::prelude::*;

use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, trace};
use uuid::Uuid;

use crate::subscription_store::{EndpointRef, SubscriptionStore};

/// Messages sent to BrokerManager for subscription management
#[derive(Debug)]
pub enum SubscriptionMessage {
    Subscribe {
        endpoint: EndpointRef,
        topics: Vec<(String, mqtt_ep::packet::Qos)>,
        sub_id: Option<u32>,
        response_tx: oneshot::Sender<Vec<mqtt_ep::result_code::SubackReturnCode>>,
    },
    Unsubscribe {
        endpoint: EndpointRef,
        topics: Vec<String>,
        response_tx: oneshot::Sender<Vec<mqtt_ep::result_code::UnsubackReasonCode>>,
    },
    ClientDisconnected {
        endpoint: EndpointRef,
    },
}

/// Main broker manager coordinating all client connections
#[derive(Clone)]
pub struct BrokerManager {
    /// Global subscription store (shared for publish processing)
    subscription_store: Arc<SubscriptionStore>,

    /// Channel to send subscription management messages
    subscription_tx: mpsc::Sender<SubscriptionMessage>,

    /// Endpoint receive buffer size
    ep_recv_buf_size: Option<usize>,
}

impl BrokerManager {
    /// Create a new broker manager
    pub async fn new(ep_recv_buf_size: Option<usize>) -> anyhow::Result<Self> {
        let subscription_store = Arc::new(SubscriptionStore::new());
        let (subscription_tx, subscription_rx) = mpsc::channel(1000);

        // Spawn subscription management task
        let store_for_task = subscription_store.clone();
        tokio::spawn(async move {
            Self::subscription_manager_task(store_for_task, subscription_rx).await;
        });

        Ok(Self {
            subscription_store,
            subscription_tx,
            ep_recv_buf_size,
        })
    }

    /// Handle a new client connection using mqtt-endpoint-tokio
    pub async fn handle_connection<T>(&self, transport: T) -> anyhow::Result<()>
    where
        T: mqtt_ep::transport::TransportOps + Send + 'static,
    {
        // Create Endpoint with Version::Undetermined for dual-version support
        let endpoint: mqtt_ep::Endpoint<mqtt_ep::role::Server> =
            mqtt_ep::Endpoint::new(mqtt_ep::Version::Undetermined);

        // Attach the connection (transport setup)
        let mut opts_builder = mqtt_ep::connection_option::ConnectionOption::builder()
            .auto_pub_response(false)
            .auto_ping_response(false)
            .auto_map_topic_alias_send(false)
            .auto_replace_topic_alias_send(false)
            .connection_establish_timeout_ms(10_000u64)
            .shutdown_timeout_ms(5_000u64);

        if let Some(recv_buf_size) = self.ep_recv_buf_size {
            opts_builder = opts_builder.recv_buffer_size(recv_buf_size);
        }

        let opts = opts_builder
            .build()
            .expect("ConnectionOption should be valid");
        match endpoint
            .attach_with_options(transport, mqtt_endpoint_tokio::mqtt_ep::Mode::Server, opts)
            .await
        {
            Ok(()) => {
                trace!("Transport connection accepted successfully");
            }
            Err(e) => {
                error!("Failed to accept transport connection: {e}");
                return Err(anyhow::anyhow!("Transport connection failed: {e}"));
            }
        };

        // Spawn dedicated endpoint task for this client
        let subscription_store_for_endpoint = self.subscription_store.clone();
        let subscription_tx_for_endpoint = self.subscription_tx.clone();
        let broker_manager_for_cleanup = self.clone();

        tokio::spawn(async move {
            // Handle client endpoint
            let endpoint_ref = Self::handle_client_endpoint(
                endpoint,
                subscription_store_for_endpoint,
                subscription_tx_for_endpoint,
            )
            .await;

            // Clean up when endpoint task finishes
            if let Some(endpoint_ref) = endpoint_ref {
                if let Err(e) = broker_manager_for_cleanup
                    .handle_client_disconnect(&endpoint_ref)
                    .await
                {
                    error!("Error during client disconnect cleanup: {e}");
                }
            }
        });

        trace!("✅ Connection accepted, endpoint task starting");
        Ok(())
    }

    /// Handle client disconnection cleanup
    async fn handle_client_disconnect(&self, endpoint_ref: &EndpointRef) -> anyhow::Result<()> {
        trace!("Starting disconnect cleanup for endpoint");

        // Remove from subscription store via message
        let _ = self
            .subscription_tx
            .send(SubscriptionMessage::ClientDisconnected {
                endpoint: endpoint_ref.clone(),
            })
            .await;

        trace!("Endpoint disconnected and cleaned up");
        Ok(())
    }

    /// Subscription management task - handles subscribe/unsubscribe requests
    async fn subscription_manager_task(
        subscription_store: Arc<SubscriptionStore>,
        mut subscription_rx: mpsc::Receiver<SubscriptionMessage>,
    ) {
        trace!("Subscription manager task started");

        while let Some(message) = subscription_rx.recv().await {
            match message {
                SubscriptionMessage::Subscribe {
                    endpoint,
                    topics,
                    sub_id,
                    response_tx,
                } => {
                    let mut return_codes = Vec::new();

                    for (topic_filter, qos) in topics {
                        match subscription_store
                            .subscribe(endpoint.clone(), &topic_filter, qos, sub_id)
                            .await
                        {
                            Ok(()) => {
                                // Convert QoS to SubAckReturnCode
                                let return_code = match qos {
                                    mqtt_ep::packet::Qos::AtMostOnce => {
                                        mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos0
                                    }
                                    mqtt_ep::packet::Qos::AtLeastOnce => {
                                        mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos1
                                    }
                                    mqtt_ep::packet::Qos::ExactlyOnce => {
                                        mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos2
                                    }
                                };
                                return_codes.push(return_code);
                                trace!("Registered subscription: endpoint topic='{topic_filter}'");
                            }
                            Err(_) => {
                                return_codes.push(mqtt_ep::result_code::SubackReturnCode::Failure);
                            }
                        }
                    }

                    let _ = response_tx.send(return_codes);
                }
                SubscriptionMessage::Unsubscribe {
                    endpoint,
                    topics,
                    response_tx,
                } => {
                    let mut return_codes = Vec::new();

                    for topic_filter in topics {
                        let _ = subscription_store
                            .unsubscribe(&endpoint, &topic_filter)
                            .await;
                        return_codes.push(mqtt_ep::result_code::UnsubackReasonCode::Success);
                        trace!("Removed subscription: endpoint topic='{topic_filter}'");
                    }

                    let _ = response_tx.send(return_codes);
                }
                SubscriptionMessage::ClientDisconnected { endpoint } => {
                    subscription_store.unsubscribe_all(&endpoint).await;
                    trace!("Removed all subscriptions for disconnected endpoint");
                }
            }
        }

        trace!("Subscription manager task finished");
    }

    /// Handle client endpoint in dedicated task
    async fn handle_client_endpoint(
        endpoint: mqtt_ep::Endpoint<mqtt_ep::role::Server>,
        subscription_store: Arc<SubscriptionStore>,
        subscription_tx: mpsc::Sender<SubscriptionMessage>,
    ) -> Option<EndpointRef> {
        trace!("Starting endpoint task (waiting for CONNECT)");

        // First, wait for CONNECT packet
        let client_id = match endpoint.recv().await {
            Ok(packet) => {
                trace!("🔍 Received first packet: {:?}", packet.packet_type());
                match &packet {
                    mqtt_ep::packet::Packet::V3_1_1Connect(connect) => {
                        let extracted_id = if connect.client_id().is_empty() {
                            format!("auto-{}", Uuid::new_v4().simple())
                        } else {
                            connect.client_id().to_string()
                        };
                        trace!("MQTT v3.1.1 client {extracted_id} connected");

                        // Send CONNACK for v3.1.1
                        let connack = mqtt_ep::packet::v3_1_1::Connack::builder()
                            .session_present(false)
                            .return_code(mqtt_ep::result_code::ConnectReturnCode::Accepted)
                            .build()
                            .unwrap();

                        trace!("Sending CONNACK to client {extracted_id}...");
                        if let Err(e) = endpoint.send(connack).await {
                            error!("Failed to send CONNACK: {e}");
                            return None;
                        }

                        trace!("CONNACK successfully sent to client {extracted_id}");
                        extracted_id
                    }
                    mqtt_ep::packet::Packet::V5_0Connect(connect) => {
                        let extracted_id = if connect.client_id().is_empty() {
                            format!("auto-{}", Uuid::new_v4().simple())
                        } else {
                            connect.client_id().to_string()
                        };
                        trace!("MQTT v5.0 client {extracted_id} connected");

                        // Send CONNACK for v5.0
                        let connack = mqtt_ep::packet::v5_0::Connack::builder()
                            .session_present(false)
                            .reason_code(mqtt_ep::result_code::ConnectReasonCode::Success)
                            .build()
                            .unwrap();

                        trace!("Sending CONNACK to client {extracted_id}...");
                        if let Err(e) = endpoint.send(connack).await {
                            error!("Failed to send CONNACK: {e}");
                            return None;
                        }

                        trace!("CONNACK successfully sent to client {extracted_id}");
                        extracted_id
                    }
                    _ => {
                        error!("Expected CONNECT packet, received: {packet:?}");
                        return None;
                    }
                }
            }
            Err(e) => {
                error!("Failed to receive CONNECT packet: {e}");
                return None;
            }
        };

        // Create endpoint reference
        let endpoint_arc = Arc::new(endpoint);
        let endpoint_ref = EndpointRef::new(endpoint_arc.clone());
        trace!("Registered client {client_id}");

        trace!("Starting main endpoint loop for client {client_id}");

        // Main packet processing loop
        loop {
            // trace!("🔄 [{}] Waiting for packet...", client_id);
            match endpoint_arc.recv().await {
                Ok(packet) => {
                    // trace!("📥 [{}] Received packet: {:?}", client_id, packet.packet_type());

                    // Handle packet directly
                    if let Err(e) = Self::handle_received_packet_in_endpoint(
                        &client_id,
                        &packet,
                        &subscription_store,
                        &subscription_tx,
                        &endpoint_arc,
                        &endpoint_ref,
                    )
                    .await
                    {
                        error!("❌ Error handling packet from client {client_id}: {e}");
                        break;
                    }
                }
                Err(e) => {
                    info!("❌ Connection error for client {client_id}: {e}");
                    break;
                }
            }
        }

        trace!("Endpoint task finished for client {client_id}");
        Some(endpoint_ref)
    }

    /// Handle received packet in endpoint task (direct processing)
    async fn handle_received_packet_in_endpoint(
        client_id: &str,
        packet: &mqtt_ep::packet::Packet,
        subscription_store: &Arc<SubscriptionStore>,
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        endpoint_ref: &EndpointRef,
    ) -> anyhow::Result<()> {
        match packet {
            mqtt_ep::packet::Packet::V3_1_1Subscribe(sub) => {
                Self::handle_subscribe(
                    sub.packet_id(),
                    sub.entries(),
                    Vec::new(),
                    subscription_tx,
                    endpoint,
                    endpoint_ref,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V5_0Subscribe(sub) => {
                Self::handle_subscribe(
                    sub.packet_id(),
                    sub.entries(),
                    sub.props().to_vec(),
                    subscription_tx,
                    endpoint,
                    endpoint_ref,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V3_1_1Unsubscribe(unsub) => {
                Self::handle_unsubscribe_in_endpoint(
                    unsub,
                    subscription_tx,
                    endpoint,
                    endpoint_ref,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V5_0Unsubscribe(unsub) => {
                Self::handle_unsubscribe_in_endpoint_v5(
                    unsub,
                    subscription_tx,
                    endpoint,
                    endpoint_ref,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V3_1_1Publish(pub_packet) => {
                Self::handle_publish(
                    endpoint,
                    pub_packet.packet_id().unwrap_or(0),
                    pub_packet.topic_name(),
                    pub_packet.qos(),
                    pub_packet.retain(),
                    pub_packet.dup(),
                    pub_packet.payload().clone(),
                    Vec::new(),
                    subscription_store,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V5_0Publish(pub_packet) => {
                Self::handle_publish(
                    endpoint,
                    pub_packet.packet_id().unwrap_or(0),
                    pub_packet.topic_name(),
                    pub_packet.qos(),
                    pub_packet.retain(),
                    pub_packet.dup(),
                    pub_packet.payload().clone(),
                    pub_packet.props().to_vec(),
                    subscription_store,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V3_1_1Puback(_puback) => {
                // QoS 1: Received PUBACK (client acknowledged our publish)
                trace!("Received PUBACK from client {client_id}");
            }
            mqtt_ep::packet::Packet::V5_0Puback(_puback) => {
                // QoS 1: Received PUBACK (client acknowledged our publish)
                trace!("Received PUBACK from client {client_id}");
            }
            mqtt_ep::packet::Packet::V3_1_1Pubrec(pubrec) => {
                // QoS 2: Received PUBREC, send PUBREL
                Self::send_pubrel(
                    endpoint,
                    pubrec.packet_id(),
                    mqtt_ep::result_code::PubrelReasonCode::Success,
                    Vec::new(),
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V5_0Pubrec(pubrec) => {
                // QoS 2: Received PUBREC, send PUBREL
                Self::send_pubrel(
                    endpoint,
                    pubrec.packet_id(),
                    mqtt_ep::result_code::PubrelReasonCode::Success,
                    Vec::new(),
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V3_1_1Pubrel(pubrel) => {
                // QoS 2: Received PUBREL, send PUBCOMP
                Self::send_pubcomp(
                    endpoint,
                    pubrel.packet_id(),
                    mqtt_ep::result_code::PubcompReasonCode::Success,
                    Vec::new(),
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V5_0Pubrel(pubrel) => {
                // QoS 2: Received PUBREL, send PUBCOMP
                Self::send_pubcomp(
                    endpoint,
                    pubrel.packet_id(),
                    mqtt_ep::result_code::PubcompReasonCode::Success,
                    Vec::new(),
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V3_1_1Pubcomp(_pubcomp) => {
                // QoS 2: Received PUBCOMP (client acknowledged our PUBREL)
                trace!("Received PUBCOMP from client {client_id}");
            }
            mqtt_ep::packet::Packet::V5_0Pubcomp(_pubcomp) => {
                // QoS 2: Received PUBCOMP (client acknowledged our PUBREL)
                trace!("Received PUBCOMP from client {client_id}");
            }
            mqtt_ep::packet::Packet::V3_1_1Pingreq(_) => {
                Self::send_pingresp(client_id, endpoint).await?;
            }
            mqtt_ep::packet::Packet::V5_0Pingreq(_) => {
                Self::send_pingresp(client_id, endpoint).await?;
            }
            mqtt_ep::packet::Packet::V5_0Auth(_auth) => {
                // AUTH packet handling (placeholder for future implementation)
                trace!("Received AUTH from client {client_id}");
            }
            mqtt_ep::packet::Packet::V3_1_1Disconnect(_)
            | mqtt_ep::packet::Packet::V5_0Disconnect(_) => {
                return Err(anyhow::anyhow!("Client disconnected"));
            }
            _ => {
                trace!("Unhandled packet type from client {client_id}");
            }
        }
        Ok(())
    }

    /// Handle SUBSCRIBE in endpoint task (unified for both v3.1.1 and v5.0)
    async fn handle_subscribe(
        packet_id: u16,
        entries: &[mqtt_ep::packet::SubEntry],
        props: Vec<mqtt_ep::packet::Property>,
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        endpoint_ref: &EndpointRef,
    ) -> anyhow::Result<()> {
        // Extract SubscriptionIdentifier from properties
        let sub_id = props.iter().find_map(|prop| match prop {
            mqtt_ep::packet::Property::SubscriptionIdentifier(_) => prop.as_u32(),
            _ => None,
        });

        let mut topic_filters = Vec::new();

        for entry in entries {
            let topic_filter = entry.topic_filter().to_string();
            let qos = entry.sub_opts().qos();
            topic_filters.push((topic_filter.clone(), qos));
            trace!("SUBSCRIBE: endpoint wants to subscribe to '{topic_filter}' with QoS {qos:?}");
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
        Self::send_suback(endpoint, packet_id, return_codes, props).await?;

        trace!("✅ SUBSCRIBE processing completed successfully");
        Ok(())
    }

    /// Handle UNSUBSCRIBE in endpoint task with proper ordering
    async fn handle_unsubscribe_in_endpoint(
        unsubscribe: &mqtt_ep::packet::v3_1_1::Unsubscribe,
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        endpoint_ref: &EndpointRef,
    ) -> anyhow::Result<()> {
        let packet_id = unsubscribe.packet_id();
        let mut topics = Vec::new();

        for topic_filter in unsubscribe.entries() {
            topics.push(topic_filter.as_str().to_string());
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

        let _return_codes = response_rx.await?;

        // Send UNSUBACK directly via endpoint (v3.1.1 doesn't have return codes)
        let unsuback_packet = mqtt_ep::packet::v3_1_1::Unsuback::builder()
            .packet_id(packet_id)
            .build()
            .unwrap();

        endpoint.send(unsuback_packet).await?;
        trace!("✅ UNSUBSCRIBE processing completed successfully");
        Ok(())
    }

    /// Handle UNSUBSCRIBE v5.0 in endpoint task
    async fn handle_unsubscribe_in_endpoint_v5(
        unsubscribe: &mqtt_ep::packet::v5_0::Unsubscribe,
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        endpoint_ref: &EndpointRef,
    ) -> anyhow::Result<()> {
        let packet_id = unsubscribe.packet_id();
        let mut topics = Vec::new();

        for topic_filter in unsubscribe.entries() {
            topics.push(topic_filter.as_str().to_string());
        }

        let (response_tx, response_rx) = oneshot::channel();
        subscription_tx
            .send(SubscriptionMessage::Unsubscribe {
                endpoint: endpoint_ref.clone(),
                topics,
                response_tx,
            })
            .await?;

        let return_codes = response_rx.await?;

        // Convert UnsubAckReturnCode to UnsubackReasonCode for v5.0
        let reason_codes: Vec<mqtt_ep::result_code::UnsubackReasonCode> = return_codes
            .into_iter()
            .map(|_| mqtt_ep::result_code::UnsubackReasonCode::Success)
            .collect();

        let unsuback_packet = mqtt_ep::packet::v5_0::Unsuback::builder()
            .packet_id(packet_id)
            .reason_codes(reason_codes)
            .build()
            .unwrap();

        endpoint.send(unsuback_packet).await?;
        trace!("✅ UNSUBSCRIBE v5.0 processing completed successfully");
        Ok(())
    }

    /// Send PUBLISH packet to endpoint (supports both v3.1.1 and v5.0)
    async fn send_publish(
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
    async fn send_puback(
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
    async fn send_pubrec(
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
    async fn send_pubrel(
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
    async fn send_pubcomp(
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
    async fn send_suback(
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

    /// Send PINGRESP packet to endpoint (supports both v3.1.1 and v5.0)
    async fn send_pingresp(
        client_id: &str,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
    ) -> anyhow::Result<()> {
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                let pingresp = mqtt_ep::packet::v3_1_1::Pingresp::builder()
                    .build()
                    .unwrap();
                endpoint.send(pingresp).await?;
            }
            mqtt_ep::Version::V5_0 => {
                let pingresp = mqtt_ep::packet::v5_0::Pingresp::builder().build().unwrap();
                endpoint.send(pingresp).await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported MQTT version"));
            }
        }

        trace!("PINGRESP sent to client {client_id}");
        Ok(())
    }

    /// Handle PUBLISH packet (unified for both v3.1.1 and v5.0)
    #[allow(clippy::too_many_arguments)]
    async fn handle_publish(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        packet_id: u16,
        topic: &str,
        qos: mqtt_ep::packet::Qos,
        retain: bool,
        _dup: bool,
        payload: impl IntoPayload,
        publisher_props: Vec<mqtt_ep::packet::Property>,
        subscription_store: &Arc<SubscriptionStore>,
    ) -> anyhow::Result<()> {
        let subscriptions = subscription_store.find_subscribers(topic).await;
        let has_subscribers = !subscriptions.is_empty();

        if !has_subscribers {
            trace!("No subscribers found for topic '{topic}'");
        }

        // Send QoS response based on QoS level
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

        if !has_subscribers {
            return Ok(());
        }

        // Convert to ArcPayload for efficient cloning (reference counting)
        let arc_payload = payload.into_payload();

        // Send to subscribers sequentially (each endpoint.send() queues via mpsc)
        for subscription in subscriptions {
            // QoS arbitration: use the lower of publish QoS and subscription QoS
            let effective_qos = qos.min(subscription.qos);

            // Prepare properties for v5.0
            let mut props = publisher_props.clone();
            if let Some(sub_id) = subscription.sub_id {
                props.push(mqtt_ep::packet::Property::SubscriptionIdentifier(
                    mqtt_ep::packet::SubscriptionIdentifier::new(sub_id).unwrap(),
                ));
            }

            // Send PUBLISH packet using the new function
            // Clone is just Arc reference counting, very cheap
            if let Err(e) = Self::send_publish(
                subscription.endpoint.endpoint(),
                topic,
                effective_qos,
                retain,
                false, // dup is false for new messages
                arc_payload.clone(),
                props,
            )
            .await
            {
                error!("Failed to send PUBLISH message to endpoint: {e}");
            }
        }

        Ok(())
    }
}
