/**
 * MIT License
 *
 * Copyright (c) 2025 Takatoshi Kondo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
            .auto_pub_response(true)
            .auto_ping_response(true)
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
        let (client_id, mqtt_version) = match endpoint.recv().await {
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
                        (extracted_id, "v3.1.1".to_string())
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
                        (extracted_id, "v5.0".to_string())
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
        trace!("Registered client {client_id} with MQTT version {mqtt_version}");

        trace!("Starting main endpoint loop for client {client_id} ({mqtt_version})");

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
                        &mqtt_version,
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
        mqtt_version: &str,
        subscription_store: &Arc<SubscriptionStore>,
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        endpoint_ref: &EndpointRef,
    ) -> anyhow::Result<()> {
        match packet {
            mqtt_ep::packet::Packet::V3_1_1Subscribe(sub) => {
                Self::handle_subscribe_in_endpoint(sub, subscription_tx, endpoint, endpoint_ref)
                    .await?;
            }
            mqtt_ep::packet::Packet::V5_0Subscribe(sub) => {
                Self::handle_subscribe_in_endpoint_v5(sub, subscription_tx, endpoint, endpoint_ref)
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
                Self::handle_publish_in_endpoint_v311(pub_packet, subscription_store).await?;
            }
            mqtt_ep::packet::Packet::V5_0Publish(pub_packet) => {
                Self::handle_publish_in_endpoint_v5(pub_packet, subscription_store).await?;
            }
            mqtt_ep::packet::Packet::V3_1_1Pingreq(_) | mqtt_ep::packet::Packet::V5_0Pingreq(_) => {
                Self::send_pingresp_to_client(client_id, mqtt_version, endpoint).await?;
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

    /// Handle SUBSCRIBE in endpoint task with proper ordering
    async fn handle_subscribe_in_endpoint(
        subscribe: &mqtt_ep::packet::v3_1_1::Subscribe,
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        endpoint_ref: &EndpointRef,
    ) -> anyhow::Result<()> {
        let packet_id = subscribe.packet_id();
        let mut topic_filters = Vec::new();

        for entry in subscribe.entries() {
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
                sub_id: None,
                response_tx,
            })
            .await?;

        let return_codes = response_rx.await?;

        // Send SUBACK directly via endpoint
        let suback_packet = mqtt_ep::packet::v3_1_1::Suback::builder()
            .packet_id(packet_id)
            .return_codes(return_codes)
            .build()
            .unwrap();

        endpoint.send(suback_packet).await?;
        trace!("✅ SUBSCRIBE processing completed successfully");
        Ok(())
    }

    /// Handle SUBSCRIBE v5.0 in endpoint task
    async fn handle_subscribe_in_endpoint_v5(
        subscribe: &mqtt_ep::packet::v5_0::Subscribe,
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        endpoint_ref: &EndpointRef,
    ) -> anyhow::Result<()> {
        let packet_id = subscribe.packet_id();

        // Extract SubscriptionIdentifier from properties
        let sub_id = subscribe.props().iter().find_map(|prop| match prop {
            mqtt_ep::packet::Property::SubscriptionIdentifier(_) => prop.as_u32(),
            _ => None,
        });

        let mut topic_filters = Vec::new();

        for entry in subscribe.entries() {
            let topic_filter = entry.topic_filter().to_string();
            let qos = entry.sub_opts().qos();
            topic_filters.push((topic_filter, qos));
        }

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

        let suback_packet = mqtt_ep::packet::v5_0::Suback::builder()
            .packet_id(packet_id)
            .reason_codes(reason_codes)
            .build()
            .unwrap();

        endpoint.send(suback_packet).await?;
        trace!("✅ SUBSCRIBE v5.0 processing completed successfully");
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

    /// Handle PUBLISH packet for MQTT v3.1.1 in endpoint task
    async fn handle_publish_in_endpoint_v311(
        publish: &mqtt_ep::packet::v3_1_1::Publish,
        subscription_store: &Arc<SubscriptionStore>,
    ) -> anyhow::Result<()> {
        let topic = publish.topic_name();
        let payload = publish.payload();
        let qos = publish.qos();
        let retain = publish.retain();

        let subscriptions = subscription_store.find_subscribers(&topic).await;

        if subscriptions.is_empty() {
            trace!("No subscribers found for topic '{topic}'");
            return Ok(());
        }

        // trace!("Found {} subscriptions for topic '{}'", subscriptions.len(), topic);

        // Send to subscribers sequentially (each endpoint.send() queues via mpsc)
        for subscription in subscriptions {
            // QoS arbitration: use the lower of publish QoS and subscription QoS
            let effective_qos = qos.min(subscription.qos);

            // Create PUBLISH packet for this subscriber
            let mut builder = mqtt_ep::packet::v3_1_1::Publish::builder()
                .topic_name(topic)
                .unwrap()
                .qos(effective_qos)
                .retain(retain)
                .payload(payload.clone());

            let publish_packet = if effective_qos != mqtt_ep::packet::Qos::AtMostOnce {
                // Acquire proper packet ID for QoS > 0
                let packet_id = subscription
                    .endpoint
                    .endpoint()
                    .acquire_packet_id()
                    .await
                    .unwrap();
                builder = builder.packet_id(packet_id);
                builder.build().unwrap()
            } else {
                builder.build().unwrap()
            };

            // Send immediately (queues in mpsc channel, maintains order)
            if let Err(e) = subscription.endpoint.endpoint().send(publish_packet).await {
                error!("Failed to send PUBLISH message to endpoint: {e}");
            }
        }

        Ok(())
    }

    /// Handle PUBLISH packet for MQTT v5.0 in endpoint task
    async fn handle_publish_in_endpoint_v5(
        publish: &mqtt_ep::packet::v5_0::Publish,
        subscription_store: &Arc<SubscriptionStore>,
    ) -> anyhow::Result<()> {
        let topic = publish.topic_name();
        let payload = publish.payload();
        let qos = publish.qos();
        let retain = publish.retain();

        let subscriptions = subscription_store.find_subscribers(&topic).await;

        if subscriptions.is_empty() {
            trace!("No subscribers found for topic '{topic}'");
            return Ok(());
        }

        // trace!("Found {} subscriptions for topic '{}'", subscriptions.len(), topic);

        // Send to subscribers sequentially (each endpoint.send() queues via mpsc)
        for subscription in subscriptions {
            // QoS arbitration: use the lower of publish QoS and subscription QoS
            let effective_qos = qos.min(subscription.qos);

            // Create PUBLISH packet for this subscriber (v5.0 format with SubscriptionIdentifier)
            let mut builder = mqtt_ep::packet::v5_0::Publish::builder()
                .topic_name(topic)
                .unwrap()
                .qos(effective_qos)
                .retain(retain)
                .payload(payload.clone());

            // Add SubscriptionIdentifier property if present
            if let Some(sub_id) = subscription.sub_id {
                builder = builder.props(vec![mqtt_ep::packet::Property::SubscriptionIdentifier(
                    mqtt_ep::packet::SubscriptionIdentifier::new(sub_id).unwrap(),
                )]);
            }

            let publish_packet = if effective_qos != mqtt_ep::packet::Qos::AtMostOnce {
                // Acquire proper packet ID for QoS > 0
                let packet_id = subscription
                    .endpoint
                    .endpoint()
                    .acquire_packet_id()
                    .await
                    .unwrap_or(1);
                builder = builder.packet_id(packet_id);
                builder.build().unwrap()
            } else {
                builder.build().unwrap()
            };

            if let Err(e) = subscription.endpoint.endpoint().send(publish_packet).await {
                error!("Failed to send PUBLISH message to endpoint: {e}");
            }
        }

        Ok(())
    }

    /// Send PINGRESP to client directly via endpoint
    async fn send_pingresp_to_client(
        client_id: &str,
        mqtt_version: &str,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
    ) -> anyhow::Result<()> {
        if mqtt_version == "v5.0" {
            let pingresp = mqtt_ep::packet::v5_0::Pingresp::builder().build().unwrap();
            endpoint.send(pingresp).await?;
        } else {
            let pingresp = mqtt_ep::packet::v3_1_1::Pingresp::builder()
                .build()
                .unwrap();
            endpoint.send(pingresp).await?;
        }
        trace!("PINGRESP sent to client {client_id}");
        Ok(())
    }
}
