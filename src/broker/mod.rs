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

use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, trace};
use uuid::Uuid;

use crate::retained_store::RetainedStore;
use crate::session_store::{SessionId, SessionStore};
use crate::subscription_store::{EndpointRef, SubscriptionStore};

use mqtt_ep::prelude::*;

mod sub_impl;
mod pub_impl;

/// Messages sent to BrokerManager for subscription management
#[derive(Debug)]
pub enum SubscriptionMessage {
    Subscribe {
        endpoint: EndpointRef,
        topics: Vec<(String, mqtt_ep::packet::Qos, bool)>, // (topic_filter, qos, rap)
        sub_id: Option<u32>,
        response_tx: oneshot::Sender<Vec<(mqtt_ep::result_code::SubackReturnCode, bool)>>, // (return_code, is_new)
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

    /// Global retained message store
    retained_store: Arc<RetainedStore>,

    /// Global session store
    session_store: Arc<SessionStore>,

    /// Channel to send subscription management messages
    subscription_tx: mpsc::Sender<SubscriptionMessage>,

    /// Endpoint receive buffer size
    ep_recv_buf_size: Option<usize>,
}

impl BrokerManager {
    /// Create a new broker manager
    pub async fn new(ep_recv_buf_size: Option<usize>) -> anyhow::Result<Self> {
        let subscription_store = Arc::new(SubscriptionStore::new());
        let retained_store = Arc::new(RetainedStore::new());
        let session_store = Arc::new(SessionStore::new());
        let (subscription_tx, subscription_rx) = mpsc::channel(1000);

        // Spawn subscription management task
        let store_for_task = subscription_store.clone();
        tokio::spawn(async move {
            Self::subscription_manager_task(store_for_task, subscription_rx).await;
        });

        Ok(Self {
            subscription_store,
            retained_store,
            session_store,
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
        let retained_store_for_endpoint = self.retained_store.clone();
        let session_store_for_endpoint = self.session_store.clone();
        let subscription_tx_for_endpoint = self.subscription_tx.clone();
        let broker_manager_for_cleanup = self.clone();

        tokio::spawn(async move {
            // Handle client endpoint
            let endpoint_ref = Self::handle_client_endpoint(
                endpoint,
                subscription_store_for_endpoint,
                retained_store_for_endpoint,
                session_store_for_endpoint,
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

                    for (topic_filter, qos, rap) in topics {
                        match subscription_store
                            .subscribe(endpoint.clone(), &topic_filter, qos, sub_id, rap)
                            .await
                        {
                            Ok(is_new) => {
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
                                return_codes.push((return_code, is_new));
                                trace!("Registered subscription: endpoint topic='{topic_filter}', is_new={is_new}");
                            }
                            Err(_) => {
                                return_codes.push((mqtt_ep::result_code::SubackReturnCode::Failure, false));
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
        retained_store: Arc<RetainedStore>,
        session_store: Arc<SessionStore>,
        subscription_tx: mpsc::Sender<SubscriptionMessage>,
    ) -> Option<EndpointRef> {
        trace!("Starting endpoint task (waiting for CONNECT)");

        // First, wait for CONNECT packet
        let (client_id, user_name, session_expiry_interval) = match endpoint.recv().await {
            Ok(packet) => {
                trace!("🔍 Received first packet: {:?}", packet.packet_type());
                match &packet {
                    mqtt_ep::packet::Packet::V3_1_1Connect(connect) => {
                        let extracted_id = if connect.client_id().is_empty() {
                            format!("auto-{}", Uuid::new_v4().simple())
                        } else {
                            connect.client_id().to_string()
                        };
                        let user_name = connect.user_name().map(|s| s.to_string());
                        let clean_session = connect.clean_session();
                        trace!("MQTT v3.1.1 client {extracted_id} connected, clean_session={clean_session}");

                        // For v3.1.1: clean_session=false means session persists indefinitely
                        let session_expiry_interval = if clean_session { 0 } else { u32::MAX };

                        // Create session ID
                        let session_id = SessionId::new(user_name.clone(), extracted_id.clone());

                        // Determine session_present
                        let session_present = if clean_session {
                            // Clean session: remove existing session if any
                            session_store.remove_session(&session_id).await;
                            false
                        } else {
                            // Check if session exists
                            session_store.get_session(&session_id).await.is_some()
                        };

                        // Send CONNACK for v3.1.1
                        let connack = mqtt_ep::packet::v3_1_1::Connack::builder()
                            .session_present(session_present)
                            .return_code(mqtt_ep::result_code::ConnectReturnCode::Accepted)
                            .build()
                            .unwrap();

                        trace!("Sending CONNACK to client {extracted_id}, session_present={session_present}...");
                        if let Err(e) = endpoint.send(connack).await {
                            error!("Failed to send CONNACK: {e}");
                            return None;
                        }

                        trace!("CONNACK successfully sent to client {extracted_id}");
                        (extracted_id, user_name, session_expiry_interval)
                    }
                    mqtt_ep::packet::Packet::V5_0Connect(connect) => {
                        let extracted_id = if connect.client_id().is_empty() {
                            format!("auto-{}", Uuid::new_v4().simple())
                        } else {
                            connect.client_id().to_string()
                        };
                        let user_name = connect.user_name().map(|s| s.to_string());
                        let clean_start = connect.clean_start();

                        // Extract SessionExpiryInterval from properties (default: 0)
                        let session_expiry_interval = connect
                            .props()
                            .iter()
                            .find_map(|prop| {
                                if let mqtt_ep::packet::Property::SessionExpiryInterval(_) = prop {
                                    prop.as_u32()
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0);

                        trace!("MQTT v5.0 client {extracted_id} connected, clean_start={clean_start}, session_expiry_interval={session_expiry_interval}");

                        // Create session ID
                        let session_id = SessionId::new(user_name.clone(), extracted_id.clone());

                        // Determine session_present
                        let session_present = if clean_start {
                            // Clean start: remove existing session if any
                            session_store.remove_session(&session_id).await;
                            false
                        } else {
                            // Check if session exists
                            session_store.get_session(&session_id).await.is_some()
                        };

                        // Send CONNACK for v5.0
                        let connack = mqtt_ep::packet::v5_0::Connack::builder()
                            .session_present(session_present)
                            .reason_code(mqtt_ep::result_code::ConnectReasonCode::Success)
                            .build()
                            .unwrap();

                        trace!("Sending CONNACK to client {extracted_id}, session_present={session_present}...");
                        if let Err(e) = endpoint.send(connack).await {
                            error!("Failed to send CONNACK: {e}");
                            return None;
                        }

                        trace!("CONNACK successfully sent to client {extracted_id}");
                        (extracted_id, user_name, session_expiry_interval)
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

        // Create/update session
        let session_id = SessionId::new(user_name, client_id.clone());
        let (session, _is_new) = session_store
            .get_or_create_session(session_id.clone(), endpoint_arc.clone(), session_expiry_interval)
            .await;

        trace!("Registered client {client_id} with session_expiry_interval={session_expiry_interval}");

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
                        &retained_store,
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

        // Handle session cleanup on disconnect
        let session_guard = session.read().await;
        let session_expiry_interval = session_guard.session_expiry_interval();
        drop(session_guard);

        if session_expiry_interval == 0 {
            // Session expiry interval is 0: delete session immediately
            trace!("Removing session for client {client_id} (session_expiry_interval=0)");
            session_store.remove_session(&session_id).await;
        } else {
            // Session should persist: mark endpoint as offline
            trace!("Preserving session for client {client_id} (session_expiry_interval={session_expiry_interval})");
            let mut session_guard = session.write().await;
            session_guard.clear_endpoint();
        }

        Some(endpoint_ref)
    }

    /// Handle received packet in endpoint task (direct processing)
    async fn handle_received_packet_in_endpoint(
        client_id: &str,
        packet: &mqtt_ep::packet::Packet,
        subscription_store: &Arc<SubscriptionStore>,
        retained_store: &Arc<RetainedStore>,
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
                    subscription_store,
                    retained_store,
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
                    subscription_store,
                    retained_store,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V3_1_1Unsubscribe(unsub) => {
                Self::handle_unsubscribe(
                    unsub.packet_id(),
                    unsub.entries(),
                    subscription_tx,
                    endpoint,
                    endpoint_ref,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V5_0Unsubscribe(unsub) => {
                Self::handle_unsubscribe(
                    unsub.packet_id(),
                    unsub.entries(),
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
                    retained_store,
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
                    retained_store,
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
}
