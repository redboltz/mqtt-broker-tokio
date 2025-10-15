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

use crate::auth_impl::Security;
use crate::retained_store::RetainedStore;
use crate::session_store::{SessionId, SessionRef, SessionStore};
use crate::subscription_store::{EndpointRef, SubscriptionStore};

use mqtt_ep::prelude::*;

mod pub_impl;
mod sub_impl;

/// Messages sent to BrokerManager for subscription management
#[derive(Debug)]
pub enum SubscriptionMessage {
    Subscribe {
        session_ref: SessionRef,
        topics: Vec<(String, mqtt_ep::packet::Qos, bool, bool)>, // (topic_filter, qos, rap, nl)
        sub_id: Option<u32>,
        response_tx: oneshot::Sender<Vec<(mqtt_ep::result_code::SubackReturnCode, bool)>>, // (return_code, is_new)
    },
    Unsubscribe {
        session_ref: SessionRef,
        topics: Vec<String>,
        response_tx: oneshot::Sender<Vec<mqtt_ep::result_code::UnsubackReasonCode>>,
    },
    ClientDisconnected {
        session_ref: SessionRef,
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

    /// Authentication and authorization manager
    security: Option<Arc<Security>>,

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
            security: None,
            subscription_tx,
            ep_recv_buf_size,
        })
    }

    /// Create a new broker manager with security
    pub async fn new_with_security(
        ep_recv_buf_size: Option<usize>,
        security: Security,
    ) -> anyhow::Result<Self> {
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
            security: Some(Arc::new(security)),
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
        let security_for_endpoint = self.security.clone();
        let subscription_tx_for_endpoint = self.subscription_tx.clone();
        let broker_manager_for_cleanup = self.clone();

        tokio::spawn(async move {
            // Handle client endpoint
            let result = Self::handle_client_endpoint(
                endpoint,
                subscription_store_for_endpoint,
                retained_store_for_endpoint,
                session_store_for_endpoint,
                security_for_endpoint,
                subscription_tx_for_endpoint,
            )
            .await;

            // Clean up when endpoint task finishes
            if let Some((session_ref, need_keep)) = result {
                // Only delete subscriptions if session should not be kept
                if !need_keep {
                    if let Err(e) = broker_manager_for_cleanup
                        .handle_client_disconnect(&session_ref)
                        .await
                    {
                        error!("Error during client disconnect cleanup: {e}");
                    }
                }
            }
        });

        trace!("✅ Connection accepted, endpoint task starting");
        Ok(())
    }

    /// Handle client disconnection cleanup
    async fn handle_client_disconnect(&self, session_ref: &SessionRef) -> anyhow::Result<()> {
        trace!("Starting disconnect cleanup for session");

        // Remove from subscription store via message
        let _ = self
            .subscription_tx
            .send(SubscriptionMessage::ClientDisconnected {
                session_ref: session_ref.clone(),
            })
            .await;

        trace!("Session disconnected and cleaned up");
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
                    session_ref,
                    topics,
                    sub_id,
                    response_tx,
                } => {
                    let mut return_codes = Vec::new();

                    for (topic_filter, qos, rap, nl) in topics {
                        match subscription_store
                            .subscribe(session_ref.clone(), &topic_filter, qos, sub_id, rap, nl)
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
                                trace!(
                                    "Registered subscription: session topic='{topic_filter}', is_new={is_new}"
                                );
                            }
                            Err(_) => {
                                return_codes
                                    .push((mqtt_ep::result_code::SubackReturnCode::Failure, false));
                            }
                        }
                    }

                    let _ = response_tx.send(return_codes);
                }
                SubscriptionMessage::Unsubscribe {
                    session_ref,
                    topics,
                    response_tx,
                } => {
                    let mut return_codes = Vec::new();

                    for topic_filter in topics {
                        let _ = subscription_store
                            .unsubscribe(&session_ref, &topic_filter)
                            .await;
                        return_codes.push(mqtt_ep::result_code::UnsubackReasonCode::Success);
                        trace!("Removed subscription: session topic='{topic_filter}'");
                    }

                    let _ = response_tx.send(return_codes);
                }
                SubscriptionMessage::ClientDisconnected { session_ref } => {
                    subscription_store.unsubscribe_all(&session_ref).await;
                    trace!("Removed all subscriptions for disconnected session");
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
        security: Option<Arc<Security>>,
        subscription_tx: mpsc::Sender<SubscriptionMessage>,
    ) -> Option<(SessionRef, bool)> {
        trace!("Starting endpoint task (waiting for CONNECT)");

        // First, wait for CONNECT packet
        let (
            client_id,
            user_name,
            password,
            session_expiry_interval,
            need_keep,
            session_present,
            clean_start_or_session,
            assigned_client_id,
        ) = match endpoint.recv().await {
            Ok(packet) => {
                trace!("🔍 Received first packet: {:?}", packet.packet_type());
                match &packet {
                    mqtt_ep::packet::Packet::V3_1_1Connect(connect) => {
                        let clean_session = connect.clean_session();
                        let client_id_empty = connect.client_id().is_empty();

                        // v3.1.1: If clean_session=false and ClientId is empty, reject with IdentifierRejected
                        if !clean_session && client_id_empty {
                            error!(
                                "MQTT v3.1.1 client with empty ClientId and clean_session=false rejected"
                            );
                            let connack = mqtt_ep::packet::v3_1_1::Connack::builder()
                                .session_present(false)
                                .return_code(
                                    mqtt_ep::result_code::ConnectReturnCode::IdentifierRejected,
                                )
                                .build()
                                .unwrap();
                            let _ = endpoint.send(connack).await;
                            return None;
                        }

                        let extracted_id = if client_id_empty {
                            format!("auto-{}", Uuid::new_v4().simple())
                        } else {
                            connect.client_id().to_string()
                        };
                        let user_name = connect.user_name().map(|s| s.to_string());
                        let password = connect.password().and_then(|p| String::from_utf8(p.to_vec()).ok());
                        trace!(
                            "MQTT v3.1.1 client {extracted_id} connected, clean_session={clean_session}"
                        );

                        // For v3.1.1: clean_session=false means session persists indefinitely
                        let session_expiry_interval = if clean_session { 0 } else { u32::MAX };
                        let need_keep = !clean_session;

                        // Create session ID
                        let session_id = SessionId::new(user_name.clone(), extracted_id.clone());

                        // Step 1: Disconnect existing online session if any (session takeover)
                        session_store.disconnect_existing_session(&session_id).await;

                        // Step 2: Check if session exists after disconnect
                        let existing_session = session_store.get_session(&session_id).await;

                        let session_present = if clean_session {
                            // Clean session: remove existing session if any
                            if existing_session.is_some() {
                                session_store.remove_session(&session_id).await;
                            }
                            false
                        } else {
                            // Check if session exists
                            existing_session.is_some()
                        };

                        trace!(
                            "Session takeover complete for {extracted_id}, session_present={session_present}"
                        );

                        // v3.1.1 doesn't use Assigned Client Identifier property
                        (
                            extracted_id,
                            user_name,
                            password,
                            session_expiry_interval,
                            need_keep,
                            session_present,
                            clean_session,
                            None,
                        )
                    }
                    mqtt_ep::packet::Packet::V5_0Connect(connect) => {
                        let client_id_empty = connect.client_id().is_empty();
                        let extracted_id = if client_id_empty {
                            format!("auto-{}", Uuid::new_v4().simple())
                        } else {
                            connect.client_id().to_string()
                        };
                        // v5.0: If ClientId was empty, need to return Assigned Client Identifier
                        let assigned_client_id = if client_id_empty {
                            Some(extracted_id.clone())
                        } else {
                            None
                        };
                        let user_name = connect.user_name().map(|s| s.to_string());
                        let password = connect.password().and_then(|p| String::from_utf8(p.to_vec()).ok());
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

                        let need_keep = session_expiry_interval > 0;

                        trace!(
                            "MQTT v5.0 client {extracted_id} connected, clean_start={clean_start}, session_expiry_interval={session_expiry_interval}"
                        );

                        // Create session ID
                        let session_id = SessionId::new(user_name.clone(), extracted_id.clone());

                        // Step 1: Disconnect existing online session if any (session takeover)
                        session_store.disconnect_existing_session(&session_id).await;

                        // Step 2: Check if session exists after disconnect
                        let existing_session = session_store.get_session(&session_id).await;

                        let session_present = if clean_start {
                            // Clean start: remove existing session if any
                            if existing_session.is_some() {
                                session_store.remove_session(&session_id).await;
                            }
                            false
                        } else {
                            // Check if session exists
                            existing_session.is_some()
                        };

                        trace!(
                            "Session takeover complete for {extracted_id}, session_present={session_present}"
                        );

                        (
                            extracted_id,
                            user_name,
                            password,
                            session_expiry_interval,
                            need_keep,
                            session_present,
                            clean_start,
                            assigned_client_id,
                        )
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
        let _endpoint_ref = EndpointRef::new(endpoint_arc.clone());

        // Authenticate user if security is configured
        let authenticated_username = if let Some(ref sec) = security {
            match (user_name.as_ref(), password.as_ref()) {
                (Some(username), Some(pwd)) => {
                    // Authenticate with username and password
                    if let Some(auth_user) = sec.login(username, pwd) {
                        trace!("User {username} authenticated successfully");
                        Some(auth_user)
                    } else {
                        error!("Authentication failed for user {username}");
                        // Send NotAuthorized CONNACK
                        Self::send_connack_not_authorized(&endpoint_arc, &client_id).await;
                        return None;
                    }
                }
                (Some(username), None) => {
                    // Username provided but no password - try client certificate
                    if sec.login_cert(username) {
                        trace!("User {username} authenticated with client certificate");
                        Some(username.clone())
                    } else {
                        error!("Client certificate authentication failed for user {username}");
                        // Send NotAuthorized CONNACK
                        Self::send_connack_not_authorized(&endpoint_arc, &client_id).await;
                        return None;
                    }
                }
                (None, None) => {
                    // No username/password - try anonymous
                    if let Some(anon_user) = sec.login_anonymous() {
                        trace!("Anonymous user {anon_user} authenticated");
                        Some(anon_user.to_string())
                    } else {
                        error!("Anonymous authentication not configured");
                        // Send NotAuthorized CONNACK
                        Self::send_connack_not_authorized(&endpoint_arc, &client_id).await;
                        return None;
                    }
                }
                (None, Some(_)) => {
                    // Password without username - not allowed
                    error!("Password provided without username");
                    // Send NotAuthorized CONNACK
                    Self::send_connack_not_authorized(&endpoint_arc, &client_id).await;
                    return None;
                }
            }
        } else {
            // No security configured - allow all connections
            user_name.clone()
        };

        // Create session ID with authenticated username
        let session_id = SessionId::new(authenticated_username, client_id.clone());

        // Handle session creation or restoration
        let session = if session_present && !clean_start_or_session {
            // Session restoration
            trace!("Restoring session for client {client_id}");

            // Restore session state (stored packets, QoS2 PIDs, offline messages)
            let offline_messages = match session_store
                .restore_session_state(
                    &session_id,
                    endpoint_arc.clone(),
                    session_expiry_interval,
                    need_keep,
                )
                .await
            {
                Ok(msgs) => msgs,
                Err(e) => {
                    error!("Failed to restore session state for {client_id}: {e}");
                    return None;
                }
            };

            // Send CONNACK with session_present=true
            if let Err(e) =
                Self::send_connack_for_restore(&endpoint_arc, &client_id, session_expiry_interval)
                    .await
            {
                error!("Failed to send CONNACK for {client_id}: {e}");
                return None;
            }

            // Send offline messages
            Self::send_offline_messages(&endpoint_arc, &client_id, offline_messages).await;

            // Get session
            match session_store.get_session(&session_id).await {
                Some(s) => s,
                None => {
                    error!("Session disappeared after restoration for {client_id}");
                    return None;
                }
            }
        } else {
            // New session or clean start
            trace!("Creating new session for client {client_id}");

            // Send CONNACK with session_present=false
            if let Err(e) = Self::send_connack_for_new(
                &endpoint_arc,
                &client_id,
                session_expiry_interval,
                assigned_client_id.as_deref(),
            )
            .await
            {
                error!("Failed to send CONNACK for {client_id}: {e}");
                return None;
            }

            // Create new session
            let (session, _is_new) = session_store
                .get_or_create_session(
                    session_id.clone(),
                    endpoint_arc.clone(),
                    session_expiry_interval,
                    need_keep,
                )
                .await;
            session
        };

        // Create session reference
        let session_ref = SessionRef::new(session_id.clone());

        trace!(
            "Registered client {client_id} with session_expiry_interval={session_expiry_interval}"
        );

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
                        &session_store,
                        &security,
                        &subscription_tx,
                        &endpoint_arc,
                        &session_ref,
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
        let need_keep = session_guard.need_keep();
        drop(session_guard);

        if need_keep {
            // Session should persist: mark as offline
            trace!("Preserving session for client {client_id} (need_keep=true)");
            let mut session_guard = session.write().await;
            session_guard.set_offline();
            // Don't delete subscriptions - they should persist with the session
        } else {
            // Delete session immediately
            trace!("Removing session for client {client_id} (need_keep=false)");
            session_store.remove_session(&session_id).await;
            // Subscriptions will be deleted via ClientDisconnected message
        }

        Some((session_ref, need_keep))
    }

    /// Handle received packet in endpoint task (direct processing)
    #[allow(clippy::too_many_arguments)]
    async fn handle_received_packet_in_endpoint(
        client_id: &str,
        packet: &mqtt_ep::packet::Packet,
        subscription_store: &Arc<SubscriptionStore>,
        retained_store: &Arc<RetainedStore>,
        session_store: &Arc<SessionStore>,
        security: &Option<Arc<Security>>,
        subscription_tx: &mpsc::Sender<SubscriptionMessage>,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        session_ref: &SessionRef,
    ) -> anyhow::Result<()> {
        match packet {
            mqtt_ep::packet::Packet::V3_1_1Subscribe(sub) => {
                Self::handle_subscribe(
                    sub.packet_id(),
                    sub.entries(),
                    Vec::new(),
                    security,
                    subscription_tx,
                    endpoint,
                    session_ref,
                    subscription_store,
                    retained_store,
                    session_store,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V5_0Subscribe(sub) => {
                Self::handle_subscribe(
                    sub.packet_id(),
                    sub.entries(),
                    sub.props().to_vec(),
                    security,
                    subscription_tx,
                    endpoint,
                    session_ref,
                    subscription_store,
                    retained_store,
                    session_store,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V3_1_1Unsubscribe(unsub) => {
                Self::handle_unsubscribe(
                    unsub.packet_id(),
                    unsub.entries(),
                    subscription_tx,
                    endpoint,
                    session_ref,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V5_0Unsubscribe(unsub) => {
                Self::handle_unsubscribe(
                    unsub.packet_id(),
                    unsub.entries(),
                    subscription_tx,
                    endpoint,
                    session_ref,
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
                    session_store,
                    session_ref,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V5_0Publish(pub_packet) => {
                // v5.0: Check for SubscriptionIdentifier in PUBLISH from client (Protocol Error)
                // MQTT spec: SubscriptionIdentifier is only sent from Server to Client, never from Client to Server
                let has_sub_id = pub_packet.props().iter().any(|prop| {
                    matches!(prop, mqtt_ep::packet::Property::SubscriptionIdentifier(_))
                });

                if has_sub_id {
                    error!(
                        "Protocol Error: Client {client_id} sent PUBLISH with SubscriptionIdentifier property"
                    );
                    // Send DISCONNECT with ProtocolError reason code
                    let disconnect = mqtt_ep::packet::v5_0::Disconnect::builder()
                        .reason_code(mqtt_ep::result_code::DisconnectReasonCode::ProtocolError)
                        .build()
                        .unwrap();
                    let _ = endpoint.send(disconnect).await;
                    return Err(anyhow::anyhow!(
                        "Protocol Error: PUBLISH contains SubscriptionIdentifier"
                    ));
                }

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
                    session_store,
                    session_ref,
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

    /// Send CONNACK for session restoration (session_present=true)
    async fn send_connack_for_restore(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        client_id: &str,
        _session_expiry_interval: u32,
    ) -> anyhow::Result<()> {
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                let connack = mqtt_ep::packet::v3_1_1::Connack::builder()
                    .session_present(true)
                    .return_code(mqtt_ep::result_code::ConnectReturnCode::Accepted)
                    .build()
                    .unwrap();
                trace!("Sending CONNACK (session_present=true) to client {client_id}");
                endpoint.send(connack).await?;
            }
            mqtt_ep::Version::V5_0 => {
                let connack = mqtt_ep::packet::v5_0::Connack::builder()
                    .session_present(true)
                    .reason_code(mqtt_ep::result_code::ConnectReasonCode::Success)
                    .build()
                    .unwrap();
                trace!("Sending CONNACK (session_present=true) to client {client_id}");
                endpoint.send(connack).await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported MQTT version"));
            }
        }

        trace!("CONNACK (session_present=true) sent to client {client_id}");
        Ok(())
    }

    /// Send CONNACK for new session (session_present=false)
    async fn send_connack_for_new(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        client_id: &str,
        _session_expiry_interval: u32,
        assigned_client_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                let connack = mqtt_ep::packet::v3_1_1::Connack::builder()
                    .session_present(false)
                    .return_code(mqtt_ep::result_code::ConnectReturnCode::Accepted)
                    .build()
                    .unwrap();
                trace!("Sending CONNACK (session_present=false) to client {client_id}");
                endpoint.send(connack).await?;
            }
            mqtt_ep::Version::V5_0 => {
                let mut builder = mqtt_ep::packet::v5_0::Connack::builder()
                    .session_present(false)
                    .reason_code(mqtt_ep::result_code::ConnectReasonCode::Success);

                // Add Assigned Client Identifier property if ClientId was auto-assigned
                if let Some(assigned_id) = assigned_client_id {
                    builder =
                        builder.props(vec![mqtt_ep::packet::Property::AssignedClientIdentifier(
                            mqtt_ep::packet::AssignedClientIdentifier::new(assigned_id)
                                .expect("Failed to create AssignedClientIdentifier"),
                        )]);
                    trace!(
                        "Sending CONNACK (session_present=false) with assigned ClientId '{assigned_id}' to client {client_id}"
                    );
                } else {
                    trace!("Sending CONNACK (session_present=false) to client {client_id}");
                }

                let connack = builder.build().unwrap();
                endpoint.send(connack).await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported MQTT version"));
            }
        }

        trace!("CONNACK (session_present=false) sent to client {client_id}");
        Ok(())
    }

    /// Send CONNACK with NotAuthorized reason code (authentication failed)
    async fn send_connack_not_authorized(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        client_id: &str,
    ) {
        let endpoint_version = endpoint
            .get_protocol_version()
            .await
            .unwrap_or(mqtt_ep::Version::V5_0);

        match endpoint_version {
            mqtt_ep::Version::V3_1_1 => {
                let connack = mqtt_ep::packet::v3_1_1::Connack::builder()
                    .session_present(false)
                    .return_code(mqtt_ep::result_code::ConnectReturnCode::NotAuthorized)
                    .build()
                    .unwrap();
                trace!("Sending CONNACK (NotAuthorized) to client {client_id}");
                let _ = endpoint.send(connack).await;
            }
            mqtt_ep::Version::V5_0 => {
                let connack = mqtt_ep::packet::v5_0::Connack::builder()
                    .session_present(false)
                    .reason_code(mqtt_ep::result_code::ConnectReasonCode::NotAuthorized)
                    .build()
                    .unwrap();
                trace!("Sending CONNACK (NotAuthorized) to client {client_id}");
                let _ = endpoint.send(connack).await;
            }
            _ => {
                error!("Unsupported MQTT version for NotAuthorized CONNACK");
            }
        }

        trace!("CONNACK (NotAuthorized) sent to client {client_id}");
    }

    /// Send offline messages to endpoint
    /// Offline messages are deleted from the list after successful send() since they become stored_packets
    async fn send_offline_messages(
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        client_id: &str,
        offline_messages: Vec<crate::session_store::OfflineMessage>,
    ) {
        trace!(
            "Sending {} offline messages to client {client_id}",
            offline_messages.len()
        );

        for msg in offline_messages {
            // Acquire packet ID for QoS > 0
            let packet_id = if msg.qos != mqtt_ep::packet::Qos::AtMostOnce {
                match endpoint.acquire_packet_id_when_available().await {
                    Ok(id) => id,
                    Err(e) => {
                        error!("Failed to acquire packet ID for offline message: {e}");
                        continue;
                    }
                }
            } else {
                0 // QoS 0 doesn't need packet ID
            };

            // Build and send PUBLISH packet
            let endpoint_version = endpoint
                .get_protocol_version()
                .await
                .unwrap_or(mqtt_ep::Version::V5_0);

            let result = match endpoint_version {
                mqtt_ep::Version::V3_1_1 => {
                    let mut builder = mqtt_ep::packet::v3_1_1::Publish::builder()
                        .topic_name(&msg.topic_name)
                        .expect("Failed to set topic_name")
                        .qos(msg.qos)
                        .retain(msg.retain)
                        .payload(msg.payload.clone());

                    if msg.qos != mqtt_ep::packet::Qos::AtMostOnce {
                        builder = builder.packet_id(packet_id);
                    }

                    let publish = builder.build().expect("Failed to build PUBLISH");
                    endpoint.send(publish).await
                }
                mqtt_ep::Version::V5_0 => {
                    let mut builder = mqtt_ep::packet::v5_0::Publish::builder()
                        .topic_name(&msg.topic_name)
                        .expect("Failed to set topic_name")
                        .qos(msg.qos)
                        .retain(msg.retain)
                        .payload(msg.payload.clone());

                    if !msg.props.is_empty() {
                        builder = builder.props(msg.props.clone());
                    }

                    if msg.qos != mqtt_ep::packet::Qos::AtMostOnce {
                        builder = builder.packet_id(packet_id);
                    }

                    let publish = builder.build().expect("Failed to build PUBLISH");
                    endpoint.send(publish).await
                }
                _ => {
                    error!("Unsupported MQTT version for offline message");
                    continue;
                }
            };

            if let Err(e) = result {
                error!("Failed to send offline message to {client_id}: {e}");
                // Failed to send: message is lost (endpoint error likely means connection is broken)
            }
            // If send() succeeds, the message is now managed by endpoint as stored_packet
            // So we implicitly delete it from offline_messages by not re-adding it
        }

        trace!("Finished sending offline messages to client {client_id}");
    }
}
