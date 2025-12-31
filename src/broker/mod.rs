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
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, trace};
use uuid::Uuid;

use crate::auth_impl::Security;
use crate::retained_store::RetainedStore;
use crate::session_store::{SessionId, SessionRef, SessionStore, WillMessage};
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

    /// Feature support flags
    retain_support: bool,
    shared_sub_support: bool,
    sub_id_support: bool,
    wc_support: bool,
    maximum_qos: mqtt_ep::packet::Qos,
    receive_maximum: Option<u16>,
    maximum_packet_size: Option<u32>,
    topic_alias_maximum: Option<u16>,
    auto_map_topic_alias: bool,
}

impl BrokerManager {
    /// Create a new broker manager
    pub async fn new(
        ep_recv_buf_size: Option<usize>,
        retain_support: bool,
        shared_sub_support: bool,
        sub_id_support: bool,
        wc_support: bool,
        maximum_qos: mqtt_ep::packet::Qos,
        receive_maximum: Option<u16>,
        maximum_packet_size: Option<u32>,
        topic_alias_maximum: Option<u16>,
        auto_map_topic_alias: bool,
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
            security: None,
            subscription_tx,
            ep_recv_buf_size,
            retain_support,
            shared_sub_support,
            sub_id_support,
            wc_support,
            maximum_qos,
            receive_maximum,
            maximum_packet_size,
            topic_alias_maximum,
            auto_map_topic_alias,
        })
    }

    /// Create a new broker manager with security
    pub async fn new_with_security(
        ep_recv_buf_size: Option<usize>,
        retain_support: bool,
        shared_sub_support: bool,
        sub_id_support: bool,
        wc_support: bool,
        maximum_qos: mqtt_ep::packet::Qos,
        receive_maximum: Option<u16>,
        maximum_packet_size: Option<u32>,
        topic_alias_maximum: Option<u16>,
        auto_map_topic_alias: bool,
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
            retain_support,
            shared_sub_support,
            sub_id_support,
            wc_support,
            maximum_qos,
            receive_maximum,
            maximum_packet_size,
            topic_alias_maximum,
            auto_map_topic_alias,
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
            .auto_map_topic_alias_send(self.auto_map_topic_alias)
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
            let result = broker_manager_for_cleanup
                .handle_client_endpoint(
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
        &self,
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
            clean_start_or_session,
            assigned_client_id,
            request_response_information,
            will_message,
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
                        let password = connect
                            .password()
                            .and_then(|p| String::from_utf8(p.to_vec()).ok());

                        // Extract Will message if present
                        let will_message = if connect.will_flag() {
                            let will_topic = connect
                                .will_topic()
                                .map(|s| s.to_string())
                                .unwrap_or_default();
                            let will_payload = connect.will_payload().unwrap_or_default();
                            let will_qos = connect.will_qos();
                            let will_retain = connect.will_retain();

                            // Check if Will QoS exceeds maximum_qos
                            if will_qos > self.maximum_qos {
                                error!(
                                    "MQTT v3.1.1 client {extracted_id} Will QoS {will_qos:?} exceeds maximum QoS {:?}",
                                    self.maximum_qos
                                );
                                let connack = mqtt_ep::packet::v3_1_1::Connack::builder()
                                    .session_present(false)
                                    .return_code(
                                        mqtt_ep::result_code::ConnectReturnCode::NotAuthorized,
                                    )
                                    .build()
                                    .unwrap();
                                let _ = endpoint.send(connack).await;
                                return None;
                            }

                            trace!(
                                "MQTT v3.1.1 client {extracted_id} has Will message: topic={will_topic}, qos={will_qos:?}, retain={will_retain}"
                            );

                            Some(WillMessage {
                                topic: will_topic,
                                payload: will_payload.into_payload(),
                                qos: will_qos,
                                retain: will_retain,
                                props: Vec::new(), // v3.1.1 doesn't support properties
                                will_delay_interval: 0, // v3.1.1 doesn't support Will Delay Interval
                                registered_at: std::time::Instant::now(),
                            })
                        } else {
                            None
                        };

                        trace!(
                            "MQTT v3.1.1 client {extracted_id} connected, clean_session={clean_session}"
                        );

                        // For v3.1.1: clean_session=false means session persists indefinitely
                        let session_expiry_interval = if clean_session { 0 } else { u32::MAX };
                        let need_keep = !clean_session;

                        // v3.1.1 doesn't use Assigned Client Identifier property
                        (
                            extracted_id,
                            user_name,
                            password,
                            session_expiry_interval,
                            need_keep,
                            clean_session,
                            None, // assigned_client_id
                            None, // request_response_information (v3.1.1 doesn't support this)
                            will_message,
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
                        let password = connect
                            .password()
                            .and_then(|p| String::from_utf8(p.to_vec()).ok());
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

                        // Extract Request Response Information from properties (default: 0 = not requested)
                        let request_response_information = connect
                            .props()
                            .iter()
                            .find_map(|prop| {
                                if let mqtt_ep::packet::Property::RequestResponseInformation(_) =
                                    prop
                                {
                                    prop.as_u8()
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0);

                        let need_keep = session_expiry_interval > 0;

                        // Extract Will message if present
                        let will_message = if connect.will_flag() {
                            let will_topic = connect
                                .will_topic()
                                .map(|s| s.to_string())
                                .unwrap_or_default();
                            let will_payload = connect.will_payload().unwrap_or_default();
                            let will_qos = connect.will_qos();
                            let will_retain = connect.will_retain();
                            let will_props = connect.will_props().to_vec();

                            // Check if Will QoS exceeds maximum_qos
                            if will_qos > self.maximum_qos {
                                error!(
                                    "MQTT v5.0 client {extracted_id} Will QoS {will_qos:?} exceeds maximum QoS {:?}",
                                    self.maximum_qos
                                );
                                let connack = mqtt_ep::packet::v5_0::Connack::builder()
                                    .session_present(false)
                                    .reason_code(
                                        mqtt_ep::result_code::ConnectReasonCode::QosNotSupported,
                                    )
                                    .build()
                                    .unwrap();
                                let _ = endpoint.send(connack).await;
                                return None;
                            }

                            // Extract Will Delay Interval from Will properties
                            let will_delay_interval = will_props
                                .iter()
                                .find_map(|prop| {
                                    if let mqtt_ep::packet::Property::WillDelayInterval(_) = prop {
                                        prop.as_u32()
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(0);

                            trace!(
                                "MQTT v5.0 client {extracted_id} has Will message: topic={will_topic}, qos={will_qos:?}, retain={will_retain}, will_delay_interval={will_delay_interval}"
                            );

                            Some(WillMessage {
                                topic: will_topic,
                                payload: will_payload.into_payload(),
                                qos: will_qos,
                                retain: will_retain,
                                props: will_props,
                                will_delay_interval,
                                registered_at: std::time::Instant::now(),
                            })
                        } else {
                            None
                        };

                        trace!(
                            "MQTT v5.0 client {extracted_id} connected, clean_start={clean_start}, session_expiry_interval={session_expiry_interval}, request_response_information={request_response_information}"
                        );

                        (
                            extracted_id,
                            user_name,
                            password,
                            session_expiry_interval,
                            need_keep,
                            clean_start,
                            assigned_client_id,
                            Some(request_response_information),
                            will_message,
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

        // Step 1: Disconnect existing online session if any (session takeover)
        session_store.disconnect_existing_session(&session_id).await;

        // Step 2: Check if session exists after disconnect
        let existing_session = session_store.get_session(&session_id).await;

        let session_present = if clean_start_or_session {
            // Clean session/start: remove existing session if any
            if let Some(ref existing_sess) = existing_session {
                // Check if there's a Will message that needs to be sent before deletion
                let session_guard = existing_sess.read().await;
                if let Some(will) = session_guard.will_message() {
                    let will_clone = will.clone();
                    drop(session_guard);

                    debug!(
                        "Sending Will message before session deletion due to CleanStart:1 for {client_id}"
                    );

                    // Create SessionRef for the existing session
                    let existing_session_ref = SessionRef::new(session_id.clone());

                    // Publish Will message
                    Self::publish_will_message(
                        &will_clone.topic,
                        will_clone.qos,
                        will_clone.retain,
                        will_clone.payload.clone(),
                        will_clone.props.clone(),
                        will_clone.registered_at,
                        &subscription_store,
                        &retained_store,
                        &session_store,
                        &existing_session_ref,
                        &security,
                    )
                    .await;
                } else {
                    drop(session_guard);
                }

                session_store.remove_session(&session_id).await;
            }
            false
        } else {
            // Check if session exists
            existing_session.is_some()
        };

        trace!("Session takeover complete for {client_id}, session_present={session_present}");

        // Determine Response Topic based on Request Response Information
        // Only for MQTT v5.0 (request_response_information is Some for v5.0, None for v3.1.1)
        let response_topic = if let Some(req_resp_info) = request_response_information {
            if req_resp_info == 1 {
                // Client requested Response Information
                if session_present {
                    // Try to use existing Response Topic from session
                    if let Some(existing) = &existing_session {
                        let existing_guard = existing.read().await;
                        if let Some(existing_topic) = existing_guard.response_topic() {
                            trace!(
                                "Reusing existing Response Topic for {client_id}: {existing_topic}"
                            );
                            Some(existing_topic.to_string())
                        } else {
                            // Session exists but no Response Topic, generate new one
                            let new_topic = format!("res_{}", uuid::Uuid::new_v4().simple());
                            trace!("Generated new Response Topic for existing session {client_id}: {new_topic}");
                            Some(new_topic)
                        }
                    } else {
                        // Should not happen (session_present but no existing_session)
                        let new_topic = format!("res_{}", uuid::Uuid::new_v4().simple());
                        trace!("Generated new Response Topic for {client_id}: {new_topic}");
                        Some(new_topic)
                    }
                } else {
                    // New session, generate new Response Topic
                    let new_topic = format!("res_{}", uuid::Uuid::new_v4().simple());
                    trace!(
                        "Generated new Response Topic for {client_id}: '{new_topic}' (length: {})",
                        new_topic.len()
                    );
                    Some(new_topic)
                }
            } else {
                // Client did not request Response Information (req_resp_info == 0)
                // Clear any existing Response Topic
                if session_present {
                    if let Some(existing) = &existing_session {
                        let existing_guard = existing.read().await;
                        if existing_guard.response_topic().is_some() {
                            trace!("Clearing Response Topic for {client_id} (not requested)");
                        }
                    }
                }
                None
            }
        } else {
            // MQTT v3.1.1 - no Response Topic support
            None
        };

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
            if let Err(e) = self
                .send_connack_for_restore(
                    &endpoint_arc,
                    &client_id,
                    session_expiry_interval,
                    response_topic.as_deref(),
                )
                .await
            {
                error!("Failed to send CONNACK for {client_id}: {e}");
                return None;
            }

            // Send offline messages
            Self::send_offline_messages(&endpoint_arc, &client_id, offline_messages).await;

            // Get session and update Response Topic and Will message
            match session_store.get_session(&session_id).await {
                Some(s) => {
                    // Update Response Topic and Will message in session
                    let mut session_guard = s.write().await;
                    let old_response_topic = session_guard.response_topic().map(|s| s.to_string());
                    session_guard.set_response_topic(response_topic.clone());
                    session_guard.set_will_message(will_message.clone());
                    drop(session_guard);

                    // Unregister old Response Topic if it exists and is different
                    if let Some(old_topic) = old_response_topic {
                        if response_topic.as_ref() != Some(&old_topic) {
                            session_store.unregister_response_topic(&old_topic).await;
                            trace!("Unregistered old Response Topic: {old_topic}");
                        }
                    }

                    // Register new Response Topic
                    if let Some(ref new_topic) = response_topic {
                        session_store
                            .register_response_topic(new_topic.clone())
                            .await;
                        trace!("Registered Response Topic: {new_topic}");
                    }

                    s
                }
                None => {
                    error!("Session disappeared after restoration for {client_id}");
                    return None;
                }
            }
        } else {
            // New session or clean start
            trace!("Creating new session for client {client_id}");

            // Send CONNACK with session_present=false
            if let Err(e) = self
                .send_connack_for_new(
                    &endpoint_arc,
                    &client_id,
                    session_expiry_interval,
                    assigned_client_id.as_deref(),
                    response_topic.as_deref(),
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

            // Set Response Topic and Will message in new session
            let mut session_guard = session.write().await;
            let old_response_topic = session_guard.response_topic().map(|s| s.to_string());
            session_guard.set_response_topic(response_topic.clone());
            session_guard.set_will_message(will_message.clone());
            drop(session_guard);

            // Unregister old Response Topic if it exists and is different
            if let Some(old_topic) = old_response_topic {
                if response_topic.as_ref() != Some(&old_topic) {
                    session_store.unregister_response_topic(&old_topic).await;
                    trace!("Unregistered old Response Topic: {old_topic}");
                }
            }

            // Register new Response Topic
            if let Some(ref new_topic) = response_topic {
                session_store
                    .register_response_topic(new_topic.clone())
                    .await;
                trace!("Registered Response Topic: {new_topic}");
            }

            session
        };

        // Create session reference
        let session_ref = SessionRef::new(session_id.clone());

        trace!(
            "Registered client {client_id} with session_expiry_interval={session_expiry_interval}"
        );

        trace!("Starting main endpoint loop for client {client_id}");

        // Flag to determine if Will message should be sent
        // false = send Will (abnormal disconnect)
        // true = don't send Will (normal disconnect)
        let mut normal_disconnect = false;

        // Main packet processing loop
        loop {
            // trace!("🔄 [{}] Waiting for packet...", client_id);
            match endpoint_arc.recv().await {
                Ok(packet) => {
                    // trace!("📥 [{}] Received packet: {:?}", client_id, packet.packet_type());

                    // Check for DISCONNECT packet
                    match &packet {
                        mqtt_ep::packet::Packet::V3_1_1Disconnect(_) => {
                            trace!("Received DISCONNECT from client {client_id} (v3.1.1)");
                            normal_disconnect = true;
                            break;
                        }
                        mqtt_ep::packet::Packet::V5_0Disconnect(disconnect) => {
                            let reason_code = disconnect.reason_code();
                            trace!(
                                "Received DISCONNECT from client {client_id} (v5.0), reason_code={reason_code:?}"
                            );

                            // Check if Will should be sent based on Reason Code
                            // 0x04 = DisconnectWithWillMessage: send Will
                            // All others: don't send Will
                            normal_disconnect = reason_code
                                != Some(mqtt_ep::result_code::DisconnectReasonCode::DisconnectWithWillMessage);
                            break;
                        }
                        _ => {
                            // Handle other packets
                            if let Err(e) = self
                                .handle_received_packet_in_endpoint(
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
                    }
                }
                Err(e) => {
                    info!("❌ Connection error for client {client_id}: {e}");
                    break;
                }
            }
        }

        trace!("Endpoint task finished for client {client_id}");

        // Check if session will be kept to determine Will Delay Interval behavior
        let session_guard = session.read().await;
        let need_keep = session_guard.need_keep();
        drop(session_guard);

        trace!("Disconnect for {client_id}: normal_disconnect={normal_disconnect}, need_keep={need_keep}");

        // Publish Will message if needed (abnormal disconnect or DisconnectWithWillMessage)
        if !normal_disconnect {
            let session_guard = session.read().await;
            if let Some(will) = session_guard.will_message() {
                let will_delay_interval = will.will_delay_interval;
                let will_clone = will.clone();
                drop(session_guard);

                trace!("Will message found: delay={will_delay_interval}s, need_keep={need_keep}");

                // Check if we should send Will immediately or start a timer
                if will_delay_interval == 0 || !need_keep {
                    // Send Will immediately: either no delay specified or session won't be kept
                    trace!(
                        "Publishing Will message immediately for client {client_id}: topic={}, will_delay_interval={will_delay_interval}, need_keep={need_keep}",
                        will_clone.topic
                    );

                    // Publish Will message to all subscribers
                    Self::publish_will_message(
                        &will_clone.topic,
                        will_clone.qos,
                        will_clone.retain,
                        will_clone.payload.clone(),
                        will_clone.props.clone(),
                        will_clone.registered_at,
                        &subscription_store,
                        &retained_store,
                        &session_store,
                        &session_ref,
                        &security,
                    )
                    .await;

                    // Clear Will message after sending
                    let mut session_guard = session.write().await;
                    session_guard.set_will_message(None);
                    drop(session_guard);
                } else {
                    // Start Will Delay Interval timer
                    trace!(
                        "Starting Will Delay Interval timer for {client_id}: {will_delay_interval} seconds"
                    );

                    let session_clone = session.clone();
                    let subscription_store_clone = subscription_store.clone();
                    let retained_store_clone = retained_store.clone();
                    let session_store_clone = session_store.clone();
                    let session_ref_clone = session_ref.clone();
                    let security_clone = security.clone();
                    let client_id_clone = client_id.clone();

                    let will_delay_timer = tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(will_delay_interval as u64)).await;

                        trace!("Will Delay Interval timer expired for {client_id_clone}, publishing Will message");

                        // Get Will message and publish it
                        let session_guard = session_clone.read().await;
                        if let Some(will) = session_guard.will_message() {
                            let will_clone = will.clone();
                            drop(session_guard);

                            Self::publish_will_message(
                                &will_clone.topic,
                                will_clone.qos,
                                will_clone.retain,
                                will_clone.payload.clone(),
                                will_clone.props.clone(),
                                will_clone.registered_at,
                                &subscription_store_clone,
                                &retained_store_clone,
                                &session_store_clone,
                                &session_ref_clone,
                                &security_clone,
                            )
                            .await;

                            // Clear Will message after sending
                            let mut session_guard = session_clone.write().await;
                            session_guard.set_will_message(None);
                        }
                    });

                    // Store the timer handle in the session
                    let mut session_guard = session.write().await;
                    session_guard.set_will_delay_timer(will_delay_timer);
                    drop(session_guard);
                }
            } else {
                drop(session_guard);
            }
        } else {
            // Normal disconnect: clear Will without sending
            trace!("Normal disconnect for client {client_id}, clearing Will without sending");
            let mut session_guard = session.write().await;
            session_guard.set_will_message(None);
            drop(session_guard);
        }

        // Handle session cleanup on disconnect
        let session_guard = session.read().await;
        let need_keep = session_guard.need_keep();
        drop(session_guard);

        if need_keep {
            // Session should persist: mark as offline
            trace!("Preserving session for client {client_id} (need_keep=true)");
            let mut session_guard = session.write().await;
            let expiry_interval = session_guard.session_expiry_interval();
            session_guard.set_offline();
            drop(session_guard);

            // Start session expiry timer if expiry_interval > 0
            if expiry_interval > 0 {
                let session_id_clone = session_id.clone();
                let session_store_clone = session_store.clone();
                let subscription_tx_clone = subscription_tx.clone();
                let client_id_clone = client_id.clone();
                let session_clone = session.clone();
                let subscription_store_clone = subscription_store.clone();
                let retained_store_clone = retained_store.clone();
                let security_clone = security.clone();
                let session_ref_clone = session_ref.clone();

                debug!(
                    "Starting session expiry timer for {client_id_clone}: {expiry_interval} seconds"
                );

                let timer = tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(expiry_interval as u64)).await;

                    debug!("Session expiry timer expired for {client_id_clone}, removing session");

                    // Check if there's a Will message that needs to be sent before deletion
                    let session_guard = session_clone.read().await;
                    if let Some(will) = session_guard.will_message() {
                        let will_clone = will.clone();
                        drop(session_guard);

                        debug!(
                            "Sending Will message before session deletion due to expiry for {client_id_clone}"
                        );

                        // Publish Will message
                        BrokerManager::publish_will_message(
                            &will_clone.topic,
                            will_clone.qos,
                            will_clone.retain,
                            will_clone.payload.clone(),
                            will_clone.props.clone(),
                            will_clone.registered_at,
                            &subscription_store_clone,
                            &retained_store_clone,
                            &session_store_clone,
                            &session_ref_clone,
                            &security_clone,
                        )
                        .await;
                    } else {
                        drop(session_guard);
                    }

                    // Remove session from session store
                    session_store_clone.remove_session(&session_id_clone).await;

                    // Remove all subscriptions for this session
                    let session_ref = SessionRef::new(session_id_clone.clone());
                    let _ = subscription_tx_clone
                        .send(SubscriptionMessage::ClientDisconnected { session_ref })
                        .await;

                    trace!("Session {session_id_clone} removed due to expiry");
                });

                let mut session_guard = session.write().await;
                session_guard.set_expiry_timer(timer);
            }
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
        &self,
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
                self.handle_subscribe(
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
                self.handle_subscribe(
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
                self.handle_publish(
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
                    security,
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

                self.handle_publish(
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
                    security,
                )
                .await?;
            }
            mqtt_ep::packet::Packet::V3_1_1Puback(_puback) => {
                // QoS 1: Received PUBACK (client acknowledged our publish)
                // v3.1.1 has no MessageExpiryInterval - no tracking
                trace!("Received PUBACK from client {client_id}");
            }
            mqtt_ep::packet::Packet::V5_0Puback(puback) => {
                // QoS 1: Received PUBACK (client acknowledged our publish)
                let packet_id = puback.packet_id();
                trace!("Received PUBACK from client {client_id}, packet_id={packet_id}");

                // Remove outgoing PUBLISH tracking (v5.0 only)
                if let Some(session_arc) = session_store.get_session(&session_ref.session_id).await
                {
                    let mut session_guard = session_arc.write().await;
                    session_guard.remove_outgoing_publish(packet_id);
                }
            }
            mqtt_ep::packet::Packet::V3_1_1Pubrec(pubrec) => {
                // QoS 2: Received PUBREC, send PUBREL
                // v3.1.1 has no MessageExpiryInterval - no tracking
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
                let packet_id = pubrec.packet_id();
                trace!("Received PUBREC from client {client_id}, packet_id={packet_id}");

                // Remove outgoing PUBLISH tracking (v5.0 only)
                // At this point, stored_packet changes from Publish to Pubrel
                if let Some(session_arc) = session_store.get_session(&session_ref.session_id).await
                {
                    let mut session_guard = session_arc.write().await;
                    session_guard.remove_outgoing_publish(packet_id);
                }

                Self::send_pubrel(
                    endpoint,
                    packet_id,
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
                // Tracking already removed at PUBREC stage
            }
            mqtt_ep::packet::Packet::V5_0Pubcomp(_pubcomp) => {
                // QoS 2: Received PUBCOMP (client acknowledged our PUBREL)
                trace!("Received PUBCOMP from client {client_id}");
                // Tracking already removed at PUBREC stage
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
                // DISCONNECT packets are handled in the main loop
                // This code path should not be reached
                unreachable!("DISCONNECT packets should be handled in main loop");
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
        &self,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        client_id: &str,
        _session_expiry_interval: u32,
        response_information: Option<&str>,
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
                let mut builder = mqtt_ep::packet::v5_0::Connack::builder()
                    .session_present(true)
                    .reason_code(mqtt_ep::result_code::ConnectReasonCode::Success);

                // Build properties list
                let mut props = Vec::new();

                // Add Response Information property if provided
                if let Some(response_info) = response_information {
                    trace!(
                        "Attempting to create Response Information with value: '{response_info}'"
                    );
                    match mqtt_ep::packet::ResponseInformation::new(response_info) {
                        Ok(ri) => {
                            props.push(mqtt_ep::packet::Property::ResponseInformation(ri));
                            trace!("Adding Response Information to CONNACK: {response_info}");
                        }
                        Err(e) => {
                            error!(
                                "Failed to create Response Information for '{response_info}': {e}"
                            );
                        }
                    }
                }

                // Add feature support properties
                if !self.retain_support {
                    props.push(mqtt_ep::packet::Property::RetainAvailable(
                        mqtt_ep::packet::RetainAvailable::new(0).unwrap(),
                    ));
                }
                if !self.shared_sub_support {
                    props.push(mqtt_ep::packet::Property::SharedSubscriptionAvailable(
                        mqtt_ep::packet::SharedSubscriptionAvailable::new(0).unwrap(),
                    ));
                }
                if !self.sub_id_support {
                    props.push(mqtt_ep::packet::Property::SubscriptionIdentifierAvailable(
                        mqtt_ep::packet::SubscriptionIdentifierAvailable::new(0).unwrap(),
                    ));
                }
                if !self.wc_support {
                    props.push(mqtt_ep::packet::Property::WildcardSubscriptionAvailable(
                        mqtt_ep::packet::WildcardSubscriptionAvailable::new(0).unwrap(),
                    ));
                }
                // Add Maximum QoS property if QoS is 0 or 1 (don't send if 2, as that would be ProtocolError)
                if self.maximum_qos != mqtt_ep::packet::Qos::ExactlyOnce {
                    let qos_value = match self.maximum_qos {
                        mqtt_ep::packet::Qos::AtMostOnce => 0,
                        mqtt_ep::packet::Qos::AtLeastOnce => 1,
                        mqtt_ep::packet::Qos::ExactlyOnce => unreachable!(),
                    };
                    props.push(mqtt_ep::packet::Property::MaximumQos(
                        mqtt_ep::packet::MaximumQos::new(qos_value).unwrap(),
                    ));
                }
                // Add Receive Maximum property if set
                if let Some(receive_maximum) = self.receive_maximum {
                    props.push(mqtt_ep::packet::Property::ReceiveMaximum(
                        mqtt_ep::packet::ReceiveMaximum::new(receive_maximum).unwrap(),
                    ));
                }
                // Add Maximum Packet Size property if set
                if let Some(maximum_packet_size) = self.maximum_packet_size {
                    props.push(mqtt_ep::packet::Property::MaximumPacketSize(
                        mqtt_ep::packet::MaximumPacketSize::new(maximum_packet_size).unwrap(),
                    ));
                }
                // Add Topic Alias Maximum property if set
                if let Some(topic_alias_maximum) = self.topic_alias_maximum {
                    props.push(mqtt_ep::packet::Property::TopicAliasMaximum(
                        mqtt_ep::packet::TopicAliasMaximum::new(topic_alias_maximum).unwrap(),
                    ));
                }

                if !props.is_empty() {
                    builder = builder.props(props);
                }

                let connack = builder.build().unwrap();
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
        &self,
        endpoint: &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        client_id: &str,
        _session_expiry_interval: u32,
        assigned_client_id: Option<&str>,
        response_information: Option<&str>,
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

                // Build properties list
                let mut props = Vec::new();

                // Add Assigned Client Identifier property if ClientId was auto-assigned
                if let Some(assigned_id) = assigned_client_id {
                    props.push(mqtt_ep::packet::Property::AssignedClientIdentifier(
                        mqtt_ep::packet::AssignedClientIdentifier::new(assigned_id)
                            .expect("Failed to create AssignedClientIdentifier"),
                    ));
                    trace!("Adding Assigned Client Identifier to CONNACK: {assigned_id}");
                }

                // Add Response Information property if provided
                if let Some(response_info) = response_information {
                    trace!(
                        "Attempting to create Response Information with value: '{response_info}'"
                    );
                    match mqtt_ep::packet::ResponseInformation::new(response_info) {
                        Ok(ri) => {
                            props.push(mqtt_ep::packet::Property::ResponseInformation(ri));
                            trace!("Adding Response Information to CONNACK: {response_info}");
                        }
                        Err(e) => {
                            error!(
                                "Failed to create Response Information for '{response_info}': {e}"
                            );
                        }
                    }
                }

                // Add feature support properties
                if !self.retain_support {
                    props.push(mqtt_ep::packet::Property::RetainAvailable(
                        mqtt_ep::packet::RetainAvailable::new(0).unwrap(),
                    ));
                }
                if !self.shared_sub_support {
                    props.push(mqtt_ep::packet::Property::SharedSubscriptionAvailable(
                        mqtt_ep::packet::SharedSubscriptionAvailable::new(0).unwrap(),
                    ));
                }
                if !self.sub_id_support {
                    props.push(mqtt_ep::packet::Property::SubscriptionIdentifierAvailable(
                        mqtt_ep::packet::SubscriptionIdentifierAvailable::new(0).unwrap(),
                    ));
                }
                if !self.wc_support {
                    props.push(mqtt_ep::packet::Property::WildcardSubscriptionAvailable(
                        mqtt_ep::packet::WildcardSubscriptionAvailable::new(0).unwrap(),
                    ));
                }
                // Add Maximum QoS property if QoS is 0 or 1 (don't send if 2, as that would be ProtocolError)
                if self.maximum_qos != mqtt_ep::packet::Qos::ExactlyOnce {
                    let qos_value = match self.maximum_qos {
                        mqtt_ep::packet::Qos::AtMostOnce => 0,
                        mqtt_ep::packet::Qos::AtLeastOnce => 1,
                        mqtt_ep::packet::Qos::ExactlyOnce => unreachable!(),
                    };
                    props.push(mqtt_ep::packet::Property::MaximumQos(
                        mqtt_ep::packet::MaximumQos::new(qos_value).unwrap(),
                    ));
                }
                // Add Receive Maximum property if set
                if let Some(receive_maximum) = self.receive_maximum {
                    props.push(mqtt_ep::packet::Property::ReceiveMaximum(
                        mqtt_ep::packet::ReceiveMaximum::new(receive_maximum).unwrap(),
                    ));
                }
                // Add Maximum Packet Size property if set
                if let Some(maximum_packet_size) = self.maximum_packet_size {
                    props.push(mqtt_ep::packet::Property::MaximumPacketSize(
                        mqtt_ep::packet::MaximumPacketSize::new(maximum_packet_size).unwrap(),
                    ));
                }
                // Add Topic Alias Maximum property if set
                if let Some(topic_alias_maximum) = self.topic_alias_maximum {
                    props.push(mqtt_ep::packet::Property::TopicAliasMaximum(
                        mqtt_ep::packet::TopicAliasMaximum::new(topic_alias_maximum).unwrap(),
                    ));
                }

                if !props.is_empty() {
                    trace!("Setting {} properties on CONNACK builder", props.len());
                    builder = builder.props(props);
                }

                trace!("Sending CONNACK (session_present=false) to client {client_id}");
                let connack = builder.build().map_err(|e| {
                    error!("Failed to build CONNACK: {e}");
                    anyhow::anyhow!("Failed to build CONNACK: {e}")
                })?;
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
            // Check MessageExpiryInterval and update props
            let (updated_props, is_expired) =
                Self::update_message_expiry_interval(&msg.props, msg.stored_at);

            if is_expired {
                trace!(
                    "Offline message for topic '{}' has expired, skipping",
                    msg.topic_name
                );
                continue; // Skip expired message (no PacketId acquired, nothing to release)
            }

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

                    if !updated_props.is_empty() {
                        builder = builder.props(updated_props.clone());
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
