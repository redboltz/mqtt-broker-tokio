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
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, trace};

/// Session identifier (UserName, ClientId)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId {
    pub user_name: Option<String>,
    pub client_id: String,
}

impl SessionId {
    pub fn new(user_name: Option<String>, client_id: String) -> Self {
        Self {
            user_name,
            client_id,
        }
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref user_name) = self.user_name {
            write!(f, "{}:{}", user_name, self.client_id)
        } else {
            write!(f, "{}", self.client_id)
        }
    }
}

/// Offline message stored for QoS1/QoS2
#[derive(Debug, Clone)]
pub struct OfflineMessage {
    pub topic_name: String,
    pub qos: mqtt_ep::packet::Qos,
    pub retain: bool,
    pub payload: mqtt_ep::common::ArcPayload,
    pub props: Vec<mqtt_ep::packet::Property>,
}

/// Will message (Last Will and Testament)
#[derive(Debug, Clone)]
pub struct WillMessage {
    pub topic: String,
    pub payload: mqtt_ep::common::ArcPayload,
    pub qos: mqtt_ep::packet::Qos,
    pub retain: bool,
    pub props: Vec<mqtt_ep::packet::Property>,
    pub will_delay_interval: u32, // Will Delay Interval in seconds (MQTT v5.0)
}

/// Session state
pub struct Session {
    /// Session identifier
    pub session_id: SessionId,

    /// Current endpoint (kept even when offline for session restoration)
    endpoint: Option<Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>>,

    /// Whether the session is currently online
    online: bool,

    /// Offline messages (QoS1/QoS2 only)
    offline_messages: Vec<OfflineMessage>,

    /// Session expiry interval in seconds (0 means delete on disconnect)
    session_expiry_interval: u32,

    /// Session expiry timer handle (v5.0)
    expiry_timer: Option<tokio::task::JoinHandle<()>>,

    /// Whether to keep session on disconnect (v3.1.1: !clean_session, v5.0: session_expiry_interval > 0)
    need_keep: bool,

    /// Response Topic for Request/Response pattern (MQTT v5.0)
    /// Generated when Request Response Information is 1 in CONNECT
    response_topic: Option<String>,

    /// Will message (Last Will and Testament)
    will_message: Option<WillMessage>,

    /// Will Delay Interval timer handle (MQTT v5.0)
    will_delay_timer: Option<tokio::task::JoinHandle<()>>,
}

impl Session {
    /// Create a new session
    pub fn new(
        session_id: SessionId,
        endpoint: Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        session_expiry_interval: u32,
        need_keep: bool,
    ) -> Self {
        Self {
            session_id,
            endpoint: Some(endpoint),
            online: true, // New session starts as online
            offline_messages: Vec::new(),
            session_expiry_interval,
            expiry_timer: None,
            need_keep,
            response_topic: None,
            will_message: None,
            will_delay_timer: None,
        }
    }

    /// Get response topic
    pub fn response_topic(&self) -> Option<&str> {
        self.response_topic.as_deref()
    }

    /// Set response topic
    pub fn set_response_topic(&mut self, topic: Option<String>) {
        self.response_topic = topic;
    }

    /// Get will message
    pub fn will_message(&self) -> Option<&WillMessage> {
        self.will_message.as_ref()
    }

    /// Set will message
    pub fn set_will_message(&mut self, will: Option<WillMessage>) {
        self.will_message = will;
    }

    #[allow(dead_code)]
    /// Take will message (removes and returns it)
    pub fn take_will_message(&mut self) -> Option<WillMessage> {
        self.will_message.take()
    }

    /// Get session expiry interval
    pub fn session_expiry_interval(&self) -> u32 {
        self.session_expiry_interval
    }

    /// Set session expiry interval
    pub fn set_session_expiry_interval(&mut self, interval: u32) {
        self.session_expiry_interval = interval;
    }

    /// Get need_keep flag
    pub fn need_keep(&self) -> bool {
        self.need_keep
    }

    /// Set need_keep flag
    pub fn set_need_keep(&mut self, need_keep: bool) {
        self.need_keep = need_keep;
    }

    /// Get endpoint reference (if online)
    pub fn endpoint(&self) -> Option<&Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>> {
        self.endpoint.as_ref()
    }

    #[allow(dead_code)]
    /// Check if session is online
    pub fn is_online(&self) -> bool {
        self.online
    }

    /// Set endpoint and mark as online
    pub fn set_endpoint(&mut self, endpoint: Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>) {
        self.endpoint = Some(endpoint);
        self.online = true;

        // Cancel expiry timer when coming online
        if let Some(timer) = self.expiry_timer.take() {
            timer.abort();
        }

        // Cancel will delay timer when coming online
        if let Some(timer) = self.will_delay_timer.take() {
            timer.abort();
        }
    }

    /// Mark session as offline
    /// Note: We don't clear the endpoint here because it's needed for session restoration
    /// (to get stored_packets and QoS2 PIDs). The send_publish method checks self.online
    /// instead of self.endpoint to determine if messages should be stored offline.
    pub fn set_offline(&mut self) {
        self.online = false;
    }

    /// Add offline message
    pub fn add_offline_message(&mut self, message: OfflineMessage) {
        // Only store QoS1/QoS2 messages
        if matches!(
            message.qos,
            mqtt_ep::packet::Qos::AtLeastOnce | mqtt_ep::packet::Qos::ExactlyOnce
        ) {
            self.offline_messages.push(message);
            trace!(
                "Added offline message for session {:?}, total: {}",
                self.session_id,
                self.offline_messages.len()
            );
        }
    }

    /// Take all offline messages
    pub fn take_offline_messages(&mut self) -> Vec<OfflineMessage> {
        std::mem::take(&mut self.offline_messages)
    }

    #[allow(dead_code)]
    /// Get offline message count
    pub fn offline_message_count(&self) -> usize {
        self.offline_messages.len()
    }

    /// Set session expiry timer
    pub fn set_expiry_timer(&mut self, timer: tokio::task::JoinHandle<()>) {
        // Cancel existing timer if any
        if let Some(old_timer) = self.expiry_timer.take() {
            old_timer.abort();
        }
        self.expiry_timer = Some(timer);
    }

    /// Clear session expiry timer
    pub fn clear_expiry_timer(&mut self) {
        if let Some(timer) = self.expiry_timer.take() {
            timer.abort();
        }
    }

    /// Set will delay timer
    pub fn set_will_delay_timer(&mut self, timer: tokio::task::JoinHandle<()>) {
        // Cancel existing timer if any
        if let Some(old_timer) = self.will_delay_timer.take() {
            old_timer.abort();
        }
        self.will_delay_timer = Some(timer);
    }

    /// Clear will delay timer
    pub fn clear_will_delay_timer(&mut self) {
        if let Some(timer) = self.will_delay_timer.take() {
            timer.abort();
        }
    }

    /// Send PUBLISH to session (online) or store offline message (offline)
    pub async fn send_publish(
        &mut self,
        topic_name: String,
        qos: mqtt_ep::packet::Qos,
        retain: bool,
        payload: mqtt_ep::common::ArcPayload,
        props: Vec<mqtt_ep::packet::Property>,
    ) {
        if self.online {
            // Online: send message directly
            if let Some(endpoint) = &self.endpoint {
                // Get endpoint version to build appropriate PUBLISH packet
                let endpoint_version = endpoint
                    .get_protocol_version()
                    .await
                    .unwrap_or(mqtt_ep::Version::V5_0);

                match endpoint_version {
                    mqtt_ep::Version::V3_1_1 => {
                        let mut builder = mqtt_ep::packet::v3_1_1::Publish::builder()
                            .topic_name(&topic_name)
                            .expect("Failed to set topic_name")
                            .qos(qos)
                            .retain(retain)
                            .payload(payload.clone());

                        let publish = if qos != mqtt_ep::packet::Qos::AtMostOnce {
                            // Acquire packet ID for QoS > 0
                            let packet_id = endpoint.acquire_packet_id().await.unwrap_or(1);
                            builder = builder.packet_id(packet_id);
                            builder.build().expect("Failed to build PUBLISH")
                        } else {
                            builder.build().expect("Failed to build PUBLISH")
                        };

                        if let Err(e) = endpoint.send(publish).await {
                            debug!(
                                "Failed to send PUBLISH to session {:?}: {e}",
                                self.session_id
                            );
                        }
                    }
                    mqtt_ep::Version::V5_0 => {
                        let mut builder = mqtt_ep::packet::v5_0::Publish::builder()
                            .topic_name(&topic_name)
                            .expect("Failed to set topic_name")
                            .qos(qos)
                            .retain(retain)
                            .payload(payload.clone());

                        if !props.is_empty() {
                            builder = builder.props(props.clone());
                        }

                        let publish = if qos != mqtt_ep::packet::Qos::AtMostOnce {
                            // Acquire packet ID for QoS > 0
                            let packet_id = endpoint.acquire_packet_id().await.unwrap_or(1);
                            builder = builder.packet_id(packet_id);
                            builder.build().expect("Failed to build PUBLISH")
                        } else {
                            builder.build().expect("Failed to build PUBLISH")
                        };

                        if let Err(e) = endpoint.send(publish).await {
                            debug!(
                                "Failed to send PUBLISH to session {:?}: {e}",
                                self.session_id
                            );
                        }
                    }
                    _ => {
                        debug!("Unsupported MQTT version for session {:?}", self.session_id);
                    }
                }
            } else {
                debug!(
                    "Session is online but endpoint is None for {:?}",
                    self.session_id
                );
            }
        } else {
            // Offline: store message for QoS1/QoS2
            if matches!(
                qos,
                mqtt_ep::packet::Qos::AtLeastOnce | mqtt_ep::packet::Qos::ExactlyOnce
            ) {
                self.add_offline_message(OfflineMessage {
                    topic_name,
                    qos,
                    retain,
                    payload,
                    props,
                });
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // Clean up expiry timer on drop
        self.clear_expiry_timer();
        // Clean up will delay timer on drop
        self.clear_will_delay_timer();
    }
}

/// Reference to a session (like EndpointRef)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionRef {
    pub session_id: SessionId,
}

impl SessionRef {
    pub fn new(session_id: SessionId) -> Self {
        Self { session_id }
    }
}

/// Session store managing all sessions
pub struct SessionStore {
    /// Sessions indexed by SessionId
    sessions: Arc<RwLock<HashMap<SessionId, Arc<RwLock<Session>>>>>,
    /// Set of all active Response Topics for fast lookup
    response_topics: Arc<RwLock<std::collections::HashSet<String>>>,
}

impl SessionStore {
    /// Create a new session store
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            response_topics: Arc::new(RwLock::new(std::collections::HashSet::new())),
        }
    }

    /// Check if a topic is a Response Topic
    pub async fn is_response_topic(&self, topic: &str) -> bool {
        let topics = self.response_topics.read().await;
        topics.contains(topic)
    }

    /// Register a Response Topic
    pub async fn register_response_topic(&self, topic: String) {
        let mut topics = self.response_topics.write().await;
        topics.insert(topic);
    }

    /// Unregister a Response Topic
    pub async fn unregister_response_topic(&self, topic: &str) {
        let mut topics = self.response_topics.write().await;
        topics.remove(topic);
    }

    /// Get or create a session
    pub async fn get_or_create_session(
        &self,
        session_id: SessionId,
        endpoint: Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        session_expiry_interval: u32,
        need_keep: bool,
    ) -> (Arc<RwLock<Session>>, bool) {
        let mut sessions = self.sessions.write().await;

        if let Some(session) = sessions.get(&session_id) {
            // Existing session - update endpoint, expiry interval, and need_keep
            debug!("Found existing session for {:?}", session_id);
            let mut session_guard = session.write().await;
            session_guard.set_endpoint(endpoint);
            session_guard.set_session_expiry_interval(session_expiry_interval);
            session_guard.set_need_keep(need_keep);
            drop(session_guard);
            (session.clone(), false)
        } else {
            // Create new session
            debug!("Creating new session for {:?}", session_id);
            let session = Arc::new(RwLock::new(Session::new(
                session_id.clone(),
                endpoint,
                session_expiry_interval,
                need_keep,
            )));
            sessions.insert(session_id, session.clone());
            (session, true)
        }
    }

    /// Get existing session
    pub async fn get_session(&self, session_id: &SessionId) -> Option<Arc<RwLock<Session>>> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).cloned()
    }

    /// Remove session
    pub async fn remove_session(&self, session_id: &SessionId) -> Option<Arc<RwLock<Session>>> {
        let mut sessions = self.sessions.write().await;
        let removed = sessions.remove(session_id);

        // Unregister Response Topic if exists
        if let Some(ref session_arc) = removed {
            let session_guard = session_arc.read().await;
            let response_topic = session_guard.response_topic().map(|s| s.to_string());
            drop(session_guard);
            drop(sessions);

            if let Some(topic) = response_topic {
                self.unregister_response_topic(&topic).await;
            }
        }

        removed
    }
    #[allow(dead_code)]
    /// Get all session IDs
    pub async fn get_all_session_ids(&self) -> Vec<SessionId> {
        let sessions = self.sessions.read().await;
        sessions.keys().cloned().collect()
    }

    /// Disconnect existing online session (for session takeover)
    /// Returns true if session was disconnected, false if no online session existed
    pub async fn disconnect_existing_session(&self, session_id: &SessionId) -> bool {
        let sessions = self.sessions.read().await;
        if let Some(session_arc) = sessions.get(session_id) {
            let session_guard = session_arc.read().await;

            if let Some(old_endpoint) = session_guard.endpoint() {
                let old_endpoint_clone = old_endpoint.clone();
                drop(session_guard);
                drop(sessions);

                // Close the old endpoint
                trace!(
                    "Disconnecting existing session for {:?} (session takeover)",
                    session_id
                );
                if let Err(e) = old_endpoint_clone.close().await {
                    debug!("Error during old endpoint close: {e}");
                }

                true
            } else {
                // Session exists but already offline
                false
            }
        } else {
            // No session exists
            false
        }
    }

    /// Restore session state from offline session to new endpoint
    /// Returns offline messages that need to be sent
    pub async fn restore_session_state(
        &self,
        session_id: &SessionId,
        new_endpoint: Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        session_expiry_interval: u32,
        need_keep: bool,
    ) -> anyhow::Result<Vec<OfflineMessage>> {
        let sessions = self.sessions.read().await;
        if let Some(session_arc) = sessions.get(session_id) {
            let mut session_guard = session_arc.write().await;

            // Get stored packets and QoS2 PIDs from old endpoint (if it exists)
            if let Some(old_endpoint) = session_guard.endpoint() {
                trace!(
                    "Restoring session state from old endpoint for {:?}",
                    session_id
                );

                // Get stored packets and QoS2 PIDs from old endpoint
                let stored_packets = old_endpoint
                    .get_stored_packets()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to get stored packets: {e}"))?;
                let qos2_pids = old_endpoint
                    .get_qos2_publish_handled_pids()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to get QoS2 PIDs: {e}"))?;

                trace!(
                    "Retrieved stored_packets: {}, qos2_pids: {} from old endpoint",
                    stored_packets.len(),
                    qos2_pids.len()
                );

                // Take offline messages
                let offline_messages = session_guard.take_offline_messages();

                // Update session with new endpoint
                session_guard.set_endpoint(new_endpoint.clone());
                session_guard.set_session_expiry_interval(session_expiry_interval);
                session_guard.set_need_keep(need_keep);

                drop(session_guard);
                drop(sessions);

                // Restore stored packets and QoS2 PIDs to new endpoint
                new_endpoint
                    .restore_stored_packets(stored_packets)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to restore stored packets: {e}"))?;
                new_endpoint
                    .restore_qos2_publish_handled_pids(qos2_pids)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to restore QoS2 PIDs: {e}"))?;

                trace!(
                    "Restored session state for {:?}, offline_messages: {}",
                    session_id,
                    offline_messages.len()
                );

                Ok(offline_messages)
            } else {
                // No old endpoint - this should not happen in normal session restoration
                Err(anyhow::anyhow!(
                    "Cannot restore: no old endpoint exists for {:?}",
                    session_id
                ))
            }
        } else {
            Err(anyhow::anyhow!(
                "Session not found for restoration: {:?}",
                session_id
            ))
        }
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}
