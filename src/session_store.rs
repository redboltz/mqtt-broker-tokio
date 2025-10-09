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

/// Offline message stored for QoS1/QoS2
#[derive(Debug, Clone)]
pub struct OfflineMessage {
    pub topic_name: String,
    pub qos: mqtt_ep::packet::Qos,
    pub retain: bool,
    pub payload: mqtt_ep::common::ArcPayload,
    pub props: Vec<mqtt_ep::packet::Property>,
}

/// Session state
pub struct Session {
    /// Session identifier
    pub session_id: SessionId,

    /// Current endpoint (None if offline)
    endpoint: Option<Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>>,

    /// Offline messages (QoS1/QoS2 only)
    offline_messages: Vec<OfflineMessage>,

    /// Session expiry interval in seconds (0 means delete on disconnect)
    session_expiry_interval: u32,

    /// Session expiry timer handle (v5.0)
    expiry_timer: Option<tokio::task::JoinHandle<()>>,
}

impl Session {
    /// Create a new session
    pub fn new(
        session_id: SessionId,
        endpoint: Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        session_expiry_interval: u32,
    ) -> Self {
        Self {
            session_id,
            endpoint: Some(endpoint),
            offline_messages: Vec::new(),
            session_expiry_interval,
            expiry_timer: None,
        }
    }

    /// Get session expiry interval
    pub fn session_expiry_interval(&self) -> u32 {
        self.session_expiry_interval
    }

    /// Set session expiry interval
    pub fn set_session_expiry_interval(&mut self, interval: u32) {
        self.session_expiry_interval = interval;
    }

    /// Get endpoint reference (if online)
    pub fn endpoint(&self) -> Option<&Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>> {
        self.endpoint.as_ref()
    }

    /// Check if session is online
    pub fn is_online(&self) -> bool {
        self.endpoint.is_some()
    }

    /// Set endpoint (online)
    pub fn set_endpoint(&mut self, endpoint: Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>) {
        self.endpoint = Some(endpoint);

        // Cancel expiry timer when coming online
        if let Some(timer) = self.expiry_timer.take() {
            timer.abort();
        }
    }

    /// Clear endpoint (go offline)
    pub fn clear_endpoint(&mut self) {
        self.endpoint = None;
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

    /// Send PUBLISH to session (online) or store offline message (offline)
    pub async fn send_publish(
        &mut self,
        topic_name: String,
        qos: mqtt_ep::packet::Qos,
        retain: bool,
        payload: mqtt_ep::common::ArcPayload,
        props: Vec<mqtt_ep::packet::Property>,
    ) {
        if let Some(endpoint) = &self.endpoint {
            // Online: send message directly
            // Get endpoint version to build appropriate PUBLISH packet
            let endpoint_version = endpoint
                .get_protocol_version()
                .await
                .unwrap_or(mqtt_ep::Version::V5_0);

            match endpoint_version {
                mqtt_ep::Version::V3_1_1 => {
                    let publish = mqtt_ep::packet::v3_1_1::Publish::builder()
                        .topic_name(&topic_name)
                        .expect("Failed to set topic_name")
                        .qos(qos)
                        .retain(retain)
                        .payload(payload.clone())
                        .build()
                        .expect("Failed to build PUBLISH");

                    if let Err(e) = endpoint.send(publish).await {
                        debug!(
                            "Failed to send PUBLISH to session {:?}: {e}",
                            self.session_id
                        );
                    }
                }
                mqtt_ep::Version::V5_0 => {
                    let publish = mqtt_ep::packet::v5_0::Publish::builder()
                        .topic_name(&topic_name)
                        .expect("Failed to set topic_name")
                        .qos(qos)
                        .retain(retain)
                        .payload(payload.clone())
                        .props(props.clone())
                        .build()
                        .expect("Failed to build PUBLISH");

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
}

impl SessionStore {
    /// Create a new session store
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or create a session
    pub async fn get_or_create_session(
        &self,
        session_id: SessionId,
        endpoint: Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>,
        session_expiry_interval: u32,
    ) -> (Arc<RwLock<Session>>, bool) {
        let mut sessions = self.sessions.write().await;

        if let Some(session) = sessions.get(&session_id) {
            // Existing session - update endpoint and expiry interval
            debug!("Found existing session for {:?}", session_id);
            let mut session_guard = session.write().await;
            session_guard.set_endpoint(endpoint);
            session_guard.set_session_expiry_interval(session_expiry_interval);
            drop(session_guard);
            (session.clone(), false)
        } else {
            // Create new session
            debug!("Creating new session for {:?}", session_id);
            let session = Arc::new(RwLock::new(Session::new(
                session_id.clone(),
                endpoint,
                session_expiry_interval,
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
        sessions.remove(session_id)
    }

    /// Get all session IDs
    pub async fn get_all_session_ids(&self) -> Vec<SessionId> {
        let sessions = self.sessions.read().await;
        sessions.keys().cloned().collect()
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}
