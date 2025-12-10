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
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

use crate::session_store::SessionRef;

/// Simple error type for subscription operations
#[derive(Debug, Clone)]
pub enum SubscriptionError {
    InvalidTopicFilter,
}

impl std::fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriptionError::InvalidTopicFilter => write!(f, "Invalid topic filter"),
        }
    }
}

impl std::error::Error for SubscriptionError {}
#[allow(dead_code)]

pub type ClientId = String;

/// Subscription information containing session reference, QoS, topic filter, subscription ID, RAP flag, and NL flag
#[derive(Debug, Clone)]
pub struct Subscription {
    pub session_ref: SessionRef,
    pub qos: mqtt_ep::packet::Qos,
    pub topic_filter: String,
    pub sub_id: Option<u32>,
    pub rap: bool, // Retain As Published flag
    pub nl: bool,  // No Local flag (v5.0 only, false for v3.1.1)
}

/// Wrapper for Arc<Endpoint> that uses pointer-based comparison and hashing
#[derive(Clone)]
pub struct EndpointRef(Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>);

impl EndpointRef {
    pub fn new(endpoint: Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>) -> Self {
        Self(endpoint)
    }
    #[allow(dead_code)]

    pub fn endpoint(&self) -> &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>> {
        &self.0
    }
    #[allow(dead_code)]

    pub fn into_arc(self) -> Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>> {
        self.0
    }
}

impl Hash for EndpointRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.0.as_ref() as *const mqtt_ep::Endpoint<mqtt_ep::role::Server>).hash(state);
    }
}

impl PartialEq for EndpointRef {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for EndpointRef {}

impl PartialOrd for EndpointRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EndpointRef {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.0.as_ref() as *const mqtt_ep::Endpoint<mqtt_ep::role::Server>)
            .cmp(&(other.0.as_ref() as *const mqtt_ep::Endpoint<mqtt_ep::role::Server>))
    }
}

impl std::fmt::Debug for EndpointRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EndpointRef({:p})", Arc::as_ptr(&self.0))
    }
}

/// Trie node containing subscription information
#[derive(Debug, Clone, Default)]
struct TrieNode {
    /// Subscriptions to this exact path
    exact_subscribers: Vec<Subscription>,
    /// Subscriptions with single-level wildcard at this position
    single_wildcard_subscribers: Vec<Subscription>,
    /// Subscriptions with multi-level wildcard from this position
    multi_wildcard_subscribers: Vec<Subscription>,
    /// Child nodes for each segment
    children: HashMap<String, TrieNode>,
    /// Special child for single-level wildcard (+)
    wildcard_child: Option<Box<TrieNode>>,
}

/// Subscription store using Trie-based structure for efficient wildcard matching
#[derive(Debug, Clone)]
pub struct SubscriptionStore {
    /// Root of the trie structure
    root: Arc<RwLock<TrieNode>>,
    /// Shared subscriptions manager with LRU-based round-robin at share-name level
    shared_subscriptions:
        Arc<RwLock<crate::shared_subscription_manager::SharedSubscriptionManager>>,
}

impl SubscriptionStore {
    /// Create a new subscription store
    pub fn new() -> Self {
        Self {
            root: Arc::new(RwLock::new(TrieNode::default())),
            shared_subscriptions: Arc::new(RwLock::new(
                crate::shared_subscription_manager::SharedSubscriptionManager::new(),
            )),
        }
    }

    /// Parse shared subscription topic filter
    /// Returns None if not a shared subscription
    /// Returns Some((share_name, actual_topic_filter)) if it is
    fn parse_shared_subscription(topic_filter: &str) -> Option<(String, String)> {
        if let Some(stripped) = topic_filter.strip_prefix("$share/") {
            if let Some(slash_pos) = stripped.find('/') {
                let share_name = stripped[..slash_pos].to_string();
                let actual_filter = stripped[slash_pos + 1..].to_string();
                if !share_name.is_empty() && !actual_filter.is_empty() {
                    return Some((share_name, actual_filter));
                }
            }
        }
        None
    }

    /// Add a subscription for a session to a topic filter
    /// Returns Ok(is_new) where is_new is true if this is a new subscription, false if updating existing
    pub async fn subscribe(
        &self,
        session_ref: SessionRef,
        topic_filter: &str,
        qos: mqtt_ep::packet::Qos,
        sub_id: Option<u32>,
        rap: bool,
        nl: bool,
    ) -> Result<bool, SubscriptionError> {
        // Check if this is a shared subscription
        if let Some((share_name, actual_filter)) = Self::parse_shared_subscription(topic_filter) {
            // Validate the actual topic filter
            Self::validate_topic_filter(&actual_filter)?;

            let mut shared_subs = self.shared_subscriptions.write().await;

            // Use SharedSubscriptionManager to insert the subscription
            shared_subs.insert(
                share_name.clone(),
                actual_filter.clone(),
                session_ref,
                crate::shared_subscription_manager::SubscriptionDetails {
                    qos,
                    topic_filter: actual_filter.clone(),
                    sub_id,
                    rap,
                    nl,
                },
            );

            trace!(
                "Shared subscription: session subscribed to '{topic_filter}' (share: {share_name}, filter: {actual_filter}) with QoS {qos:?}"
            );
            // For now, always return true for shared subscriptions (could track is_new in manager if needed)
            Ok(true)
        } else {
            // Regular subscription
            Self::validate_topic_filter(topic_filter)?;

            let mut root = self.root.write().await;
            let segments: Vec<&str> = topic_filter.split('/').collect();

            let is_new = Self::insert_subscription(
                &mut root,
                &segments,
                session_ref.clone(),
                topic_filter,
                qos,
                sub_id,
                rap,
                nl,
                0,
            );

            trace!(
                "Subscribed session to topic filter '{topic_filter}' with QoS {qos:?}, sub_id {sub_id:?}, is_new: {is_new}"
            );
            Ok(is_new)
        }
    }

    /// Recursively insert subscription into trie
    /// Returns true if this is a new subscription, false if updating existing
    #[allow(clippy::too_many_arguments)]
    fn insert_subscription(
        node: &mut TrieNode,
        segments: &[&str],
        session_ref: SessionRef,
        topic_filter: &str,
        qos: mqtt_ep::packet::Qos,
        sub_id: Option<u32>,
        rap: bool,
        nl: bool,
        depth: usize,
    ) -> bool {
        if depth >= segments.len() {
            // End of path - add to exact subscribers
            return Self::upsert_subscription(
                &mut node.exact_subscribers,
                session_ref,
                topic_filter,
                qos,
                sub_id,
                rap,
                nl,
            );
        }

        let segment = segments[depth];

        match segment {
            "#" => {
                // Multi-level wildcard - matches everything from this point
                Self::upsert_subscription(
                    &mut node.multi_wildcard_subscribers,
                    session_ref,
                    topic_filter,
                    qos,
                    sub_id,
                    rap,
                    nl,
                )
            }
            "+" => {
                // Single-level wildcard
                if node.wildcard_child.is_none() {
                    node.wildcard_child = Some(Box::new(TrieNode::default()));
                }
                if let Some(ref mut wildcard_child) = node.wildcard_child {
                    if depth + 1 >= segments.len() {
                        // This is the last segment
                        Self::upsert_subscription(
                            &mut wildcard_child.single_wildcard_subscribers,
                            session_ref,
                            topic_filter,
                            qos,
                            sub_id,
                            rap,
                            nl,
                        )
                    } else {
                        Self::insert_subscription(
                            wildcard_child,
                            segments,
                            session_ref,
                            topic_filter,
                            qos,
                            sub_id,
                            rap,
                            nl,
                            depth + 1,
                        )
                    }
                } else {
                    false
                }
            }
            _ => {
                // Exact segment
                let child = node
                    .children
                    .entry(segment.to_string())
                    .or_insert_with(TrieNode::default);
                Self::insert_subscription(
                    child,
                    segments,
                    session_ref,
                    topic_filter,
                    qos,
                    sub_id,
                    rap,
                    nl,
                    depth + 1,
                )
            }
        }
    }

    /// Insert or update subscription entry (overwrite if identical topic_filter)
    /// Returns true if this is a new subscription, false if updating existing
    fn upsert_subscription(
        subscribers: &mut Vec<Subscription>,
        session_ref: SessionRef,
        topic_filter: &str,
        qos: mqtt_ep::packet::Qos,
        sub_id: Option<u32>,
        rap: bool,
        nl: bool,
    ) -> bool {
        // Find existing subscription with same session_ref and topic_filter
        if let Some(existing) = subscribers
            .iter_mut()
            .find(|s| s.session_ref == session_ref && s.topic_filter == topic_filter)
        {
            // Update existing subscription (QoS, sub_id, rap, and nl overwrite)
            existing.qos = qos;
            existing.sub_id = sub_id;
            existing.rap = rap;
            existing.nl = nl;
            false // Not a new subscription
        } else {
            // Add new subscription
            subscribers.push(Subscription {
                session_ref,
                qos,
                topic_filter: topic_filter.to_string(),
                sub_id,
                rap,
                nl,
            });
            true // New subscription
        }
    }

    /// Remove a subscription for a session from a topic filter
    pub async fn unsubscribe(
        &self,
        session_ref: &SessionRef,
        topic_filter: &str,
    ) -> Result<bool, SubscriptionError> {
        // Check if this is a shared subscription
        if let Some((share_name, actual_filter)) = Self::parse_shared_subscription(topic_filter) {
            let mut shared_subs = self.shared_subscriptions.write().await;

            // Use SharedSubscriptionManager to erase the subscription
            let removed = shared_subs.erase(&share_name, &actual_filter, session_ref);
            Ok(removed)
        } else {
            // Regular subscription
            Self::validate_topic_filter(topic_filter)?;

            let mut root = self.root.write().await;
            let segments: Vec<&str> = topic_filter.split('/').collect();

            let removed = Self::remove_subscription(&mut root, &segments, session_ref, 0);
            Ok(removed)
        }
    }

    /// Recursively remove subscription from trie
    fn remove_subscription(
        node: &mut TrieNode,
        segments: &[&str],
        session_ref: &SessionRef,
        depth: usize,
    ) -> bool {
        if depth >= segments.len() {
            return Self::remove_from_vec(&mut node.exact_subscribers, session_ref);
        }

        let segment = segments[depth];

        match segment {
            "#" => Self::remove_from_vec(&mut node.multi_wildcard_subscribers, session_ref),
            "+" => {
                if let Some(ref mut wildcard_child) = node.wildcard_child {
                    if depth + 1 >= segments.len() {
                        Self::remove_from_vec(
                            &mut wildcard_child.single_wildcard_subscribers,
                            session_ref,
                        )
                    } else {
                        Self::remove_subscription(wildcard_child, segments, session_ref, depth + 1)
                    }
                } else {
                    false
                }
            }
            _ => {
                if let Some(child) = node.children.get_mut(segment) {
                    Self::remove_subscription(child, segments, session_ref, depth + 1)
                } else {
                    false
                }
            }
        }
    }

    /// Remove session from subscription vector
    fn remove_from_vec(subscribers: &mut Vec<Subscription>, session_ref: &SessionRef) -> bool {
        if let Some(pos) = subscribers
            .iter()
            .position(|s| &s.session_ref == session_ref)
        {
            subscribers.remove(pos);
            true
        } else {
            false
        }
    }

    /// Remove all subscriptions for a session
    pub async fn unsubscribe_all(&self, session_ref: &SessionRef) {
        // Remove regular subscriptions
        let mut root = self.root.write().await;
        Self::remove_all_subscriptions(&mut root, session_ref);
        drop(root);

        // Remove shared subscriptions
        let mut shared_subs = self.shared_subscriptions.write().await;
        // Use SharedSubscriptionManager to remove all subscriptions for this session
        shared_subs.erase_session(session_ref);
    }

    /// Recursively remove all subscriptions for a session
    fn remove_all_subscriptions(node: &mut TrieNode, session_ref: &SessionRef) {
        node.exact_subscribers
            .retain(|s| &s.session_ref != session_ref);
        node.single_wildcard_subscribers
            .retain(|s| &s.session_ref != session_ref);
        node.multi_wildcard_subscribers
            .retain(|s| &s.session_ref != session_ref);

        // Recursively clean children
        for child in node.children.values_mut() {
            Self::remove_all_subscriptions(child, session_ref);
        }

        if let Some(ref mut wildcard_child) = node.wildcard_child {
            Self::remove_all_subscriptions(wildcard_child, session_ref);
        }
    }

    /// Find all subscriber endpoints for a given published topic
    ///
    /// # Arguments
    /// * `topic` - The topic to match subscribers against
    /// * `auth_checker` - Optional authorization checker function that returns true if a subscriber is authorized
    ///
    /// # Returns
    /// Vector of subscriptions for subscribers that match and are authorized (if checker provided)
    pub async fn find_subscribers<F>(
        &self,
        topic: &str,
        auth_checker: Option<F>,
    ) -> Vec<Subscription>
    where
        F: Fn(&SessionRef, &str) -> bool + Clone,
    {
        // Get regular subscribers
        let root = self.root.read().await;
        let mut all_subscribers = Vec::new();
        let segments: Vec<&str> = topic.split('/').collect();

        Self::collect_subscribers(&root, &segments, 0, &mut all_subscribers);
        drop(root);

        let mut result: Vec<Subscription> = all_subscribers;

        // Get shared subscribers (share-name level round-robin with LRU)
        let mut shared_subs = self.shared_subscriptions.write().await;

        // Find all matching shared subscriptions using the SharedSubscriptionManager
        // For performance optimization: only apply client filter for wildcard + auth_checker
        // We need to check if any subscription might have wildcard BEFORE calling find_all_targets
        // to avoid consuming turn counter twice
        let shared_matches = if let Some(ref checker) = auth_checker {
            // Check if any subscription group has a wildcard filter that could match this topic
            // This is a heuristic: we check if the topic has multiple segments which might match wildcards
            let might_match_wildcard = segments.len() > 1;

            if might_match_wildcard {
                // Use authorization filter to be safe
                let checker_clone = checker.clone();
                let topic_owned = topic.to_string();
                shared_subs.find_all_targets_with_filter(
                    &segments,
                    Self::topic_matches_filter,
                    move |session_ref| {
                        // Check if this client has authorization to receive this topic
                        checker_clone(session_ref, &topic_owned)
                    },
                )
            } else {
                // Single segment topic unlikely to match wildcards with different permissions
                shared_subs.find_all_targets(&segments, Self::topic_matches_filter)
            }
        } else {
            // No auth_checker, use normal find
            shared_subs.find_all_targets(&segments, Self::topic_matches_filter)
        };

        for (_share_name, session_ref, details) in shared_matches {
            result.push(Subscription {
                session_ref,
                qos: details.qos,
                topic_filter: details.topic_filter,
                sub_id: details.sub_id,
                rap: details.rap,
                nl: details.nl,
            });

            trace!("Shared subscription match: topic '{topic}' matched with LRU selection");
        }

        result
    }

    /// Check if a topic matches a topic filter (for shared subscriptions)
    fn topic_matches_filter(topic_segments: &[&str], filter_segments: &[&str]) -> bool {
        let mut topic_idx = 0;
        let mut filter_idx = 0;

        while filter_idx < filter_segments.len() {
            let filter_seg = filter_segments[filter_idx];

            if filter_seg == "#" {
                // Multi-level wildcard matches everything remaining
                return true;
            } else if filter_seg == "+" {
                // Single-level wildcard matches any single segment
                if topic_idx >= topic_segments.len() {
                    return false;
                }
                topic_idx += 1;
                filter_idx += 1;
            } else {
                // Exact match required
                if topic_idx >= topic_segments.len() || topic_segments[topic_idx] != filter_seg {
                    return false;
                }
                topic_idx += 1;
                filter_idx += 1;
            }
        }

        // Both must be at the end
        topic_idx == topic_segments.len()
    }

    /// Recursively collect all matching subscribers
    fn collect_subscribers(
        node: &TrieNode,
        topic_segments: &[&str],
        depth: usize,
        subscribers: &mut Vec<Subscription>,
    ) {
        // Multi-level wildcards match everything from this point
        for entry in &node.multi_wildcard_subscribers {
            subscribers.push(entry.clone());
        }

        if depth >= topic_segments.len() {
            // End of topic path - collect exact subscribers
            for entry in &node.exact_subscribers {
                subscribers.push(entry.clone());
            }
            return;
        }

        let current_segment = topic_segments[depth];

        // 1. Check exact match
        if let Some(child) = node.children.get(current_segment) {
            Self::collect_subscribers(child, topic_segments, depth + 1, subscribers);
        }

        // 2. Check single-level wildcard match
        if let Some(ref wildcard_child) = node.wildcard_child {
            if depth + 1 >= topic_segments.len() {
                // This is the last segment - collect single wildcard subscribers
                for entry in &wildcard_child.single_wildcard_subscribers {
                    subscribers.push(entry.clone());
                }
            } else {
                // Continue to next level
                Self::collect_subscribers(wildcard_child, topic_segments, depth + 1, subscribers);
            }
        }
    }
    #[allow(dead_code)]

    /// Check if a session is subscribed to a specific topic filter
    pub async fn is_subscribed(&self, session_ref: &SessionRef, topic_filter: &str) -> bool {
        let root = self.root.read().await;
        let segments: Vec<&str> = topic_filter.split('/').collect();

        Self::check_subscription(&root, &segments, session_ref, 0)
    }
    #[allow(dead_code)]

    /// Recursively check if a subscription exists
    fn check_subscription(
        node: &TrieNode,
        segments: &[&str],
        session_ref: &SessionRef,
        depth: usize,
    ) -> bool {
        if depth >= segments.len() {
            return node
                .exact_subscribers
                .iter()
                .any(|s| &s.session_ref == session_ref);
        }

        let segment = segments[depth];

        match segment {
            "#" => node
                .multi_wildcard_subscribers
                .iter()
                .any(|s| &s.session_ref == session_ref),
            "+" => {
                if let Some(ref wildcard_child) = node.wildcard_child {
                    if depth + 1 >= segments.len() {
                        wildcard_child
                            .single_wildcard_subscribers
                            .iter()
                            .any(|s| &s.session_ref == session_ref)
                    } else {
                        Self::check_subscription(wildcard_child, segments, session_ref, depth + 1)
                    }
                } else {
                    false
                }
            }
            _ => {
                if let Some(child) = node.children.get(segment) {
                    Self::check_subscription(child, segments, session_ref, depth + 1)
                } else {
                    false
                }
            }
        }
    }

    /// Get all topic filters for a session
    #[allow(dead_code)]
    pub async fn get_session_subscriptions(&self, session_ref: &SessionRef) -> Vec<String> {
        let root = self.root.read().await;
        let mut subscriptions = Vec::new();
        let mut current_path = Vec::new();

        Self::collect_session_subscriptions(
            &root,
            session_ref,
            &mut current_path,
            &mut subscriptions,
        );

        subscriptions
    }

    /// Recursively collect all subscriptions for a session
    #[allow(dead_code)]
    fn collect_session_subscriptions(
        node: &TrieNode,
        session_ref: &SessionRef,
        current_path: &mut Vec<String>,
        subscriptions: &mut Vec<String>,
    ) {
        // Check exact subscription at this level
        for entry in &node.exact_subscribers {
            if &entry.session_ref == session_ref {
                subscriptions.push(current_path.join("/"));
            }
        }

        // Check multi-level wildcard subscription
        for entry in &node.multi_wildcard_subscribers {
            if &entry.session_ref == session_ref {
                let mut path = current_path.clone();
                path.push("#".to_string());
                subscriptions.push(path.join("/"));
            }
        }

        // Check single-level wildcard subscription
        if let Some(ref wildcard_child) = node.wildcard_child {
            for entry in &wildcard_child.single_wildcard_subscribers {
                if &entry.session_ref == session_ref {
                    let mut path = current_path.clone();
                    path.push("+".to_string());
                    subscriptions.push(path.join("/"));
                }
            }

            // Recurse into wildcard child
            current_path.push("+".to_string());
            Self::collect_session_subscriptions(
                wildcard_child,
                session_ref,
                current_path,
                subscriptions,
            );
            current_path.pop();
        }

        // Recurse into exact children
        for (segment, child) in &node.children {
            current_path.push(segment.clone());
            Self::collect_session_subscriptions(child, session_ref, current_path, subscriptions);
            current_path.pop();
        }
    }

    /// Validate MQTT topic filter according to spec
    fn validate_topic_filter(topic_filter: &str) -> Result<(), SubscriptionError> {
        if topic_filter.is_empty() {
            return Err(SubscriptionError::InvalidTopicFilter);
        }

        let segments = topic_filter.split('/');
        let mut has_multi_wildcard = false;

        for (i, segment) in segments.enumerate() {
            // Check for multi-level wildcard
            if segment == "#" {
                if has_multi_wildcard {
                    return Err(SubscriptionError::InvalidTopicFilter);
                }
                has_multi_wildcard = true;

                // # must be the last segment
                if i != topic_filter.split('/').count() - 1 {
                    return Err(SubscriptionError::InvalidTopicFilter);
                }
            }
            // Check for single-level wildcard
            else if segment == "+" {
                // + is valid as a complete segment
            }
            // Check for invalid wildcard usage
            else if segment.contains('+') || segment.contains('#') {
                return Err(SubscriptionError::InvalidTopicFilter);
            }
        }

        Ok(())
    }
}

impl Default for SubscriptionStore {
    fn default() -> Self {
        Self::new()
    }
}
