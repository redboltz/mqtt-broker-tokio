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

pub type ClientId = String;

/// Subscription information containing endpoint, QoS, topic filter, subscription ID, and RAP flag
#[derive(Debug, Clone)]
pub struct Subscription {
    pub endpoint: EndpointRef,
    pub qos: mqtt_ep::packet::Qos,
    pub topic_filter: String,
    pub sub_id: Option<u32>,
    pub rap: bool, // Retain As Published flag
}

/// Wrapper for Arc<Endpoint> that uses pointer-based comparison and hashing
#[derive(Clone)]
pub struct EndpointRef(Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>);

impl EndpointRef {
    pub fn new(endpoint: Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>>) -> Self {
        Self(endpoint)
    }

    pub fn endpoint(&self) -> &Arc<mqtt_ep::Endpoint<mqtt_ep::role::Server>> {
        &self.0
    }

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

/// Subscription entry in trie node
#[derive(Debug, Clone)]
struct SubscriptionEntry {
    pub endpoint: EndpointRef,
    pub qos: mqtt_ep::packet::Qos,
    pub topic_filter: String,
    pub sub_id: Option<u32>,
    pub rap: bool,
}

/// Trie node containing subscription information
#[derive(Debug, Clone, Default)]
struct TrieNode {
    /// Subscriptions to this exact path
    exact_subscribers: Vec<SubscriptionEntry>,
    /// Subscriptions with single-level wildcard at this position
    single_wildcard_subscribers: Vec<SubscriptionEntry>,
    /// Subscriptions with multi-level wildcard from this position
    multi_wildcard_subscribers: Vec<SubscriptionEntry>,
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
}

impl SubscriptionStore {
    /// Create a new subscription store
    pub fn new() -> Self {
        Self {
            root: Arc::new(RwLock::new(TrieNode::default())),
        }
    }

    /// Add a subscription for an endpoint to a topic filter
    /// Returns Ok(is_new) where is_new is true if this is a new subscription, false if updating existing
    pub async fn subscribe(
        &self,
        endpoint: EndpointRef,
        topic_filter: &str,
        qos: mqtt_ep::packet::Qos,
        sub_id: Option<u32>,
        rap: bool,
    ) -> Result<bool, SubscriptionError> {
        Self::validate_topic_filter(topic_filter)?;

        let mut root = self.root.write().await;
        let segments: Vec<&str> = topic_filter.split('/').collect();

        let is_new = Self::insert_subscription(
            &mut root,
            &segments,
            endpoint.clone(),
            topic_filter,
            qos,
            sub_id,
            rap,
            0,
        );

        trace!(
            "Subscribed endpoint to topic filter '{topic_filter}' with QoS {qos:?}, sub_id {sub_id:?}, is_new: {is_new}"
        );
        Ok(is_new)
    }

    /// Recursively insert subscription into trie
    /// Returns true if this is a new subscription, false if updating existing
    fn insert_subscription(
        node: &mut TrieNode,
        segments: &[&str],
        endpoint: EndpointRef,
        topic_filter: &str,
        qos: mqtt_ep::packet::Qos,
        sub_id: Option<u32>,
        rap: bool,
        depth: usize,
    ) -> bool {
        if depth >= segments.len() {
            // End of path - add to exact subscribers
            return Self::upsert_subscription(
                &mut node.exact_subscribers,
                endpoint,
                topic_filter,
                qos,
                sub_id,
                rap,
            );
        }

        let segment = segments[depth];

        match segment {
            "#" => {
                // Multi-level wildcard - matches everything from this point
                Self::upsert_subscription(
                    &mut node.multi_wildcard_subscribers,
                    endpoint,
                    topic_filter,
                    qos,
                    sub_id,
                    rap,
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
                            endpoint,
                            topic_filter,
                            qos,
                            sub_id,
                            rap,
                        )
                    } else {
                        Self::insert_subscription(
                            wildcard_child,
                            segments,
                            endpoint,
                            topic_filter,
                            qos,
                            sub_id,
                            rap,
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
                    endpoint,
                    topic_filter,
                    qos,
                    sub_id,
                    rap,
                    depth + 1,
                )
            }
        }
    }

    /// Insert or update subscription entry (overwrite if identical topic_filter)
    /// Returns true if this is a new subscription, false if updating existing
    fn upsert_subscription(
        subscribers: &mut Vec<SubscriptionEntry>,
        endpoint: EndpointRef,
        topic_filter: &str,
        qos: mqtt_ep::packet::Qos,
        sub_id: Option<u32>,
        rap: bool,
    ) -> bool {
        // Find existing subscription with same endpoint and topic_filter
        if let Some(existing) = subscribers
            .iter_mut()
            .find(|s| s.endpoint == endpoint && s.topic_filter == topic_filter)
        {
            // Update existing subscription (QoS, sub_id, and rap overwrite)
            existing.qos = qos;
            existing.sub_id = sub_id;
            existing.rap = rap;
            false // Not a new subscription
        } else {
            // Add new subscription
            subscribers.push(SubscriptionEntry {
                endpoint,
                qos,
                topic_filter: topic_filter.to_string(),
                sub_id,
                rap,
            });
            true // New subscription
        }
    }

    /// Remove a subscription for an endpoint from a topic filter
    pub async fn unsubscribe(
        &self,
        endpoint: &EndpointRef,
        topic_filter: &str,
    ) -> Result<bool, SubscriptionError> {
        Self::validate_topic_filter(topic_filter)?;

        let mut root = self.root.write().await;
        let segments: Vec<&str> = topic_filter.split('/').collect();

        let removed = Self::remove_subscription(&mut root, &segments, endpoint, 0);
        Ok(removed)
    }

    /// Recursively remove subscription from trie
    fn remove_subscription(
        node: &mut TrieNode,
        segments: &[&str],
        endpoint: &EndpointRef,
        depth: usize,
    ) -> bool {
        if depth >= segments.len() {
            return Self::remove_from_vec(&mut node.exact_subscribers, endpoint);
        }

        let segment = segments[depth];

        match segment {
            "#" => Self::remove_from_vec(&mut node.multi_wildcard_subscribers, endpoint),
            "+" => {
                if let Some(ref mut wildcard_child) = node.wildcard_child {
                    if depth + 1 >= segments.len() {
                        Self::remove_from_vec(
                            &mut wildcard_child.single_wildcard_subscribers,
                            endpoint,
                        )
                    } else {
                        Self::remove_subscription(wildcard_child, segments, endpoint, depth + 1)
                    }
                } else {
                    false
                }
            }
            _ => {
                if let Some(child) = node.children.get_mut(segment) {
                    Self::remove_subscription(child, segments, endpoint, depth + 1)
                } else {
                    false
                }
            }
        }
    }

    /// Remove endpoint from subscription vector
    fn remove_from_vec(subscribers: &mut Vec<SubscriptionEntry>, endpoint: &EndpointRef) -> bool {
        if let Some(pos) = subscribers.iter().position(|s| &s.endpoint == endpoint) {
            subscribers.remove(pos);
            true
        } else {
            false
        }
    }

    /// Remove all subscriptions for an endpoint
    pub async fn unsubscribe_all(&self, endpoint: &EndpointRef) {
        let mut root = self.root.write().await;
        Self::remove_all_subscriptions(&mut root, endpoint);
    }

    /// Recursively remove all subscriptions for an endpoint
    fn remove_all_subscriptions(node: &mut TrieNode, endpoint: &EndpointRef) {
        node.exact_subscribers.retain(|s| &s.endpoint != endpoint);
        node.single_wildcard_subscribers
            .retain(|s| &s.endpoint != endpoint);
        node.multi_wildcard_subscribers
            .retain(|s| &s.endpoint != endpoint);

        // Recursively clean children
        for child in node.children.values_mut() {
            Self::remove_all_subscriptions(child, endpoint);
        }

        if let Some(ref mut wildcard_child) = node.wildcard_child {
            Self::remove_all_subscriptions(wildcard_child, endpoint);
        }
    }

    /// Find all subscriber endpoints for a given published topic
    pub async fn find_subscribers(&self, topic: &str) -> Vec<Subscription> {
        let root = self.root.read().await;
        let mut all_subscribers = Vec::new();
        let segments: Vec<&str> = topic.split('/').collect();

        // trace!("Looking for subscribers to topic '{}'", topic);

        Self::collect_subscribers(&root, &segments, 0, &mut all_subscribers);

        let result: Vec<Subscription> = all_subscribers
            .into_iter()
            .map(|entry| Subscription {
                endpoint: entry.endpoint,
                qos: entry.qos,
                topic_filter: entry.topic_filter,
                sub_id: entry.sub_id,
                rap: entry.rap,
            })
            .collect();
        // trace!("Final subscriber list for '{}': {} subscriptions", topic, result.len());
        result
    }

    /// Recursively collect all matching subscribers
    fn collect_subscribers(
        node: &TrieNode,
        topic_segments: &[&str],
        depth: usize,
        subscribers: &mut Vec<SubscriptionEntry>,
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

    /// Check if an endpoint is subscribed to a specific topic filter
    pub async fn is_subscribed(&self, endpoint: &EndpointRef, topic_filter: &str) -> bool {
        let root = self.root.read().await;
        let segments: Vec<&str> = topic_filter.split('/').collect();

        Self::check_subscription(&root, &segments, endpoint, 0)
    }

    /// Recursively check if a subscription exists
    fn check_subscription(
        node: &TrieNode,
        segments: &[&str],
        endpoint: &EndpointRef,
        depth: usize,
    ) -> bool {
        if depth >= segments.len() {
            return node
                .exact_subscribers
                .iter()
                .any(|s| &s.endpoint == endpoint);
        }

        let segment = segments[depth];

        match segment {
            "#" => node
                .multi_wildcard_subscribers
                .iter()
                .any(|s| &s.endpoint == endpoint),
            "+" => {
                if let Some(ref wildcard_child) = node.wildcard_child {
                    if depth + 1 >= segments.len() {
                        wildcard_child
                            .single_wildcard_subscribers
                            .iter()
                            .any(|s| &s.endpoint == endpoint)
                    } else {
                        Self::check_subscription(wildcard_child, segments, endpoint, depth + 1)
                    }
                } else {
                    false
                }
            }
            _ => {
                if let Some(child) = node.children.get(segment) {
                    Self::check_subscription(child, segments, endpoint, depth + 1)
                } else {
                    false
                }
            }
        }
    }

    /// Get all topic filters for an endpoint
    pub async fn get_endpoint_subscriptions(&self, endpoint: &EndpointRef) -> Vec<String> {
        let root = self.root.read().await;
        let mut subscriptions = Vec::new();
        let mut current_path = Vec::new();

        Self::collect_endpoint_subscriptions(
            &root,
            endpoint,
            &mut current_path,
            &mut subscriptions,
        );

        subscriptions
    }

    /// Recursively collect all subscriptions for an endpoint
    fn collect_endpoint_subscriptions(
        node: &TrieNode,
        endpoint: &EndpointRef,
        current_path: &mut Vec<String>,
        subscriptions: &mut Vec<String>,
    ) {
        // Check exact subscription at this level
        for entry in &node.exact_subscribers {
            if &entry.endpoint == endpoint {
                subscriptions.push(current_path.join("/"));
            }
        }

        // Check multi-level wildcard subscription
        for entry in &node.multi_wildcard_subscribers {
            if &entry.endpoint == endpoint {
                let mut path = current_path.clone();
                path.push("#".to_string());
                subscriptions.push(path.join("/"));
            }
        }

        // Check single-level wildcard subscription
        if let Some(ref wildcard_child) = node.wildcard_child {
            for entry in &wildcard_child.single_wildcard_subscribers {
                if &entry.endpoint == endpoint {
                    let mut path = current_path.clone();
                    path.push("+".to_string());
                    subscriptions.push(path.join("/"));
                }
            }

            // Recurse into wildcard child
            current_path.push("+".to_string());
            Self::collect_endpoint_subscriptions(
                wildcard_child,
                endpoint,
                current_path,
                subscriptions,
            );
            current_path.pop();
        }

        // Recurse into exact children
        for (segment, child) in &node.children {
            current_path.push(segment.clone());
            Self::collect_endpoint_subscriptions(child, endpoint, current_path, subscriptions);
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

