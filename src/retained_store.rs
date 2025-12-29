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
use mqtt_endpoint_tokio::mqtt_ep::prelude::PropertyValueAccess;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

/// Retained message information
#[derive(Debug, Clone)]
pub struct RetainedMessage {
    pub topic_name: String,
    pub qos: mqtt_ep::packet::Qos,
    pub payload: mqtt_ep::common::ArcPayload,
    pub props: Vec<mqtt_ep::packet::Property>,
    pub stored_at: std::time::Instant, // Time when message was stored
}

/// Retained message entry with Arc and timer management
#[derive(Debug)]
struct RetainedMessageEntry {
    message: Arc<RetainedMessage>,
    expiry_timer: Option<tokio::task::JoinHandle<()>>,
}

/// Trie node for retained messages (simpler than subscription trie)
#[derive(Debug, Default)]
struct RetainedTrieNode {
    /// Retained message at this exact topic name (if any)
    message: Option<RetainedMessageEntry>,
    /// Child nodes for each segment
    children: HashMap<String, RetainedTrieNode>,
}

/// Retained message store using Trie-based structure for efficient wildcard matching
#[derive(Debug)]
pub struct RetainedStore {
    /// Root of the trie structure
    root: Arc<RwLock<RetainedTrieNode>>,
}

impl RetainedStore {
    /// Create a new retained message store
    pub fn new() -> Self {
        Self {
            root: Arc::new(RwLock::new(RetainedTrieNode::default())),
        }
    }

    /// Store a retained message for a topic name
    /// If a message already exists for this topic, it will be replaced
    pub async fn store(
        &self,
        topic_name: &str,
        qos: mqtt_ep::packet::Qos,
        payload: mqtt_ep::common::ArcPayload,
        props: Vec<mqtt_ep::packet::Property>,
    ) {
        // Extract MessageExpiryInterval from props
        let message_expiry_interval = props.iter().find_map(|prop| {
            if let mqtt_ep::packet::Property::MessageExpiryInterval(_) = prop {
                prop.as_u32()
            } else {
                None
            }
        });

        // Create retained message
        let message = RetainedMessage {
            topic_name: topic_name.to_string(),
            qos,
            payload,
            props,
            stored_at: std::time::Instant::now(),
        };

        let message_arc = Arc::new(message);

        // Spawn expiry timer if MessageExpiryInterval is set
        let expiry_timer = if let Some(expiry_interval) = message_expiry_interval {
            let root_weak = Arc::downgrade(&self.root);
            let message_weak = Arc::downgrade(&message_arc);
            let topic_name_owned = topic_name.to_string();

            Some(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(expiry_interval as u64))
                    .await;

                trace!(
                    "MessageExpiryInterval timer fired for retained message, topic={topic_name_owned}"
                );

                // Remove retained message
                if let Some(root_arc) = root_weak.upgrade() {
                    if message_weak.upgrade().is_some() {
                        let mut root_guard = root_arc.write().await;
                        let segments: Vec<&str> = topic_name_owned.split('/').collect();

                        if let Some(node) = Self::find_node_mut(&mut root_guard, &segments, 0) {
                            node.message = None;
                            trace!("Removed expired retained message for topic '{topic_name_owned}'");
                        }
                    }
                }
            }))
        } else {
            None
        };

        // Store in trie
        let mut root = self.root.write().await;
        let segments: Vec<&str> = topic_name.split('/').collect();

        let node = Self::get_or_create_node(&mut root, &segments, 0);

        // Cancel existing timer if any
        if let Some(mut old_entry) = node.message.take() {
            if let Some(timer) = old_entry.expiry_timer.take() {
                timer.abort();
            }
        }

        node.message = Some(RetainedMessageEntry {
            message: message_arc,
            expiry_timer,
        });

        trace!("Stored retained message for topic '{topic_name}' with QoS {qos:?}");
    }

    /// Remove a retained message for a topic name
    pub async fn remove(&self, topic_name: &str) {
        let mut root = self.root.write().await;
        let segments: Vec<&str> = topic_name.split('/').collect();

        if let Some(node) = Self::find_node_mut(&mut root, &segments, 0) {
            // Cancel timer if any
            if let Some(mut old_entry) = node.message.take() {
                if let Some(timer) = old_entry.expiry_timer.take() {
                    timer.abort();
                }
            }
            trace!("Removed retained message for topic '{topic_name}'");
        }
    }

    /// Get all retained messages that match a topic filter (with wildcards)
    pub async fn get_matching(&self, topic_filter: &str) -> Vec<RetainedMessage> {
        let root = self.root.read().await;
        let segments: Vec<&str> = topic_filter.split('/').collect();

        Self::search_matching(&root, &segments, 0)
    }

    /// Recursively get or create a node at the given path
    fn get_or_create_node<'a>(
        node: &'a mut RetainedTrieNode,
        segments: &[&str],
        depth: usize,
    ) -> &'a mut RetainedTrieNode {
        if depth >= segments.len() {
            return node;
        }

        let segment = segments[depth];
        let child = node
            .children
            .entry(segment.to_string())
            .or_insert_with(RetainedTrieNode::default);

        Self::get_or_create_node(child, segments, depth + 1)
    }

    /// Recursively find a node at the given path (mutable)
    fn find_node_mut<'a>(
        node: &'a mut RetainedTrieNode,
        segments: &[&str],
        depth: usize,
    ) -> Option<&'a mut RetainedTrieNode> {
        if depth >= segments.len() {
            return Some(node);
        }

        let segment = segments[depth];
        if let Some(child) = node.children.get_mut(segment) {
            Self::find_node_mut(child, segments, depth + 1)
        } else {
            None
        }
    }

    /// Recursively search for retained messages matching a topic filter
    fn search_matching(
        node: &RetainedTrieNode,
        filter_segments: &[&str],
        depth: usize,
    ) -> Vec<RetainedMessage> {
        let mut results = Vec::new();

        if depth >= filter_segments.len() {
            // Filter exhausted - collect all messages under this node
            Self::collect_all_messages(node, &mut results);
            return results;
        }

        let segment = filter_segments[depth];

        match segment {
            "#" => {
                // Multi-level wildcard - collect everything from this node down
                Self::collect_all_messages(node, &mut results);
            }
            "+" => {
                // Single-level wildcard - search all children at next depth
                for child in node.children.values() {
                    results.extend(Self::search_matching(child, filter_segments, depth + 1));
                }
            }
            _ => {
                // Exact match - search only matching child
                if let Some(child) = node.children.get(segment) {
                    results.extend(Self::search_matching(child, filter_segments, depth + 1));
                }
            }
        }

        results
    }

    /// Recursively collect all retained messages under a node
    fn collect_all_messages(node: &RetainedTrieNode, results: &mut Vec<RetainedMessage>) {
        // Add message at this node if present
        if let Some(ref entry) = node.message {
            results.push((*entry.message).clone());
        }

        // Recursively collect from all children
        for child in node.children.values() {
            Self::collect_all_messages(child, results);
        }
    }
}
