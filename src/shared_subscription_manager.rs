// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// SPDX-License-Identifier: MIT

// Shared Subscription Manager with LRU-based round-robin
//
// This module implements share-name level round-robin using LRU (Least Recently Used) strategy.
// Each client tracks when it last received a message, and the client with the oldest timestamp
// (least recently used) is selected for the next message delivery.

use crate::session_store::SessionRef;
use mqtt_endpoint_tokio::mqtt_ep;
use std::collections::HashMap;

/// Details of a subscription
#[derive(Debug, Clone)]
pub struct SubscriptionDetails {
    pub qos: mqtt_ep::packet::Qos,
    pub topic_filter: String,
    pub sub_id: Option<u32>,
    pub rap: bool,
    pub nl: bool,
}

/// Entry for each client in a share group
#[derive(Debug, Clone)]
struct ClientEntry {
    session_ref: SessionRef,
    /// topic_filter -> subscription details
    subscriptions: HashMap<String, SubscriptionDetails>,
    /// Counter representing when this client last received a message
    /// Lower value = older = higher priority for next delivery
    last_delivery_counter: u64,
}

/// A group of clients sharing the same share name
#[derive(Debug)]
struct ShareGroup {
    /// session_id -> ClientEntry for fast lookup
    clients: HashMap<String, ClientEntry>,
}

impl ShareGroup {
    fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }
}

/// Shared Subscription Manager
///
/// Manages shared subscriptions with LRU-based round-robin at share-name level.
/// When a message needs to be delivered to a share group, it selects the client
/// with the smallest last_delivery_counter (least recently used) that has a
/// matching topic filter.
#[derive(Debug)]
pub struct SharedSubscriptionManager {
    /// share_name -> ShareGroup
    groups: HashMap<String, ShareGroup>,
    /// Global counter that increments with each delivery
    /// Used as a logical timestamp for LRU tracking
    global_counter: u64,
}

impl SharedSubscriptionManager {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            global_counter: 0,
        }
    }

    /// Insert a subscription for a client
    ///
    /// # Arguments
    /// * `share_name` - The share name
    /// * `topic_filter` - The topic filter (without $share/share_name/ prefix)
    /// * `session_ref` - Reference to the session
    /// * `details` - Subscription details (QoS, RAP, etc.)
    pub fn insert(
        &mut self,
        share_name: String,
        topic_filter: String,
        session_ref: SessionRef,
        details: SubscriptionDetails,
    ) {
        let group = self
            .groups
            .entry(share_name.clone())
            .or_insert_with(ShareGroup::new);

        let session_id = session_ref.session_id.to_string();

        tracing::debug!(
            "SharedSubscriptionManager::insert: share_name={share_name}, session_id={session_id}, topic_filter={topic_filter}"
        );

        if let Some(client_entry) = group.clients.get_mut(&session_id) {
            // Client already exists in this share group, add/update topic filter
            client_entry.subscriptions.insert(topic_filter, details);
        } else {
            // New client in this share group
            let mut subscriptions = HashMap::new();
            subscriptions.insert(topic_filter, details);

            group.clients.insert(
                session_id.clone(),
                ClientEntry {
                    session_ref,
                    subscriptions,
                    last_delivery_counter: 0, // Initialize to 0 (highest priority)
                },
            );
            tracing::debug!(
                "SharedSubscriptionManager::insert: Added new client {session_id} to share group"
            );
        }
    }

    /// Remove a specific subscription
    ///
    /// # Arguments
    /// * `share_name` - The share name
    /// * `topic_filter` - The topic filter to remove
    /// * `session_ref` - Reference to the session
    ///
    /// # Returns
    /// `true` if the subscription was found and removed, `false` otherwise
    pub fn erase(
        &mut self,
        share_name: &str,
        topic_filter: &str,
        session_ref: &SessionRef,
    ) -> bool {
        let Some(group) = self.groups.get_mut(share_name) else {
            return false;
        };

        let session_id = session_ref.session_id.to_string();
        let Some(client_entry) = group.clients.get_mut(&session_id) else {
            return false;
        };

        let removed = client_entry.subscriptions.remove(topic_filter).is_some();

        // Remove client if no more subscriptions
        if client_entry.subscriptions.is_empty() {
            group.clients.remove(&session_id);
        }

        // Remove group if no more clients
        if group.clients.is_empty() {
            self.groups.remove(share_name);
        }

        removed
    }

    /// Remove all subscriptions for a session across all share groups
    ///
    /// # Arguments
    /// * `session_ref` - Reference to the session
    pub fn erase_session(&mut self, session_ref: &SessionRef) {
        let session_id = session_ref.session_id.to_string();
        let mut groups_to_remove = Vec::new();

        for (share_name, group) in self.groups.iter_mut() {
            group.clients.remove(&session_id);
            if group.clients.is_empty() {
                groups_to_remove.push(share_name.clone());
            }
        }

        for share_name in groups_to_remove {
            self.groups.remove(&share_name);
        }
    }

    /// Get the target client for message delivery using LRU strategy
    ///
    /// # Arguments
    /// * `share_name` - The share name
    /// * `topic_filter` - The topic filter to match
    ///
    /// # Returns
    #[allow(dead_code)]
    /// `Some((session_ref, details))` if a matching client is found, `None` otherwise
    pub fn get_target(
        &mut self,
        share_name: &str,
        topic_filter: &str,
    ) -> Option<(SessionRef, SubscriptionDetails)> {
        self.get_target_with_filter(share_name, topic_filter, |_| true)
    }

    /// Get target with client filter function
    ///
    /// # Arguments
    /// * `share_name` - The share name
    /// * `topic_filter` - The topic filter
    /// * `client_filter` - Function to filter eligible clients (takes SessionRef)
    ///
    /// # Returns
    /// The selected client's session ref and subscription details
    pub fn get_target_with_filter<F>(
        &mut self,
        share_name: &str,
        topic_filter: &str,
        client_filter: F,
    ) -> Option<(SessionRef, SubscriptionDetails)>
    where
        F: Fn(&SessionRef) -> bool,
    {
        let group = self.groups.get_mut(share_name)?;

        if group.clients.is_empty() {
            return None;
        }

        // Find the client with the smallest last_delivery_counter that has this topic_filter
        // If multiple clients have the same counter, use lexicographic order of client_id for determinism
        let mut best_client_id: Option<String> = None;
        let mut best_counter = u64::MAX;
        let mut best_details: Option<SubscriptionDetails> = None;

        // Collect all matching clients first, then sort for deterministic selection
        let mut candidates: Vec<(&String, &ClientEntry)> = group
            .clients
            .iter()
            .filter(|(_id, entry)| {
                entry.subscriptions.contains_key(topic_filter) && client_filter(&entry.session_ref)
            })
            .collect();

        // Sort by (counter, client_id) for deterministic selection
        candidates.sort_by(|a, b| {
            a.1.last_delivery_counter
                .cmp(&b.1.last_delivery_counter)
                .then_with(|| a.0.cmp(b.0))
        });

        // Select the first candidate (lowest counter, lexicographically first if tied)
        if let Some((client_id, client_entry)) = candidates.first() {
            best_client_id = Some((*client_id).clone());
            best_counter = client_entry.last_delivery_counter;
            if let Some(details) = client_entry.subscriptions.get(topic_filter) {
                best_details = Some(details.clone());
            }
        }

        if let Some(client_id) = best_client_id {
            tracing::debug!(
                "SharedSubscriptionManager::get_target: share_name={share_name}, topic_filter={topic_filter}, selected client_id={client_id}, counter before={best_counter}, global_counter={}",
                self.global_counter
            );
            // Update the counter for the selected client
            self.global_counter += 1;
            if let Some(client_entry) = group.clients.get_mut(&client_id) {
                client_entry.last_delivery_counter = self.global_counter;
                tracing::debug!(
                    "SharedSubscriptionManager::get_target: Updated client {client_id} counter to {}",
                    self.global_counter
                );
                return Some((client_entry.session_ref.clone(), best_details.unwrap()));
            }
        }

        None
    }

    /// Find targets for a topic across all share groups
    ///
    /// # Arguments
    /// * `topic_segments` - The topic segments to match against
    /// * `topic_matches_filter` - Function to check if topic matches a filter
    ///
    /// # Returns
    /// Vector of (share_name, session_ref, details) tuples for all matching share groups
    pub fn find_all_targets<F>(
        &mut self,
        topic_segments: &[&str],
        topic_matches_filter: F,
    ) -> Vec<(String, SessionRef, SubscriptionDetails)>
    where
        F: Fn(&[&str], &[&str]) -> bool,
    {
        self.find_all_targets_with_filter(topic_segments, topic_matches_filter, |_| true)
    }

    /// Find targets for a topic across all share groups with client filter
    ///
    /// # Arguments
    /// * `topic_segments` - The topic segments to match against
    /// * `topic_matches_filter` - Function to check if topic matches a filter
    /// * `client_filter` - Function to filter eligible clients (takes SessionRef)
    ///
    /// # Returns
    /// Vector of (share_name, session_ref, details) tuples for all matching share groups
    pub fn find_all_targets_with_filter<F, G>(
        &mut self,
        topic_segments: &[&str],
        topic_matches_filter: F,
        client_filter: G,
    ) -> Vec<(String, SessionRef, SubscriptionDetails)>
    where
        F: Fn(&[&str], &[&str]) -> bool,
        G: Fn(&SessionRef) -> bool,
    {
        let mut results = Vec::new();

        // First pass: collect all (share_name, matching_filter) pairs
        // We need to collect all unique topic filters in each share group that match
        let mut share_filter_pairs: Vec<(String, String)> = Vec::new();

        for (share_name, group) in &self.groups {
            // Collect all unique matching topic filters in this share group
            let mut seen_filters = std::collections::HashSet::new();

            for client_entry in group.clients.values() {
                for topic_filter in client_entry.subscriptions.keys() {
                    // Skip if we've already seen this filter in this share group
                    if seen_filters.contains(topic_filter) {
                        continue;
                    }

                    let filter_segments: Vec<&str> = topic_filter.split('/').collect();
                    if topic_matches_filter(topic_segments, &filter_segments) {
                        share_filter_pairs.push((share_name.clone(), topic_filter.clone()));
                        seen_filters.insert(topic_filter.clone());
                    }
                }
            }
        }

        // Second pass: get target for each (share_name, filter) pair
        // Important: Only one match per share group (stop after first successful get_target)
        let mut processed_shares = std::collections::HashSet::new();

        for (share_name, filter) in share_filter_pairs {
            // Skip if we've already processed this share group
            if processed_shares.contains(&share_name) {
                continue;
            }

            if let Some((session_ref, details)) =
                self.get_target_with_filter(&share_name, &filter, &client_filter)
            {
                results.push((share_name.clone(), session_ref, details));
                processed_shares.insert(share_name);
            }
        }

        results
    }
}

impl Default for SharedSubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_store::SessionId;

    fn create_session_ref(client_id: &str) -> SessionRef {
        SessionRef {
            session_id: SessionId {
                user_name: None,
                client_id: client_id.to_string(),
            },
        }
    }

    fn create_details(qos: u8) -> SubscriptionDetails {
        SubscriptionDetails {
            qos: match qos {
                0 => mqtt_ep::packet::Qos::AtMostOnce,
                1 => mqtt_ep::packet::Qos::AtLeastOnce,
                2 => mqtt_ep::packet::Qos::ExactlyOnce,
                _ => panic!("Invalid QoS"),
            },
            topic_filter: String::new(),
            sub_id: None,
            rap: false,
            nl: false,
        }
    }

    #[test]
    fn test_basic_lru_round_robin() {
        let mut mgr = SharedSubscriptionManager::new();

        // Client1 and Client2 subscribe to t1
        mgr.insert(
            "sn1".to_string(),
            "t1".to_string(),
            create_session_ref("client1"),
            create_details(0),
        );
        mgr.insert(
            "sn1".to_string(),
            "t1".to_string(),
            create_session_ref("client2"),
            create_details(0),
        );

        // Should alternate between client1 and client2 based on LRU
        let (session_ref, _) = mgr.get_target("sn1", "t1").unwrap();
        assert_eq!(session_ref.session_id.client_id, "client1");

        let (session_ref, _) = mgr.get_target("sn1", "t1").unwrap();
        assert_eq!(session_ref.session_id.client_id, "client2");

        let (session_ref, _) = mgr.get_target("sn1", "t1").unwrap();
        assert_eq!(session_ref.session_id.client_id, "client1");

        let (session_ref, _) = mgr.get_target("sn1", "t1").unwrap();
        assert_eq!(session_ref.session_id.client_id, "client2");
    }

    #[test]
    fn test_share_name_level_lru() {
        let mut mgr = SharedSubscriptionManager::new();

        // Subscribe order (same as GitHub issue):
        // c1: t1, t2
        // c2: t2
        // c3: t1, t2
        mgr.insert(
            "sn1".to_string(),
            "t1".to_string(),
            create_session_ref("c1"),
            create_details(0),
        );
        mgr.insert(
            "sn1".to_string(),
            "t2".to_string(),
            create_session_ref("c1"),
            create_details(0),
        );
        mgr.insert(
            "sn1".to_string(),
            "t2".to_string(),
            create_session_ref("c2"),
            create_details(0),
        );
        mgr.insert(
            "sn1".to_string(),
            "t1".to_string(),
            create_session_ref("c3"),
            create_details(0),
        );
        mgr.insert(
            "sn1".to_string(),
            "t2".to_string(),
            create_session_ref("c3"),
            create_details(0),
        );

        // Publish sequence: t1, t2, t1, t2, t1, t2, t1, t2, t1, t2, t1
        // Expected according to GitHub issue:
        // msg1(t1) -> c1, msg2(t2) -> c2, msg3(t1) -> c3, msg4(t2) -> c1
        // msg5(t1) -> c3, msg6(t2) -> c2, msg7(t1) -> c1, msg8(t2) -> c3
        // msg9(t1) -> c1, msg10(t2) -> c2, msg11(t1) -> c3

        let deliveries = vec![
            ("t1", "c1"),
            ("t2", "c2"),
            ("t1", "c3"),
            ("t2", "c1"),
            ("t1", "c3"),
            ("t2", "c2"),
            ("t1", "c1"),
            ("t2", "c3"),
            ("t1", "c1"),
            ("t2", "c2"),
            ("t1", "c3"),
        ];

        for (i, (topic, expected_client)) in deliveries.iter().enumerate() {
            let (session_ref, _) = mgr
                .get_target("sn1", topic)
                .unwrap_or_else(|| panic!("No target for msg{} ({})", i + 1, topic));
            assert_eq!(
                session_ref.session_id.client_id,
                *expected_client,
                "msg{} ({}) should go to {} but went to {}",
                i + 1,
                topic,
                expected_client,
                session_ref.session_id.client_id
            );
        }
    }

    #[test]
    fn test_erase() {
        let mut mgr = SharedSubscriptionManager::new();

        let c1_ref = create_session_ref("client1");
        let c2_ref = create_session_ref("client2");

        mgr.insert(
            "sn1".to_string(),
            "t1".to_string(),
            c1_ref.clone(),
            create_details(0),
        );
        mgr.insert(
            "sn1".to_string(),
            "t1".to_string(),
            c2_ref.clone(),
            create_details(0),
        );

        let (session_ref, _) = mgr.get_target("sn1", "t1").unwrap();
        assert_eq!(session_ref.session_id.client_id, "client1");

        // Unsubscribe client2
        assert!(mgr.erase("sn1", "t1", &c2_ref));

        // Now only client1 should receive messages
        let (session_ref, _) = mgr.get_target("sn1", "t1").unwrap();
        assert_eq!(session_ref.session_id.client_id, "client1");

        let (session_ref, _) = mgr.get_target("sn1", "t1").unwrap();
        assert_eq!(session_ref.session_id.client_id, "client1");
    }
}
