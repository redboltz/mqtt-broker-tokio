// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// SPDX-License-Identifier: MIT

//! Unit tests for shared subscription LRU-based round-robin logic

use std::collections::HashMap;

/// Session ID
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionId {
    username: Option<String>,
    client_id: String,
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref username) = self.username {
            write!(f, "{}:{}", username, self.client_id)
        } else {
            write!(f, "{}", self.client_id)
        }
    }
}

/// Session Reference
#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionRef {
    session_id: SessionId,
}

/// Subscription details
#[derive(Debug, Clone)]
struct SubscriptionDetails {
    qos: u8,
    topic_filter: String,
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

/// Shared Subscription Manager with LRU-based round-robin
#[derive(Debug)]
struct SharedSubscriptionManager {
    /// share_name -> ShareGroup
    groups: HashMap<String, ShareGroup>,
    /// Global counter that increments with each delivery
    global_counter: u64,
}

impl SharedSubscriptionManager {
    fn new() -> Self {
        Self {
            groups: HashMap::new(),
            global_counter: 0,
        }
    }

    /// Insert a subscription
    fn subscribe(&mut self, share_name: String, topic_filter: String, client_id: String, qos: u8) {
        let group = self
            .groups
            .entry(share_name)
            .or_insert_with(ShareGroup::new);

        let session_ref = SessionRef {
            session_id: SessionId {
                username: None,
                client_id: client_id.clone(),
            },
        };
        let session_id_str = session_ref.session_id.to_string();

        if let Some(client_entry) = group.clients.get_mut(&session_id_str) {
            // Client already exists in this share group
            client_entry.subscriptions.insert(
                topic_filter.clone(),
                SubscriptionDetails { qos, topic_filter },
            );
        } else {
            // New client in this share group
            let mut subscriptions = HashMap::new();
            subscriptions.insert(
                topic_filter.clone(),
                SubscriptionDetails { qos, topic_filter },
            );

            group.clients.insert(
                session_id_str,
                ClientEntry {
                    session_ref,
                    subscriptions,
                    last_delivery_counter: 0, // Initialize to 0 (highest priority)
                },
            );
        }
    }

    /// Remove a subscription
    fn unsubscribe(&mut self, share_name: &str, topic_filter: &str, client_id: &str) -> bool {
        let Some(group) = self.groups.get_mut(share_name) else {
            return false;
        };

        let Some(client_entry) = group.clients.get_mut(client_id) else {
            return false;
        };

        let removed = client_entry.subscriptions.remove(topic_filter).is_some();

        // Remove client if no more subscriptions
        if client_entry.subscriptions.is_empty() {
            group.clients.remove(client_id);
        }

        // Remove group if no more clients
        if group.clients.is_empty() {
            self.groups.remove(share_name);
        }

        removed
    }

    /// Get next target using LRU strategy
    fn get_target(&mut self, share_name: &str, topic_filter: &str) -> Option<String> {
        let group = self.groups.get_mut(share_name)?;

        if group.clients.is_empty() {
            return None;
        }

        // Find the client with the smallest last_delivery_counter that has this topic_filter
        // If multiple clients have the same counter, use lexicographic order of client_id for determinism
        let mut best_client_id: Option<String> = None;
        let mut best_counter = u64::MAX;

        for (client_id, client_entry) in &group.clients {
            if client_entry.subscriptions.contains_key(topic_filter) {
                // Select if:
                // 1. Counter is smaller, OR
                // 2. Counter is equal and (no best yet OR this client_id is lexicographically smaller)
                let should_select = match client_entry.last_delivery_counter.cmp(&best_counter) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Equal => match &best_client_id {
                        None => true,
                        Some(best_id) => client_id < best_id,
                    },
                    std::cmp::Ordering::Greater => false,
                };

                if should_select {
                    best_counter = client_entry.last_delivery_counter;
                    best_client_id = Some(client_id.clone());
                }
            }
        }

        if let Some(client_id) = best_client_id {
            // Update the counter for the selected client
            self.global_counter += 1;
            if let Some(client_entry) = group.clients.get_mut(&client_id) {
                client_entry.last_delivery_counter = self.global_counter;
                return Some(client_entry.session_ref.session_id.client_id.clone());
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_lru_round_robin() {
        let mut mgr = SharedSubscriptionManager::new();

        // Client1 and Client2 subscribe to t1
        mgr.subscribe(
            "sn1".to_string(),
            "t1".to_string(),
            "client1".to_string(),
            0,
        );
        mgr.subscribe(
            "sn1".to_string(),
            "t1".to_string(),
            "client2".to_string(),
            0,
        );

        // Should alternate between client1 and client2 based on LRU
        assert_eq!(mgr.get_target("sn1", "t1").unwrap(), "client1");
        assert_eq!(mgr.get_target("sn1", "t1").unwrap(), "client2");
        assert_eq!(mgr.get_target("sn1", "t1").unwrap(), "client1");
        assert_eq!(mgr.get_target("sn1", "t1").unwrap(), "client2");
    }

    #[test]
    fn test_share_name_level_lru() {
        let mut mgr = SharedSubscriptionManager::new();

        // Subscribe order (as corrected):
        // c1: t1, t2
        // c2: t2
        // c3: t1, t2
        mgr.subscribe("sn1".to_string(), "t1".to_string(), "c1".to_string(), 0);
        mgr.subscribe("sn1".to_string(), "t2".to_string(), "c1".to_string(), 0);
        mgr.subscribe("sn1".to_string(), "t2".to_string(), "c2".to_string(), 0);
        mgr.subscribe("sn1".to_string(), "t1".to_string(), "c3".to_string(), 0);
        mgr.subscribe("sn1".to_string(), "t2".to_string(), "c3".to_string(), 0);

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
            let actual_client = mgr
                .get_target("sn1", topic)
                .unwrap_or_else(|| panic!("No target for msg{} ({})", i + 1, topic));
            assert_eq!(
                actual_client,
                *expected_client,
                "msg{} ({}) should go to {} but went to {}",
                i + 1,
                topic,
                expected_client,
                actual_client
            );
        }
    }

    #[test]
    fn test_different_share_names_independent() {
        let mut mgr = SharedSubscriptionManager::new();

        // Two different share names with same topic filter
        mgr.subscribe(
            "sn1".to_string(),
            "t1".to_string(),
            "client1".to_string(),
            0,
        );
        mgr.subscribe(
            "sn1".to_string(),
            "t1".to_string(),
            "client2".to_string(),
            0,
        );
        mgr.subscribe(
            "sn2".to_string(),
            "t1".to_string(),
            "client3".to_string(),
            0,
        );
        mgr.subscribe(
            "sn2".to_string(),
            "t1".to_string(),
            "client4".to_string(),
            0,
        );

        // Each share name should have independent LRU
        let sn1_first = mgr.get_target("sn1", "t1").unwrap();
        let sn2_first = mgr.get_target("sn2", "t1").unwrap();
        let sn1_second = mgr.get_target("sn1", "t1").unwrap();
        let sn2_second = mgr.get_target("sn2", "t1").unwrap();

        // Verify different clients were selected
        assert_ne!(sn1_first, sn1_second);
        assert_ne!(sn2_first, sn2_second);

        // Verify clients are from the correct share groups
        assert!(sn1_first == "client1" || sn1_first == "client2");
        assert!(sn1_second == "client1" || sn1_second == "client2");
        assert!(sn2_first == "client3" || sn2_first == "client4");
        assert!(sn2_second == "client3" || sn2_second == "client4");
    }

    #[test]
    fn test_unsubscribe() {
        let mut mgr = SharedSubscriptionManager::new();

        mgr.subscribe(
            "sn1".to_string(),
            "t1".to_string(),
            "client1".to_string(),
            0,
        );
        mgr.subscribe(
            "sn1".to_string(),
            "t1".to_string(),
            "client2".to_string(),
            0,
        );

        assert_eq!(mgr.get_target("sn1", "t1").unwrap(), "client1");

        // Unsubscribe client2
        assert!(mgr.unsubscribe("sn1", "t1", "client2"));

        // Now only client1 should receive messages
        assert_eq!(mgr.get_target("sn1", "t1").unwrap(), "client1");
        assert_eq!(mgr.get_target("sn1", "t1").unwrap(), "client1");
    }

    #[test]
    fn test_no_matching_subscription() {
        let mut mgr = SharedSubscriptionManager::new();

        mgr.subscribe(
            "sn1".to_string(),
            "t1".to_string(),
            "client1".to_string(),
            0,
        );

        // Non-existent share name
        assert!(mgr.get_target("sn2", "t1").is_none());

        // Non-existent topic filter
        assert!(mgr.get_target("sn1", "t2").is_none());
    }

    #[test]
    fn test_three_clients_lru() {
        let mut mgr = SharedSubscriptionManager::new();

        mgr.subscribe(
            "sn1".to_string(),
            "t1".to_string(),
            "client1".to_string(),
            0,
        );
        mgr.subscribe(
            "sn1".to_string(),
            "t1".to_string(),
            "client2".to_string(),
            0,
        );
        mgr.subscribe(
            "sn1".to_string(),
            "t1".to_string(),
            "client3".to_string(),
            0,
        );

        // Should cycle through all three based on LRU
        // First 3 deliveries: all have counter=0, order depends on HashMap iteration
        let first = mgr.get_target("sn1", "t1").unwrap();
        let second = mgr.get_target("sn1", "t1").unwrap();
        let third = mgr.get_target("sn1", "t1").unwrap();

        // All three clients should have received exactly one message
        let mut clients = vec![first.clone(), second.clone(), third.clone()];
        clients.sort();
        assert_eq!(clients, vec!["client1", "client2", "client3"]);

        // Fourth delivery should go to the first client again (counter=1 is oldest)
        let fourth = mgr.get_target("sn1", "t1").unwrap();
        assert_eq!(fourth, first);

        // Fifth delivery should go to the second client (counter=2 is now oldest)
        let fifth = mgr.get_target("sn1", "t1").unwrap();
        assert_eq!(fifth, second);
    }
}
