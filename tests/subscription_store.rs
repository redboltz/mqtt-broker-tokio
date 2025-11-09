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

// Include session_store module for testing (must be included first since subscription_store depends on it)
mod session_store {
    include!("../src/session_store.rs");
}

// Include shared_subscription_manager module for testing (subscription_store depends on it)
mod shared_subscription_manager {
    include!("../src/shared_subscription_manager.rs");
}

// Include subscription_store module for testing
mod common_subscription {
    include!("../src/subscription_store.rs");
}

use common_subscription::SubscriptionStore;
use session_store::{SessionId, SessionRef};

// Helper function to create mock session reference
fn create_mock_session_ref(id: &str) -> SessionRef {
    SessionRef::new(SessionId::new(None, id.to_string()))
}
#[tokio::test]
async fn test_same_endpoint_multiple_subscriptions() {
    let store = SubscriptionStore::new();
    let session_ref = create_mock_session_ref("client1");

    // Same endpoint subscribes to multiple patterns that match the same topic
    store
        .subscribe(
            session_ref.clone(),
            "a/b",
            mqtt_ep::packet::Qos::AtMostOnce,
            Some(1),
            false,
            false,
        )
        .await
        .unwrap();
    store
        .subscribe(
            session_ref.clone(),
            "a/+",
            mqtt_ep::packet::Qos::AtLeastOnce,
            Some(2),
            false,
            false,
        )
        .await
        .unwrap();
    store
        .subscribe(
            session_ref.clone(),
            "a/#",
            mqtt_ep::packet::Qos::ExactlyOnce,
            Some(3),
            false,
            false,
        )
        .await
        .unwrap();

    let subscriptions = store
        .find_subscribers(
            "a/b",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;

    // Should get 3 subscriptions for the same endpoint
    assert_eq!(subscriptions.len(), 3);

    // Verify all subscriptions are for the same endpoint but different topic filters
    let topic_filters: Vec<String> = subscriptions
        .iter()
        .map(|s| s.topic_filter.clone())
        .collect();
    assert!(topic_filters.contains(&"a/b".to_string()));
    assert!(topic_filters.contains(&"a/+".to_string()));
    assert!(topic_filters.contains(&"a/#".to_string()));

    // Verify QoS and sub_id values
    for sub in &subscriptions {
        match sub.topic_filter.as_str() {
            "a/b" => {
                assert_eq!(sub.qos, mqtt_ep::packet::Qos::AtMostOnce);
                assert_eq!(sub.sub_id, Some(1));
            }
            "a/+" => {
                assert_eq!(sub.qos, mqtt_ep::packet::Qos::AtLeastOnce);
                assert_eq!(sub.sub_id, Some(2));
            }
            "a/#" => {
                assert_eq!(sub.qos, mqtt_ep::packet::Qos::ExactlyOnce);
                assert_eq!(sub.sub_id, Some(3));
            }
            _ => panic!("Unexpected topic filter: {}", sub.topic_filter),
        }
    }
}

#[tokio::test]
async fn test_qos_and_sub_id_overwrite() {
    let store = SubscriptionStore::new();
    let session_ref = create_mock_session_ref("client1");

    // Subscribe with initial values
    store
        .subscribe(
            session_ref.clone(),
            "test/topic",
            mqtt_ep::packet::Qos::AtMostOnce,
            Some(100),
            false,
            false,
        )
        .await
        .unwrap();

    let subscriptions = store
        .find_subscribers(
            "test/topic",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].qos, mqtt_ep::packet::Qos::AtMostOnce);
    assert_eq!(subscriptions[0].sub_id, Some(100));

    // Subscribe again with different QoS and sub_id (should overwrite)
    store
        .subscribe(
            session_ref.clone(),
            "test/topic",
            mqtt_ep::packet::Qos::ExactlyOnce,
            Some(200),
            false,
            false,
        )
        .await
        .unwrap();

    let subscriptions = store
        .find_subscribers(
            "test/topic",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1); // Still only one subscription
    assert_eq!(subscriptions[0].qos, mqtt_ep::packet::Qos::ExactlyOnce);
    assert_eq!(subscriptions[0].sub_id, Some(200));
}

#[tokio::test]
async fn test_partial_unsubscribe() {
    let store = SubscriptionStore::new();
    let session_ref = create_mock_session_ref("client1");

    // Subscribe to multiple patterns
    store
        .subscribe(
            session_ref.clone(),
            "sensor/+/temperature",
            mqtt_ep::packet::Qos::AtMostOnce,
            Some(1),
            false,
            false,
        )
        .await
        .unwrap();
    store
        .subscribe(
            session_ref.clone(),
            "sensor/#",
            mqtt_ep::packet::Qos::AtLeastOnce,
            Some(2),
            false,
            false,
        )
        .await
        .unwrap();
    store
        .subscribe(
            session_ref.clone(),
            "sensor/room1/temperature",
            mqtt_ep::packet::Qos::ExactlyOnce,
            Some(3),
            false,
            false,
        )
        .await
        .unwrap();

    // Verify all match
    let subscriptions = store
        .find_subscribers(
            "sensor/room1/temperature",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 3);

    // Unsubscribe from one pattern
    let removed = store
        .unsubscribe(&session_ref, "sensor/+/temperature")
        .await
        .unwrap();
    assert!(removed);

    // Verify only the unsubscribed pattern is removed
    let subscriptions = store
        .find_subscribers(
            "sensor/room1/temperature",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 2);

    let topic_filters: Vec<String> = subscriptions
        .iter()
        .map(|s| s.topic_filter.clone())
        .collect();
    assert!(!topic_filters.contains(&"sensor/+/temperature".to_string()));
    assert!(topic_filters.contains(&"sensor/#".to_string()));
    assert!(topic_filters.contains(&"sensor/room1/temperature".to_string()));
}

#[tokio::test]
async fn test_complex_wildcard_patterns() {
    let store = SubscriptionStore::new();
    let session_ref = create_mock_session_ref("client1");

    // Subscribe to complex pattern: a/+/c/+/e
    store
        .subscribe(
            session_ref.clone(),
            "a/+/c/+/e",
            mqtt_ep::packet::Qos::AtMostOnce,
            Some(1),
            false,
            false,
        )
        .await
        .unwrap();

    // Should match
    let subscriptions = store
        .find_subscribers(
            "a/b/c/d/e",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1);

    let subscriptions = store
        .find_subscribers(
            "a/x/c/y/e",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1);

    // Should not match
    let subscriptions = store
        .find_subscribers(
            "a/b/c/d",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await; // Missing 'e'
    assert_eq!(subscriptions.len(), 0);

    let subscriptions = store
        .find_subscribers(
            "a/b/c/d/e/f",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await; // Too deep
    assert_eq!(subscriptions.len(), 0);

    let subscriptions = store
        .find_subscribers(
            "a/b/x/d/e",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await; // Wrong middle segment
    assert_eq!(subscriptions.len(), 0);
}

#[tokio::test]
async fn test_mixed_wildcard_pattern() {
    let store = SubscriptionStore::new();
    let session_ref = create_mock_session_ref("client1");

    // Subscribe to mixed pattern: a/+/#
    store
        .subscribe(
            session_ref.clone(),
            "a/+/#",
            mqtt_ep::packet::Qos::AtMostOnce,
            Some(1),
            false,
            false,
        )
        .await
        .unwrap();

    // Should NOT match "a/b" (no trailing slash, # needs at least one more level)
    let subscriptions = store
        .find_subscribers(
            "a/b",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 0);

    // Should match "a/b/" (with trailing level)
    let subscriptions = store
        .find_subscribers(
            "a/b/",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1);

    // Should match "a/b/c"
    let subscriptions = store
        .find_subscribers(
            "a/b/c",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1);

    // Should match "a/b/c/d/e/f"
    let subscriptions = store
        .find_subscribers(
            "a/b/c/d/e/f",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1);

    // Should NOT match "a" (needs the + level)
    let subscriptions = store
        .find_subscribers(
            "a",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 0);

    // Should NOT match "b/x/y" (wrong prefix)
    let subscriptions = store
        .find_subscribers(
            "b/x/y",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 0);
}

#[tokio::test]
async fn test_root_multilevel_wildcard() {
    let store = SubscriptionStore::new();
    let session_ref = create_mock_session_ref("client1");

    // Subscribe to root multilevel wildcard
    store
        .subscribe(
            session_ref.clone(),
            "#",
            mqtt_ep::packet::Qos::AtMostOnce,
            Some(1),
            false,
            false,
        )
        .await
        .unwrap();

    // Should match everything
    let subscriptions = store
        .find_subscribers(
            "a",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1);

    let subscriptions = store
        .find_subscribers(
            "a/b/c/d",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1);

    let subscriptions = store
        .find_subscribers(
            "sensor/room1/temperature",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1);
}

#[tokio::test]
async fn test_empty_segment_handling() {
    let store = SubscriptionStore::new();
    let session_ref = create_mock_session_ref("client1");

    // Subscribe to pattern with empty segments
    store
        .subscribe(
            session_ref.clone(),
            "a//b",
            mqtt_ep::packet::Qos::AtMostOnce,
            Some(1),
            false,
            false,
        )
        .await
        .unwrap();
    store
        .subscribe(
            session_ref.clone(),
            "a/+/b",
            mqtt_ep::packet::Qos::AtLeastOnce,
            Some(2),
            false,
            false,
        )
        .await
        .unwrap();

    // Should match exactly
    let subscriptions = store
        .find_subscribers(
            "a//b",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 2); // Both patterns should match

    // The + should match empty segment
    let topic_filters: Vec<String> = subscriptions
        .iter()
        .map(|s| s.topic_filter.clone())
        .collect();
    assert!(topic_filters.contains(&"a//b".to_string()));
    assert!(topic_filters.contains(&"a/+/b".to_string()));
}

#[tokio::test]
async fn test_multiple_endpoints_same_pattern() {
    let store = SubscriptionStore::new();
    let session_ref1 = create_mock_session_ref("client1");
    let session_ref2 = create_mock_session_ref("client2");

    // Different sessions subscribe to same pattern
    store
        .subscribe(
            session_ref1.clone(),
            "test/topic",
            mqtt_ep::packet::Qos::AtMostOnce,
            Some(1),
            false,
            false,
        )
        .await
        .unwrap();
    store
        .subscribe(
            session_ref2.clone(),
            "test/topic",
            mqtt_ep::packet::Qos::AtLeastOnce,
            Some(2),
            false,
            false,
        )
        .await
        .unwrap();

    let subscriptions = store
        .find_subscribers(
            "test/topic",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 2);

    // Verify different sessions
    let session_refs: Vec<_> = subscriptions.iter().map(|s| &s.session_ref).collect();
    assert!(session_refs.contains(&&session_ref1));
    assert!(session_refs.contains(&&session_ref2));
}

#[tokio::test]
async fn test_unsubscribe_nonexistent() {
    let store = SubscriptionStore::new();
    let session_ref = create_mock_session_ref("client1");

    // Try to unsubscribe from non-existent subscription
    let removed = store
        .unsubscribe(&session_ref, "nonexistent/topic")
        .await
        .unwrap();
    assert!(!removed);
}

#[tokio::test]
async fn test_subscription_with_none_sub_id() {
    let store = SubscriptionStore::new();
    let session_ref = create_mock_session_ref("client1");

    // Subscribe with None sub_id (v3.1.1 style)
    store
        .subscribe(
            session_ref.clone(),
            "test/topic",
            mqtt_ep::packet::Qos::AtMostOnce,
            None,
            false,
            false,
        )
        .await
        .unwrap();

    let subscriptions = store
        .find_subscribers(
            "test/topic",
            None::<fn(&crate::session_store::SessionRef, &str) -> bool>,
        )
        .await;
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].sub_id, None);
}
