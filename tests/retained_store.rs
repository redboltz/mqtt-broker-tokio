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
use mqtt_endpoint_tokio::mqtt_ep::prelude::IntoPayload;

// We need to expose RetainedStore for testing
// Since it's in src/, we need to add a lib.rs or make it a module
// For now, we'll include it directly
mod common_retained {
    include!("../src/retained_store.rs");
}

use common_retained::RetainedStore;

#[tokio::test]
async fn test_store_and_retrieve_exact_match() {
    let store = RetainedStore::new();

    // Store a message
    store
        .store(
            "sport/tennis/score",
            mqtt_ep::packet::Qos::AtLeastOnce,
            "15-30".into_payload(),
            vec![],
        )
        .await;

    // Retrieve with exact match
    let messages = store.get_matching("sport/tennis/score").await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].topic_name, "sport/tennis/score");
    assert_eq!(messages[0].qos, mqtt_ep::packet::Qos::AtLeastOnce);
    assert_eq!(messages[0].payload.len(), 5); // "15-30" = 5 bytes
}

#[tokio::test]
async fn test_store_overwrite() {
    let store = RetainedStore::new();

    // Store first message
    store
        .store(
            "topic/test",
            mqtt_ep::packet::Qos::AtMostOnce,
            "first".into_payload(),
            vec![],
        )
        .await;

    // Overwrite with second message
    store
        .store(
            "topic/test",
            mqtt_ep::packet::Qos::ExactlyOnce,
            "second".into_payload(),
            vec![],
        )
        .await;

    // Should get only the second message
    let messages = store.get_matching("topic/test").await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload.len(), 6); // "second" = 6 bytes
    assert_eq!(messages[0].qos, mqtt_ep::packet::Qos::ExactlyOnce);
}

#[tokio::test]
async fn test_remove_message() {
    let store = RetainedStore::new();

    // Store a message
    store
        .store(
            "topic/remove",
            mqtt_ep::packet::Qos::AtMostOnce,
            "data".into_payload(),
            vec![],
        )
        .await;

    // Verify it exists
    let messages = store.get_matching("topic/remove").await;
    assert_eq!(messages.len(), 1);

    // Remove it
    store.remove("topic/remove").await;

    // Should be empty now
    let messages = store.get_matching("topic/remove").await;
    assert_eq!(messages.len(), 0);
}

#[tokio::test]
async fn test_single_level_wildcard() {
    let store = RetainedStore::new();

    // Store messages
    store
        .store(
            "sport/tennis/score",
            mqtt_ep::packet::Qos::AtMostOnce,
            "15-30".into_payload(),
            vec![],
        )
        .await;
    store
        .store(
            "sport/soccer/score",
            mqtt_ep::packet::Qos::AtMostOnce,
            "2-1".into_payload(),
            vec![],
        )
        .await;
    store
        .store(
            "sport/tennis/player",
            mqtt_ep::packet::Qos::AtMostOnce,
            "Federer".into_payload(),
            vec![],
        )
        .await;

    // Match with single-level wildcard
    let messages = store.get_matching("sport/+/score").await;
    assert_eq!(messages.len(), 2);

    let topics: Vec<&str> = messages.iter().map(|m| m.topic_name.as_str()).collect();
    assert!(topics.contains(&"sport/tennis/score"));
    assert!(topics.contains(&"sport/soccer/score"));
}

#[tokio::test]
async fn test_multi_level_wildcard() {
    let store = RetainedStore::new();

    // Store messages
    store
        .store(
            "a/b/c/d",
            mqtt_ep::packet::Qos::AtMostOnce,
            "msg1".into_payload(),
            vec![],
        )
        .await;
    store
        .store(
            "a/b/c/e",
            mqtt_ep::packet::Qos::AtMostOnce,
            "msg2".into_payload(),
            vec![],
        )
        .await;
    store
        .store(
            "a/b/f",
            mqtt_ep::packet::Qos::AtMostOnce,
            "msg3".into_payload(),
            vec![],
        )
        .await;
    store
        .store(
            "x/y/z",
            mqtt_ep::packet::Qos::AtMostOnce,
            "msg4".into_payload(),
            vec![],
        )
        .await;

    // Match with multi-level wildcard
    let messages = store.get_matching("a/b/#").await;
    assert_eq!(messages.len(), 3);

    let topics: Vec<&str> = messages.iter().map(|m| m.topic_name.as_str()).collect();
    assert!(topics.contains(&"a/b/c/d"));
    assert!(topics.contains(&"a/b/c/e"));
    assert!(topics.contains(&"a/b/f"));
    assert!(!topics.contains(&"x/y/z"));
}

#[tokio::test]
async fn test_combined_wildcards() {
    let store = RetainedStore::new();

    // Store messages
    store
        .store(
            "a/b/c/d",
            mqtt_ep::packet::Qos::AtMostOnce,
            "msg1".into_payload(),
            vec![],
        )
        .await;
    store
        .store(
            "a/x/c/e",
            mqtt_ep::packet::Qos::AtMostOnce,
            "msg2".into_payload(),
            vec![],
        )
        .await;
    store
        .store(
            "a/y/c/f",
            mqtt_ep::packet::Qos::AtMostOnce,
            "msg3".into_payload(),
            vec![],
        )
        .await;
    store
        .store(
            "a/b/z/g",
            mqtt_ep::packet::Qos::AtMostOnce,
            "msg4".into_payload(),
            vec![],
        )
        .await;

    // Match with a/+/c/#
    let messages = store.get_matching("a/+/c/#").await;
    assert_eq!(messages.len(), 3);

    let topics: Vec<&str> = messages.iter().map(|m| m.topic_name.as_str()).collect();
    assert!(topics.contains(&"a/b/c/d"));
    assert!(topics.contains(&"a/x/c/e"));
    assert!(topics.contains(&"a/y/c/f"));
    assert!(!topics.contains(&"a/b/z/g"));
}

#[tokio::test]
async fn test_no_match() {
    let store = RetainedStore::new();

    // Store a message
    store
        .store(
            "topic/test",
            mqtt_ep::packet::Qos::AtMostOnce,
            "data".into_payload(),
            vec![],
        )
        .await;

    // Try to match with non-matching filter
    let messages = store.get_matching("other/topic").await;
    assert_eq!(messages.len(), 0);
}

#[tokio::test]
async fn test_empty_store() {
    let store = RetainedStore::new();

    // Try to get messages from empty store
    let messages = store.get_matching("any/topic").await;
    assert_eq!(messages.len(), 0);

    // Try to get with wildcard
    let messages = store.get_matching("any/#").await;
    assert_eq!(messages.len(), 0);
}
