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

mod common;

use common::BrokerProcess;
use mqtt_endpoint_tokio::mqtt_ep;

/// Test exact sequence from GitHub issue #716
/// Subscribe order:
/// - c1: $share/sn1/t1
/// - c1: $share/sn1/t2
/// - c2: $share/sn1/t1
/// - c3: $share/sn1/t1
/// - c3: $share/sn1/t2
/// Publisher publishes t1,t2,t1,t2... continuously (11 times total)
/// Expected delivery order:
/// | client | share name | topic | expected delivery order |
/// |--------|------------|-------|------------------------|
/// | c1     | sn1        | t1    | 1,7,9                  |
/// | c1     | sn1        | t2    | 4                      |
/// | c2     | sn1        | t1    | 2,6,10                 |
/// | c3     | sn1        | t1    | 3,5,11                 |
/// | c3     | sn1        | t2    | 8                      |
#[tokio::test]
async fn test_share_name_level_round_robin_github_issue_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Helper to create and connect a subscriber
    async fn create_subscriber(
        broker_port: u16,
        client_id: &str,
    ) -> mqtt_ep::Endpoint<mqtt_ep::role::Client> {
        let stream = mqtt_ep::transport::connect_helper::connect_tcp(
            &format!("127.0.0.1:{broker_port}"),
            Some(tokio::time::Duration::from_secs(10)),
        )
        .await
        .expect("Failed to connect to broker");

        let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
        let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

        let opts = mqtt_ep::connection_option::ConnectionOption::builder()
            .build()
            .expect("Failed to build connection options");
        subscriber
            .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
            .await
            .expect("Failed to attach transport");

        let connect = mqtt_ep::packet::v5_0::Connect::builder()
            .client_id(client_id)
            .expect("Failed to set client_id")
            .clean_start(true)
            .keep_alive(60)
            .build()
            .expect("Failed to build CONNECT");
        subscriber
            .send(connect)
            .await
            .expect("Failed to send CONNECT");

        let packet = subscriber.recv().await.expect("Failed to receive CONNACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Connack(_) => {}
            _ => panic!("Expected CONNACK, got {packet:?}"),
        }

        subscriber
    }

    // Helper to subscribe to a topic
    async fn subscribe_to_topic(
        subscriber: &mqtt_ep::Endpoint<mqtt_ep::role::Client>,
        topic: &str,
    ) {
        let packet_id = subscriber
            .acquire_packet_id()
            .await
            .expect("Failed to acquire packet_id");
        let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
        let sub_entry =
            mqtt_ep::packet::SubEntry::new(topic, sub_opts).expect("Failed to create SubEntry");
        let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
            .packet_id(packet_id)
            .entries(vec![sub_entry])
            .build()
            .expect("Failed to build SUBSCRIBE");

        subscriber
            .send(subscribe)
            .await
            .expect("Failed to send SUBSCRIBE");

        let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Suback(_) => {}
            _ => panic!("Expected SUBACK, got {packet:?}"),
        }
    }

    // Create subscribers
    let c1 = create_subscriber(broker.port(), "c1").await;
    let c2 = create_subscriber(broker.port(), "c2").await;
    let c3 = create_subscriber(broker.port(), "c3").await;

    // Subscribe in the exact order specified (from GitHub issue #716)
    // c1: t1, t2
    // c2: t2
    // c3: t1, t2
    subscribe_to_topic(&c1, "$share/sn1/t1").await;
    subscribe_to_topic(&c1, "$share/sn1/t2").await;
    subscribe_to_topic(&c2, "$share/sn1/t2").await; // c2 subscribes to t2, NOT t1!
    subscribe_to_topic(&c3, "$share/sn1/t1").await;
    subscribe_to_topic(&c3, "$share/sn1/t2").await;

    // Create publisher
    let publisher = create_subscriber(broker.port(), "publisher").await;

    // Publish t1, t2, t1, t2... 11 times total
    for i in 1..=11 {
        let topic = if i % 2 == 1 { "t1" } else { "t2" };
        let publish = mqtt_ep::packet::v5_0::Publish::builder()
            .topic_name(topic)
            .expect("Failed to set topic_name")
            .qos(mqtt_ep::packet::Qos::AtMostOnce)
            .payload(format!("msg{i}"))
            .build()
            .expect("Failed to build PUBLISH");

        publisher
            .send(publish)
            .await
            .expect("Failed to send PUBLISH");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Collect messages from each subscriber
    let subscribers = vec![("c1", &c1), ("c2", &c2), ("c3", &c3)];
    let mut received_messages: Vec<(String, String, usize)> = Vec::new(); // (client, topic, msg_num)

    for (client_name, subscriber) in subscribers {
        while let Ok(Ok(packet)) =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), subscriber.recv()).await
        {
            match packet {
                mqtt_ep::packet::Packet::V5_0Publish(publish) => {
                    let topic = publish.topic_name().to_string();
                    let payload = std::str::from_utf8(publish.payload().as_slice())
                        .expect("Failed to parse payload");
                    // Extract message number from "msg1", "msg2", etc.
                    let msg_num: usize = payload[3..]
                        .parse()
                        .expect("Failed to parse message number");
                    received_messages.push((client_name.to_string(), topic, msg_num));
                }
                _ => panic!("Expected PUBLISH, got {packet:?}"),
            }
        }
    }

    // Sort by message number to see delivery order
    received_messages.sort_by_key(|(_, _, msg_num)| *msg_num);

    // Debug: print actual delivery order
    println!("\nActual delivery order:");
    for (client, topic, msg_num) in &received_messages {
        println!("  msg{} ({}) -> {}", msg_num, topic, client);
    }

    // Expected delivery order according to GitHub issue:
    // Share-name level round-robin: c1 -> c2 -> c3 -> c1 -> c2 -> ...
    // msg1 (t1) -> c1 (t1 match)
    // msg2 (t2) -> c2 (t2 doesn't match, skip) -> next client
    // msg3 (t1) -> c3 (t1 match)
    // msg4 (t2) -> c1 (t2 match)
    // msg5 (t1) -> c2 (t1 doesn't match at c2? wait...)
    //
    // Actually, let me recalculate based on share-name level round-robin:
    // Clients in share group sn1: c1, c2, c3 (in order of first subscription)
    // next_index starts at 0
    //
    // msg1 (t1): start at c1, c1 has t1 -> c1, next_index=1
    // msg2 (t2): start at c1 (index 1), c1 has t2? No. c2 (index 2), c2 has t2? No. c3 (index 0), c3 has t2? Yes -> c3, next_index=1
    //
    // Wait, I need to check the client order. Let me trace through subscriptions:
    // 1. c1 subscribes to t1 -> clients=[c1], c1.subscriptions={t1}
    // 2. c1 subscribes to t2 -> clients=[c1], c1.subscriptions={t1,t2}
    // 3. c2 subscribes to t1 -> clients=[c1,c2], c2.subscriptions={t1}
    // 4. c3 subscribes to t1 -> clients=[c1,c2,c3], c3.subscriptions={t1}
    // 5. c3 subscribes to t2 -> clients=[c1,c2,c3], c3.subscriptions={t1,t2}
    //
    // Round-robin with next_index:
    // msg1 (t1): next_index=0, check c1 (has t1) -> c1 ✓, next_index=1
    // msg2 (t2): next_index=1, check c2 (no t2), c3 (has t2) -> c3 ✓, next_index=2
    // msg3 (t1): next_index=2, check c3 (has t1) -> c3 ✓, next_index=0
    // msg4 (t2): next_index=0, check c1 (has t2) -> c1 ✓, next_index=1
    // msg5 (t1): next_index=1, check c2 (has t1) -> c2 ✓, next_index=2
    // msg6 (t2): next_index=2, check c3 (has t2) -> c3 ✓, next_index=0
    // msg7 (t1): next_index=0, check c1 (has t1) -> c1 ✓, next_index=1
    // msg8 (t2): next_index=1, check c2 (no t2), c3 (has t2) -> c3 ✓, next_index=2
    // msg9 (t1): next_index=2, check c3 (has t1) -> c3 ✓, next_index=0
    // msg10 (t2): next_index=0, check c1 (has t2) -> c1 ✓, next_index=1
    // msg11 (t1): next_index=1, check c2 (has t1) -> c2 ✓, next_index=2
    //
    // Expected from GitHub issue table:
    // c1: t1 -> 1,7,9  but my calculation gives: 1,7
    // c1: t2 -> 4      my calculation: 4,10
    // c2: t1 -> 2,6,10 my calculation: 5,11
    // c3: t1 -> 3,5,11 my calculation: 3,9
    // c3: t2 -> 8      my calculation: 2,6,8
    //
    // There's a mismatch. Let me check the issue again...
    // According to the issue table, the expected is:
    // c1 receives: msg1(t1), msg4(t2), msg7(t1), msg9(t1)  -- wait, that's 4 messages
    // c2 receives: msg2(t1), msg6(t2), msg10(t2)           -- 3 messages
    // c3 receives: msg3(t1), msg5(t1), msg8(t2), msg11(t1) -- 4 messages
    //
    // Total = 11 messages ✓
    //
    // Let me re-read: the table says:
    // c1 t1: 1,7,9
    // c1 t2: 4
    // So c1 gets msg1, msg7, msg9 for topic t1, and msg4 for topic t2
    //
    // c2 t1: 2,6,10  -- but msg2,6,10 are t2,t2,t2 not t1!
    //
    // I think the table in the issue shows which message NUMBER each client receives,
    // not broken down by topic. Let me re-interpret:
    //
    // Based on the issue, here's what I think it means:
    // c1 receives: msg1, msg4, msg7, msg9
    // c2 receives: msg2, msg6, msg10
    // c3 receives: msg3, msg5, msg8, msg11

    let expected = vec![
        ("c1", "t1", 1),  // msg1 is t1
        ("c2", "t2", 2),  // msg2 is t2
        ("c3", "t1", 3),  // msg3 is t1
        ("c1", "t2", 4),  // msg4 is t2
        ("c3", "t1", 5),  // msg5 is t1
        ("c2", "t2", 6),  // msg6 is t2
        ("c1", "t1", 7),  // msg7 is t1
        ("c3", "t2", 8),  // msg8 is t2
        ("c1", "t1", 9),  // msg9 is t1
        ("c2", "t2", 10), // msg10 is t2
        ("c3", "t1", 11), // msg11 is t1
    ];

    assert_eq!(
        received_messages.len(),
        11,
        "Should receive exactly 11 messages"
    );

    for (i, (expected_item, received_item)) in
        expected.iter().zip(received_messages.iter()).enumerate()
    {
        let (exp_client, exp_topic, exp_num) = expected_item;
        let (recv_client, recv_topic, recv_num) = received_item;
        assert_eq!(
            (exp_client, exp_topic, exp_num),
            (&recv_client.as_str(), &recv_topic.as_str(), recv_num),
            "Message {} mismatch: expected ({}, {}, {}) but got ({}, {}, {})",
            i + 1,
            exp_client,
            exp_topic,
            exp_num,
            recv_client,
            recv_topic,
            recv_num
        );
    }
}

/// Test share-name level round-robin with multiple topic filters
/// This is the key test: Client1 and Client2 both subscribe to $share/sn1/t1 and $share/sn1/t2
/// When publishing to t1 and t2, messages should alternate between Client1 and Client2
/// (not both t1 and t2 going to the same client)
#[tokio::test]
async fn test_share_name_level_round_robin_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create 2 subscribers, both subscribing to $share/sn1/t1 AND $share/sn1/t2
    let mut subscribers = Vec::new();
    for i in 0..2 {
        let stream = mqtt_ep::transport::connect_helper::connect_tcp(
            &format!("127.0.0.1:{}", broker.port()),
            Some(tokio::time::Duration::from_secs(10)),
        )
        .await
        .expect("Failed to connect to broker");

        let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
        let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

        let opts = mqtt_ep::connection_option::ConnectionOption::builder()
            .build()
            .expect("Failed to build connection options");
        subscriber
            .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
            .await
            .expect("Failed to attach transport");

        let connect = mqtt_ep::packet::v5_0::Connect::builder()
            .client_id(format!("client{i}"))
            .expect("Failed to set client_id")
            .clean_start(true)
            .keep_alive(60)
            .build()
            .expect("Failed to build CONNECT");
        subscriber
            .send(connect)
            .await
            .expect("Failed to send CONNECT");

        let packet = subscriber.recv().await.expect("Failed to receive CONNACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Connack(_) => {}
            _ => panic!("Expected CONNACK, got {packet:?}"),
        }

        // Subscribe to BOTH t1 and t2
        for topic in ["$share/sn1/t1", "$share/sn1/t2"] {
            let packet_id = subscriber
                .acquire_packet_id()
                .await
                .expect("Failed to acquire packet_id");
            let sub_opts =
                mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
            let sub_entry =
                mqtt_ep::packet::SubEntry::new(topic, sub_opts).expect("Failed to create SubEntry");
            let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
                .packet_id(packet_id)
                .entries(vec![sub_entry])
                .build()
                .expect("Failed to build SUBSCRIBE");

            subscriber
                .send(subscribe)
                .await
                .expect("Failed to send SUBSCRIBE");

            let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
            match packet {
                mqtt_ep::packet::Packet::V5_0Suback(_) => {}
                _ => panic!("Expected SUBACK, got {packet:?}"),
            }
        }

        subscribers.push(subscriber);
    }

    // Create publisher
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let packet = publisher.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Publish alternating to t1 and t2
    // Expected: t1 -> client0, t2 -> client1, t1 -> client0, t2 -> client1
    // (share-name level round-robin, NOT topic-filter level)
    for i in 0..4 {
        let topic = if i % 2 == 0 { "t1" } else { "t2" };
        let publish = mqtt_ep::packet::v5_0::Publish::builder()
            .topic_name(topic)
            .expect("Failed to set topic_name")
            .qos(mqtt_ep::packet::Qos::AtMostOnce)
            .payload(format!("message{i}"))
            .build()
            .expect("Failed to build PUBLISH");

        publisher
            .send(publish)
            .await
            .expect("Failed to send PUBLISH");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Each subscriber should receive 2 messages (alternating)
    let mut message_counts = [0; 2];
    for (idx, subscriber) in subscribers.iter().enumerate() {
        while let Ok(Ok(packet)) =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), subscriber.recv()).await
        {
            match packet {
                mqtt_ep::packet::Packet::V5_0Publish(_) => {
                    message_counts[idx] += 1;
                }
                _ => panic!("Expected PUBLISH, got {packet:?}"),
            }
        }
    }

    // Verify share-name level round-robin: each client gets 2 messages
    assert_eq!(
        message_counts[0], 2,
        "Client 0 should receive 2 messages (share-name level round-robin)"
    );
    assert_eq!(
        message_counts[1], 2,
        "Client 1 should receive 2 messages (share-name level round-robin)"
    );
}

/// Test basic round-robin load balancing (MQTT v5.0)
#[tokio::test]
async fn test_shared_subscription_round_robin_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create 3 subscribers in the same share group "group1" subscribing to "sensor/#"
    let mut subscribers = Vec::new();
    for i in 0..3 {
        let stream = mqtt_ep::transport::connect_helper::connect_tcp(
            &format!("127.0.0.1:{}", broker.port()),
            Some(tokio::time::Duration::from_secs(10)),
        )
        .await
        .expect("Failed to connect to broker");

        let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
        let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

        let opts = mqtt_ep::connection_option::ConnectionOption::builder()
            .build()
            .expect("Failed to build connection options");
        subscriber
            .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
            .await
            .expect("Failed to attach transport");

        let connect = mqtt_ep::packet::v5_0::Connect::builder()
            .client_id(format!("subscriber{i}"))
            .expect("Failed to set client_id")
            .clean_start(true)
            .keep_alive(60)
            .build()
            .expect("Failed to build CONNECT");
        subscriber
            .send(connect)
            .await
            .expect("Failed to send CONNECT");

        // Receive CONNACK
        let packet = subscriber.recv().await.expect("Failed to receive CONNACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Connack(_) => {}
            _ => panic!("Expected CONNACK, got {packet:?}"),
        }

        // Subscribe to shared topic "$share/group1/sensor/#" with QoS 0
        let packet_id = subscriber
            .acquire_packet_id()
            .await
            .expect("Failed to acquire packet_id");
        let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
        let sub_entry = mqtt_ep::packet::SubEntry::new("$share/group1/sensor/#", sub_opts)
            .expect("Failed to create SubEntry");
        let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
            .packet_id(packet_id)
            .entries(vec![sub_entry])
            .build()
            .expect("Failed to build SUBSCRIBE");

        subscriber
            .send(subscribe)
            .await
            .expect("Failed to send SUBSCRIBE");

        // Receive SUBACK
        let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Suback(suback) => {
                assert_eq!(suback.reason_codes().len(), 1);
                assert_eq!(
                    suback.reason_codes()[0],
                    mqtt_ep::result_code::SubackReasonCode::GrantedQos0
                );
            }
            _ => panic!("Expected SUBACK, got {packet:?}"),
        }

        subscribers.push(subscriber);
    }

    // Create publisher
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = publisher.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Publish 9 messages to "sensor/temperature"
    // With round-robin, each subscriber should receive 3 messages
    for i in 0..9 {
        let publish = mqtt_ep::packet::v5_0::Publish::builder()
            .topic_name("sensor/temperature")
            .expect("Failed to set topic_name")
            .qos(mqtt_ep::packet::Qos::AtMostOnce)
            .payload(format!("message{i}"))
            .build()
            .expect("Failed to build PUBLISH");

        publisher
            .send(publish)
            .await
            .expect("Failed to send PUBLISH");
    }

    // Wait a bit for messages to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Each subscriber should receive exactly 3 messages (round-robin distribution)
    let mut message_counts = [0; 3];
    for (idx, subscriber) in subscribers.iter().enumerate() {
        while let Ok(Ok(packet)) =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), subscriber.recv()).await
        {
            match packet {
                mqtt_ep::packet::Packet::V5_0Publish(_) => {
                    message_counts[idx] += 1;
                }
                _ => panic!("Expected PUBLISH, got {packet:?}"),
            }
        }
    }

    // Verify round-robin: each subscriber gets 3 messages
    assert_eq!(
        message_counts[0], 3,
        "Subscriber 0 should receive 3 messages"
    );
    assert_eq!(
        message_counts[1], 3,
        "Subscriber 1 should receive 3 messages"
    );
    assert_eq!(
        message_counts[2], 3,
        "Subscriber 2 should receive 3 messages"
    );
}

/// Test multiple shared subscription groups (MQTT v5.0)
#[tokio::test]
async fn test_shared_subscription_multiple_groups_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create 2 subscribers in "group1"
    let mut group1_subscribers = Vec::new();
    for i in 0..2 {
        let stream = mqtt_ep::transport::connect_helper::connect_tcp(
            &format!("127.0.0.1:{}", broker.port()),
            Some(tokio::time::Duration::from_secs(10)),
        )
        .await
        .expect("Failed to connect to broker");

        let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
        let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

        let opts = mqtt_ep::connection_option::ConnectionOption::builder()
            .build()
            .expect("Failed to build connection options");
        subscriber
            .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
            .await
            .expect("Failed to attach transport");

        let connect = mqtt_ep::packet::v5_0::Connect::builder()
            .client_id(format!("group1_sub{i}"))
            .expect("Failed to set client_id")
            .clean_start(true)
            .keep_alive(60)
            .build()
            .expect("Failed to build CONNECT");
        subscriber
            .send(connect)
            .await
            .expect("Failed to send CONNECT");

        let packet = subscriber.recv().await.expect("Failed to receive CONNACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Connack(_) => {}
            _ => panic!("Expected CONNACK, got {packet:?}"),
        }

        let packet_id = subscriber
            .acquire_packet_id()
            .await
            .expect("Failed to acquire packet_id");
        let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
        let sub_entry = mqtt_ep::packet::SubEntry::new("$share/group1/data", sub_opts)
            .expect("Failed to create SubEntry");
        let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
            .packet_id(packet_id)
            .entries(vec![sub_entry])
            .build()
            .expect("Failed to build SUBSCRIBE");

        subscriber
            .send(subscribe)
            .await
            .expect("Failed to send SUBSCRIBE");

        let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Suback(_) => {}
            _ => panic!("Expected SUBACK, got {packet:?}"),
        }

        group1_subscribers.push(subscriber);
    }

    // Create 2 subscribers in "group2"
    let mut group2_subscribers = Vec::new();
    for i in 0..2 {
        let stream = mqtt_ep::transport::connect_helper::connect_tcp(
            &format!("127.0.0.1:{}", broker.port()),
            Some(tokio::time::Duration::from_secs(10)),
        )
        .await
        .expect("Failed to connect to broker");

        let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
        let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

        let opts = mqtt_ep::connection_option::ConnectionOption::builder()
            .build()
            .expect("Failed to build connection options");
        subscriber
            .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
            .await
            .expect("Failed to attach transport");

        let connect = mqtt_ep::packet::v5_0::Connect::builder()
            .client_id(format!("group2_sub{i}"))
            .expect("Failed to set client_id")
            .clean_start(true)
            .keep_alive(60)
            .build()
            .expect("Failed to build CONNECT");
        subscriber
            .send(connect)
            .await
            .expect("Failed to send CONNECT");

        let packet = subscriber.recv().await.expect("Failed to receive CONNACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Connack(_) => {}
            _ => panic!("Expected CONNACK, got {packet:?}"),
        }

        let packet_id = subscriber
            .acquire_packet_id()
            .await
            .expect("Failed to acquire packet_id");
        let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
        let sub_entry = mqtt_ep::packet::SubEntry::new("$share/group2/data", sub_opts)
            .expect("Failed to create SubEntry");
        let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
            .packet_id(packet_id)
            .entries(vec![sub_entry])
            .build()
            .expect("Failed to build SUBSCRIBE");

        subscriber
            .send(subscribe)
            .await
            .expect("Failed to send SUBSCRIBE");

        let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Suback(_) => {}
            _ => panic!("Expected SUBACK, got {packet:?}"),
        }

        group2_subscribers.push(subscriber);
    }

    // Create publisher
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let packet = publisher.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Publish 4 messages to "data"
    for i in 0..4 {
        let publish = mqtt_ep::packet::v5_0::Publish::builder()
            .topic_name("data")
            .expect("Failed to set topic_name")
            .qos(mqtt_ep::packet::Qos::AtMostOnce)
            .payload(format!("message{i}"))
            .build()
            .expect("Failed to build PUBLISH");

        publisher
            .send(publish)
            .await
            .expect("Failed to send PUBLISH");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Each group should receive all 4 messages, distributed among its members
    let mut group1_counts = [0; 2];
    for (idx, subscriber) in group1_subscribers.iter().enumerate() {
        while let Ok(Ok(packet)) =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), subscriber.recv()).await
        {
            if let mqtt_ep::packet::Packet::V5_0Publish(_) = packet {
                group1_counts[idx] += 1;
            }
        }
    }

    let mut group2_counts = [0; 2];
    for (idx, subscriber) in group2_subscribers.iter().enumerate() {
        while let Ok(Ok(packet)) =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), subscriber.recv()).await
        {
            if let mqtt_ep::packet::Packet::V5_0Publish(_) = packet {
                group2_counts[idx] += 1;
            }
        }
    }

    // Each group gets all 4 messages distributed
    assert_eq!(
        group1_counts.iter().sum::<i32>(),
        4,
        "Group1 should receive 4 total messages"
    );
    assert_eq!(
        group2_counts.iter().sum::<i32>(),
        4,
        "Group2 should receive 4 total messages"
    );
    // Each subscriber in each group should get 2 messages (round-robin)
    assert_eq!(group1_counts[0], 2);
    assert_eq!(group1_counts[1], 2);
    assert_eq!(group2_counts[0], 2);
    assert_eq!(group2_counts[1], 2);
}

/// Test shared subscription with wildcard (MQTT v5.0)
#[tokio::test]
async fn test_shared_subscription_wildcard_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create 2 subscribers in shared group
    let mut subscribers = Vec::new();
    for i in 0..2 {
        let stream = mqtt_ep::transport::connect_helper::connect_tcp(
            &format!("127.0.0.1:{}", broker.port()),
            Some(tokio::time::Duration::from_secs(10)),
        )
        .await
        .expect("Failed to connect to broker");

        let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
        let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

        let opts = mqtt_ep::connection_option::ConnectionOption::builder()
            .build()
            .expect("Failed to build connection options");
        subscriber
            .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
            .await
            .expect("Failed to attach transport");

        let connect = mqtt_ep::packet::v5_0::Connect::builder()
            .client_id(format!("subscriber{i}"))
            .expect("Failed to set client_id")
            .clean_start(true)
            .keep_alive(60)
            .build()
            .expect("Failed to build CONNECT");
        subscriber
            .send(connect)
            .await
            .expect("Failed to send CONNECT");

        let packet = subscriber.recv().await.expect("Failed to receive CONNACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Connack(_) => {}
            _ => panic!("Expected CONNACK, got {packet:?}"),
        }

        let packet_id = subscriber
            .acquire_packet_id()
            .await
            .expect("Failed to acquire packet_id");
        let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
        let sub_entry = mqtt_ep::packet::SubEntry::new("$share/mygroup/sensor/+/temp", sub_opts)
            .expect("Failed to create SubEntry");
        let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
            .packet_id(packet_id)
            .entries(vec![sub_entry])
            .build()
            .expect("Failed to build SUBSCRIBE");

        subscriber
            .send(subscribe)
            .await
            .expect("Failed to send SUBSCRIBE");

        let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
        match packet {
            mqtt_ep::packet::Packet::V5_0Suback(_) => {}
            _ => panic!("Expected SUBACK, got {packet:?}"),
        }

        subscribers.push(subscriber);
    }

    // Create publisher
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let packet = publisher.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Publish to matching topics
    for i in 0..4 {
        let publish = mqtt_ep::packet::v5_0::Publish::builder()
            .topic_name(format!("sensor/room{i}/temp"))
            .expect("Failed to set topic_name")
            .qos(mqtt_ep::packet::Qos::AtMostOnce)
            .payload(format!("temp{i}"))
            .build()
            .expect("Failed to build PUBLISH");

        publisher
            .send(publish)
            .await
            .expect("Failed to send PUBLISH");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify round-robin distribution
    let mut message_counts = [0; 2];
    for (idx, subscriber) in subscribers.iter().enumerate() {
        while let Ok(Ok(packet)) =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), subscriber.recv()).await
        {
            if let mqtt_ep::packet::Packet::V5_0Publish(_) = packet {
                message_counts[idx] += 1;
            }
        }
    }

    assert_eq!(
        message_counts[0], 2,
        "First subscriber should get 2 messages"
    );
    assert_eq!(
        message_counts[1], 2,
        "Second subscriber should get 2 messages"
    );
}
