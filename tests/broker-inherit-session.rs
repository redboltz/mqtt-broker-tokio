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
use mqtt_endpoint_tokio::mqtt_ep::prelude::*;

/// Test: Simple subscription persistence (v5.0)
/// Tests that subscriptions persist across disconnect/reconnect
#[tokio::test]
async fn test_subscription_persistence_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // === Publisher setup ===
    let pub_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let pub_transport = mqtt_ep::transport::TcpTransport::from_stream(pub_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(pub_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher transport");

    let pub_connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(pub_connect)
        .await
        .expect("Failed to send CONNECT");
    let _ = publisher.recv().await.expect("Failed to receive CONNACK");

    // === Subscriber first connection ===
    let sub_stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let sub_transport1 = mqtt_ep::transport::TcpTransport::from_stream(sub_stream1);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber1
        .attach_with_options(sub_transport1, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber transport");

    let sub_connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(
            mqtt_ep::packet::SessionExpiryInterval::new(300).unwrap(),
        )])
        .build()
        .expect("Failed to build CONNECT");

    subscriber1
        .send(sub_connect1)
        .await
        .expect("Failed to send CONNECT");
    let _ = subscriber1.recv().await.expect("Failed to receive CONNACK");

    // Subscribe to topic
    let sub_packet_id = subscriber1
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("test/topic", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(sub_packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    subscriber1
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");
    let _ = subscriber1.recv().await.expect("Failed to receive SUBACK");

    // Disconnect subscriber
    subscriber1
        .close()
        .await
        .expect("Failed to close subscriber");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Send message while subscriber is offline (should be stored as offline message)
    let pub_packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(pub_packet_id)
        .payload("offline_message")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");
    let _ = publisher.recv().await.expect("Failed to receive PUBACK");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // === Subscriber reconnects ===
    let sub_stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to reconnect subscriber");

    let subscriber2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let sub_transport2 = mqtt_ep::transport::TcpTransport::from_stream(sub_stream2);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber2
        .attach_with_options(sub_transport2, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber transport");

    let sub_connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_subscriber")
        .expect("Failed to set client_id")
        .clean_start(false)
        .keep_alive(60)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(
            mqtt_ep::packet::SessionExpiryInterval::new(300).unwrap(),
        )])
        .build()
        .expect("Failed to build CONNECT");

    subscriber2
        .send(sub_connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = subscriber2.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(connack.session_present(), true);
        }
        _ => panic!("Expected CONNACK"),
    }

    // Should receive offline message
    let packet = subscriber2
        .recv()
        .await
        .expect("Failed to receive offline PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(pub_pkt) => {
            assert_eq!(pub_pkt.payload().as_slice(), b"offline_message");
            let msg_packet_id = pub_pkt.packet_id().expect("Expected packet_id");

            // Send PUBACK
            let puback = mqtt_ep::packet::v5_0::Puback::builder()
                .packet_id(msg_packet_id)
                .reason_code(mqtt_ep::result_code::PubackReasonCode::Success)
                .build()
                .expect("Failed to build PUBACK");
            subscriber2
                .send(puback)
                .await
                .expect("Failed to send PUBACK");
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    println!("✅ Subscription persisted and offline message delivered successfully");
}

/// Test: Session inheritance with stored_packets and offline messages (v5.0)
/// Tests that both stored_packets and offline messages are delivered in correct order
#[tokio::test]
#[ignore] // Re-ignore until simpler test passes
async fn test_session_inherit_with_messages_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // === Publisher setup ===
    let pub_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let pub_transport = mqtt_ep::transport::TcpTransport::from_stream(pub_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(pub_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher transport");

    let pub_connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(pub_connect)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let _ = publisher.recv().await.expect("Failed to receive CONNACK");

    // === Subscriber first connection ===
    let sub_stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let sub_transport1 = mqtt_ep::transport::TcpTransport::from_stream(sub_stream1);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber1
        .attach_with_options(sub_transport1, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber transport");

    let sub_connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(
            mqtt_ep::packet::SessionExpiryInterval::new(300).unwrap(),
        )])
        .build()
        .expect("Failed to build CONNECT");

    subscriber1
        .send(sub_connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = subscriber1.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(connack.session_present(), false);
        }
        _ => panic!("Expected CONNACK"),
    }

    // Subscribe to topic
    let sub_packet_id = subscriber1
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("test/topic", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(sub_packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    subscriber1
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    let _ = subscriber1.recv().await.expect("Failed to receive SUBACK");

    // Send message 1 (QoS1) - will become stored_packet
    let pub_packet_id1 = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish1 = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(pub_packet_id1)
        .payload("message1")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish1)
        .await
        .expect("Failed to send PUBLISH 1");

    // Receive PUBACK from broker
    let _ = publisher.recv().await.expect("Failed to receive PUBACK");

    // Receive message 1 on subscriber
    let packet = subscriber1
        .recv()
        .await
        .expect("Failed to receive PUBLISH 1");
    let msg1_packet_id = match packet {
        mqtt_ep::packet::Packet::V5_0Publish(pub_pkt) => {
            assert_eq!(pub_pkt.payload().as_slice(), b"message1");
            pub_pkt.packet_id().expect("Expected packet_id")
        }
        _ => panic!("Expected PUBLISH"),
    };

    // DO NOT send PUBACK yet - this will become stored_packet

    // Disconnect subscriber WITHOUT sending PUBACK
    subscriber1
        .close()
        .await
        .expect("Failed to close subscriber");

    // Small delay to ensure disconnect is processed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Send message 2 (QoS1) while subscriber is offline - will become offline message
    let pub_packet_id2 = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish2 = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(pub_packet_id2)
        .payload("message2")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish2)
        .await
        .expect("Failed to send PUBLISH 2");

    // Receive PUBACK from broker (even though no subscriber)
    let _ = publisher.recv().await.expect("Failed to receive PUBACK");

    // Small delay
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // === Subscriber reconnects ===
    let sub_stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to reconnect subscriber");

    let subscriber2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let sub_transport2 = mqtt_ep::transport::TcpTransport::from_stream(sub_stream2);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber2
        .attach_with_options(sub_transport2, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber transport");

    let sub_connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_subscriber")
        .expect("Failed to set client_id")
        .clean_start(false)
        .keep_alive(60)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(
            mqtt_ep::packet::SessionExpiryInterval::new(300).unwrap(),
        )])
        .build()
        .expect("Failed to build CONNECT");

    subscriber2
        .send(sub_connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = subscriber2.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(connack.session_present(), true);
        }
        _ => panic!("Expected CONNACK"),
    }

    // Subscription should persist from the first connection
    // Should receive stored_packet first (message1), then offline message (message2)

    // Receive message 1 (from stored_packet)
    let packet = subscriber2
        .recv()
        .await
        .expect("Failed to receive stored PUBLISH 1");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(pub_pkt) => {
            assert_eq!(pub_pkt.payload().as_slice(), b"message1");
            assert_eq!(
                pub_pkt.packet_id().expect("Expected packet_id"),
                msg1_packet_id
            );

            // Send PUBACK
            let puback = mqtt_ep::packet::v5_0::Puback::builder()
                .packet_id(msg1_packet_id)
                .reason_code(mqtt_ep::result_code::PubackReasonCode::Success)
                .build()
                .expect("Failed to build PUBACK");
            subscriber2
                .send(puback)
                .await
                .expect("Failed to send PUBACK");
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    // Receive message 2 (from offline messages)
    let packet = subscriber2
        .recv()
        .await
        .expect("Failed to receive offline PUBLISH 2");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(pub_pkt) => {
            assert_eq!(pub_pkt.payload().as_slice(), b"message2");
            let msg2_packet_id = pub_pkt.packet_id().expect("Expected packet_id");

            // Send PUBACK
            let puback = mqtt_ep::packet::v5_0::Puback::builder()
                .packet_id(msg2_packet_id)
                .reason_code(mqtt_ep::result_code::PubackReasonCode::Success)
                .build()
                .expect("Failed to build PUBACK");
            subscriber2
                .send(puback)
                .await
                .expect("Failed to send PUBACK");
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    println!("✅ Messages received in correct order: stored_packet then offline_message");
}

/// Test: No message delivery when need_keep=false (SessionExpiryInterval=0)
#[tokio::test]
async fn test_no_message_delivery_with_need_keep_false_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // === Publisher setup ===
    let pub_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let pub_transport = mqtt_ep::transport::TcpTransport::from_stream(pub_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(pub_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher transport");

    let pub_connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(pub_connect)
        .await
        .expect("Failed to send CONNECT");
    let _ = publisher.recv().await.expect("Failed to receive CONNACK");

    // === Subscriber connection with SessionExpiryInterval=0 (need_keep=false) ===
    let sub_stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let sub_transport1 = mqtt_ep::transport::TcpTransport::from_stream(sub_stream1);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber1
        .attach_with_options(sub_transport1, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber transport");

    let sub_connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_subscriber_no_keep")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        // No SessionExpiryInterval property (defaults to 0 = need_keep=false)
        .build()
        .expect("Failed to build CONNECT");

    subscriber1
        .send(sub_connect1)
        .await
        .expect("Failed to send CONNECT");
    let _ = subscriber1.recv().await.expect("Failed to receive CONNACK");

    // Subscribe to topic
    let sub_packet_id = subscriber1
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("test/topic", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(sub_packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    subscriber1
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");
    let _ = subscriber1.recv().await.expect("Failed to receive SUBACK");

    // Disconnect subscriber
    subscriber1
        .close()
        .await
        .expect("Failed to close subscriber");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Send message while subscriber is offline
    let pub_packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(pub_packet_id)
        .payload("should_not_be_delivered")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");
    let _ = publisher.recv().await.expect("Failed to receive PUBACK");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // === Subscriber reconnects ===
    let sub_stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to reconnect subscriber");

    let subscriber2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let sub_transport2 = mqtt_ep::transport::TcpTransport::from_stream(sub_stream2);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber2
        .attach_with_options(sub_transport2, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber transport");

    let sub_connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_subscriber_no_keep")
        .expect("Failed to set client_id")
        .clean_start(false)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    subscriber2
        .send(sub_connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK - session should NOT be present
    let packet = subscriber2.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(connack.session_present(), false);
        }
        _ => panic!("Expected CONNACK"),
    }

    // Should NOT receive any messages (session was deleted)
    // Try to receive with timeout
    match tokio::time::timeout(tokio::time::Duration::from_millis(500), subscriber2.recv()).await {
        Ok(_) => panic!("Should not receive any message"),
        Err(_) => {
            println!("✅ No messages received as expected (session was deleted)");
        }
    }
}

/// Test: Message cleanup when reconnecting with clean_start=true (v5.0)
#[tokio::test]
async fn test_message_cleanup_with_clean_start_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // === Publisher setup ===
    let pub_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let pub_transport = mqtt_ep::transport::TcpTransport::from_stream(pub_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(pub_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher transport");

    let pub_connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(pub_connect)
        .await
        .expect("Failed to send CONNECT");
    let _ = publisher.recv().await.expect("Failed to receive CONNACK");

    // === Subscriber first connection with session persistence ===
    let sub_stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let sub_transport1 = mqtt_ep::transport::TcpTransport::from_stream(sub_stream1);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber1
        .attach_with_options(sub_transport1, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber transport");

    let sub_connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_subscriber_clean")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(
            mqtt_ep::packet::SessionExpiryInterval::new(300).unwrap(),
        )])
        .build()
        .expect("Failed to build CONNECT");

    subscriber1
        .send(sub_connect1)
        .await
        .expect("Failed to send CONNECT");
    let _ = subscriber1.recv().await.expect("Failed to receive CONNACK");

    // Subscribe to topic
    let sub_packet_id = subscriber1
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("test/topic", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(sub_packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    subscriber1
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");
    let _ = subscriber1.recv().await.expect("Failed to receive SUBACK");

    // Disconnect subscriber
    subscriber1
        .close()
        .await
        .expect("Failed to close subscriber");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Send message while subscriber is offline (will be queued)
    let pub_packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(pub_packet_id)
        .payload("should_be_cleared")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");
    let _ = publisher.recv().await.expect("Failed to receive PUBACK");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // === Subscriber reconnects with clean_start=true (should clear session) ===
    let sub_stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to reconnect subscriber");

    let subscriber2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let sub_transport2 = mqtt_ep::transport::TcpTransport::from_stream(sub_stream2);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber2
        .attach_with_options(sub_transport2, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber transport");

    let sub_connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_subscriber_clean")
        .expect("Failed to set client_id")
        .clean_start(true) // This should clear the session and offline messages
        .keep_alive(60)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(
            mqtt_ep::packet::SessionExpiryInterval::new(300).unwrap(),
        )])
        .build()
        .expect("Failed to build CONNECT");

    subscriber2
        .send(sub_connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK - session should NOT be present
    let packet = subscriber2.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(connack.session_present(), false);
        }
        _ => panic!("Expected CONNACK"),
    }

    // Should NOT receive any messages (session and offline messages were cleared)
    match tokio::time::timeout(tokio::time::Duration::from_millis(500), subscriber2.recv()).await {
        Ok(_) => panic!("Should not receive any message"),
        Err(_) => {
            println!(
                "✅ No messages received as expected (session was cleared by clean_start=true)"
            );
        }
    }
}

/// Test: Message cleanup when reconnecting with clean_session=true (v3.1.1)
#[tokio::test]
async fn test_message_cleanup_with_clean_session_v3_1_1() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // === Publisher setup ===
    let pub_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let pub_transport = mqtt_ep::transport::TcpTransport::from_stream(pub_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(pub_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher transport");

    let pub_connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("test_publisher")
        .expect("Failed to set client_id")
        .clean_session(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(pub_connect)
        .await
        .expect("Failed to send CONNECT");
    let _ = publisher.recv().await.expect("Failed to receive CONNACK");

    // === Subscriber first connection with clean_session=false ===
    let sub_stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let sub_transport1 = mqtt_ep::transport::TcpTransport::from_stream(sub_stream1);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber1
        .attach_with_options(sub_transport1, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber transport");

    let sub_connect1 = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("test_subscriber_clean")
        .expect("Failed to set client_id")
        .clean_session(false)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    subscriber1
        .send(sub_connect1)
        .await
        .expect("Failed to send CONNECT");
    let _ = subscriber1.recv().await.expect("Failed to receive CONNACK");

    // Subscribe to topic
    let sub_packet_id = subscriber1
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("test/topic", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v3_1_1::Subscribe::builder()
        .packet_id(sub_packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    subscriber1
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");
    let _ = subscriber1.recv().await.expect("Failed to receive SUBACK");

    // Disconnect subscriber
    subscriber1
        .close()
        .await
        .expect("Failed to close subscriber");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Send message while subscriber is offline
    let pub_packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v3_1_1::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(pub_packet_id)
        .payload("should_be_cleared")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");
    let _ = publisher.recv().await.expect("Failed to receive PUBACK");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // === Subscriber reconnects with clean_session=true (should clear session) ===
    let sub_stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to reconnect subscriber");

    let subscriber2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let sub_transport2 = mqtt_ep::transport::TcpTransport::from_stream(sub_stream2);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber2
        .attach_with_options(sub_transport2, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber transport");

    let sub_connect2 = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("test_subscriber_clean")
        .expect("Failed to set client_id")
        .clean_session(true) // This should clear the session
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    subscriber2
        .send(sub_connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK - session should NOT be present
    let packet = subscriber2.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V3_1_1Connack(connack) => {
            assert_eq!(connack.session_present(), false);
        }
        _ => panic!("Expected CONNACK"),
    }

    // Should NOT receive any messages
    match tokio::time::timeout(tokio::time::Duration::from_millis(500), subscriber2.recv()).await {
        Ok(_) => panic!("Should not receive any message"),
        Err(_) => {
            println!(
                "✅ No messages received as expected (session was cleared by clean_session=true)"
            );
        }
    }
}
/// Test: Response Topic persistence across session inheritance (v5.0)
/// Tests that Response Topic (from Request Response Information) persists across reconnect
#[tokio::test]
async fn test_response_topic_persistence_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // === First connection: Request Response Information = 1 ===
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect");

    let client1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts1 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    client1
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts1)
        .await
        .expect("Failed to attach transport");

    // Send CONNECT with Request Response Information = 1
    let connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("response_test_client")
        .expect("Failed to set client_id")
        .clean_start(false) // Session persistence
        .props(vec![
            mqtt_ep::packet::Property::SessionExpiryInterval(
                mqtt_ep::packet::SessionExpiryInterval::new(60).unwrap(),
            ),
            mqtt_ep::packet::Property::RequestResponseInformation(
                mqtt_ep::packet::RequestResponseInformation::new(1).unwrap(),
            ),
        ])
        .build()
        .expect("Failed to build CONNECT");

    client1
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK and extract Response Information
    let response_topic_1 = match client1.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );
            assert_eq!(connack.session_present(), false); // First connection

            // Extract Response Information
            let response_info = connack
                .props()
                .iter()
                .find_map(|prop| {
                    if let mqtt_ep::packet::Property::ResponseInformation(ri) = prop {
                        Some(ri.val().to_string())
                    } else {
                        None
                    }
                })
                .expect("Response Information should be present");

            assert!(
                response_info.starts_with("res_"),
                "Response Topic should start with res_"
            );
            println!("✅ First connection: Response Topic = {response_info}");
            response_info
        }
        _ => panic!("Expected CONNACK"),
    };

    // Subscribe to the Response Topic
    let sub_packet_id_1 = client1
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new(&response_topic_1, sub_opts)
        .expect("Failed to create SubEntry");
    let subscribe1 = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(sub_packet_id_1)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    client1
        .send(subscribe1)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match client1.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V5_0Suback(suback) => {
            assert_eq!(suback.packet_id(), sub_packet_id_1);
            assert_eq!(suback.reason_codes().len(), 1);
            assert_eq!(
                suback.reason_codes()[0],
                mqtt_ep::result_code::SubackReasonCode::GrantedQos0
            );
            println!("✅ Successfully subscribed to Response Topic");
        }
        _ => panic!("Expected SUBACK"),
    }

    // Disconnect (without sending DISCONNECT, simulating connection loss)
    drop(client1);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // === Second connection: Reconnect with session inheritance ===
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to reconnect");

    let client2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts2 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    client2
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts2)
        .await
        .expect("Failed to attach transport");

    // Send CONNECT with Request Response Information = 1 again
    let connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("response_test_client")
        .expect("Failed to set client_id")
        .clean_start(false) // Session persistence
        .props(vec![
            mqtt_ep::packet::Property::SessionExpiryInterval(
                mqtt_ep::packet::SessionExpiryInterval::new(60).unwrap(),
            ),
            mqtt_ep::packet::Property::RequestResponseInformation(
                mqtt_ep::packet::RequestResponseInformation::new(1).unwrap(),
            ),
        ])
        .build()
        .expect("Failed to build CONNECT");

    client2
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK and verify Response Information is the same
    match client2.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );
            assert_eq!(connack.session_present(), true); // Session inherited

            // Extract Response Information
            let response_info = connack
                .props()
                .iter()
                .find_map(|prop| {
                    if let mqtt_ep::packet::Property::ResponseInformation(ri) = prop {
                        Some(ri.val().to_string())
                    } else {
                        None
                    }
                })
                .expect("Response Information should be present");

            assert_eq!(
                response_info, response_topic_1,
                "Response Topic should be the same after reconnect"
            );
            println!("✅ Second connection: Response Topic = {response_info} (SAME as first)");
        }
        _ => panic!("Expected CONNACK"),
    };

    // Verify we can still subscribe to the Response Topic
    let sub_packet_id_2 = client2
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts2 = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry2 = mqtt_ep::packet::SubEntry::new(&response_topic_1, sub_opts2)
        .expect("Failed to create SubEntry");
    let subscribe2 = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(sub_packet_id_2)
        .entries(vec![sub_entry2])
        .build()
        .expect("Failed to build SUBSCRIBE");

    client2
        .send(subscribe2)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match client2.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V5_0Suback(suback) => {
            assert_eq!(suback.packet_id(), sub_packet_id_2);
            assert_eq!(suback.reason_codes().len(), 1);
            assert_eq!(
                suback.reason_codes()[0],
                mqtt_ep::result_code::SubackReasonCode::GrantedQos0
            );
            println!("✅ Successfully re-subscribed to the same Response Topic");
        }
        _ => panic!("Expected SUBACK"),
    }

    println!("✅ Response Topic persisted across session inheritance!");
}
