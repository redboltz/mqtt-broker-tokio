// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// SPDX-License-Identifier: MIT

//! Tests for MessageExpiryInterval functionality (MQTT v5.0 only)

mod common;

use common::BrokerProcess;
use mqtt_endpoint_tokio::mqtt_ep;
use mqtt_endpoint_tokio::mqtt_ep::prelude::PropertyValueAccess;
use std::time::Duration;
use tokio::time::sleep;

/// Test retained message with MessageExpiryInterval (expired)
#[tokio::test]
async fn test_retained_message_expiry_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Publisher: Publish retained message with 2 second expiry
    let publisher_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let publisher_transport = mqtt_ep::transport::TcpTransport::from_stream(publisher_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(publisher_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    // Wait for CONNACK
    let _connack = publisher.recv().await.expect("Failed to receive CONNACK");

    // Publish retained message with MessageExpiryInterval=2
    let mei = mqtt_ep::packet::MessageExpiryInterval::new(2).unwrap();
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/expiry")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true)
        .payload(b"expired message".to_vec())
        .props(vec![mqtt_ep::packet::Property::MessageExpiryInterval(mei)])
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Wait 3 seconds for message to expire
    sleep(Duration::from_secs(3)).await;

    // Subscriber: Subscribe to the topic
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Subscribe
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("test/expiry", sub_opts).expect("Failed to create SubEntry");

    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    let _suback = subscriber.recv().await.expect("Failed to receive SUBACK");

    // Should NOT receive the retained message (expired)
    tokio::select! {
        result = subscriber.recv() => {
            if let Ok(_packet) = result {
                panic!("Received expired retained message!");
            }
        }
        _ = sleep(Duration::from_millis(500)) => {
            println!("✅ Expired retained message was not delivered");
        }
    }

    publisher.close().await.expect("Failed to close publisher");
    subscriber
        .close()
        .await
        .expect("Failed to close subscriber");
    // broker is automatically killed on drop
}

/// Test retained message with MessageExpiryInterval (not expired, updated)
#[tokio::test]
async fn test_retained_message_expiry_updated_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Publisher: Publish retained message with 10 second expiry
    let publisher_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let publisher_transport = mqtt_ep::transport::TcpTransport::from_stream(publisher_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(publisher_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = publisher.recv().await.expect("Failed to receive CONNACK");

    // Publish retained message with MessageExpiryInterval=10
    let mei = mqtt_ep::packet::MessageExpiryInterval::new(10).unwrap();
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/expiry/updated")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true)
        .payload(b"valid message".to_vec())
        .props(vec![mqtt_ep::packet::Property::MessageExpiryInterval(mei)])
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Wait 2 seconds (message should still be valid)
    sleep(Duration::from_secs(2)).await;

    // Subscriber: Subscribe to the topic
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Subscribe
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("test/expiry/updated", sub_opts)
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

    let _suback = subscriber.recv().await.expect("Failed to receive SUBACK");

    // Should receive the retained message with updated MessageExpiryInterval
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive retained message");

    if let mqtt_ep::packet::Packet::V5_0Publish(pub_packet) = packet {
        assert_eq!(pub_packet.topic_name(), "test/expiry/updated");
        assert_eq!(pub_packet.payload().len(), 13); // "valid message".len()
        assert!(pub_packet.retain());

        // Check that MessageExpiryInterval was updated (should be ~8 or less)
        let mei_prop = pub_packet.props().iter().find_map(|prop| {
            if let mqtt_ep::packet::Property::MessageExpiryInterval(_) = prop {
                prop.as_u32()
            } else {
                None
            }
        });

        if let Some(mei_value) = mei_prop {
            assert!(
                mei_value <= 8,
                "MessageExpiryInterval should be updated (expected ≤8, got {mei_value})"
            );
            println!("✅ MessageExpiryInterval updated correctly: {mei_value}");
        } else {
            panic!("MessageExpiryInterval property not found in retained message");
        }
    } else {
        panic!("Expected V5_0Publish packet");
    }

    publisher.close().await.expect("Failed to close publisher");
    subscriber
        .close()
        .await
        .expect("Failed to close subscriber");
    // broker is automatically killed on drop
}
/// Test Will message with MessageExpiryInterval (expired)
#[tokio::test]
async fn test_will_message_expiry_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Subscriber: Subscribe to Will topic
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Subscribe
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("test/will/expiry", sub_opts)
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

    let _suback = subscriber.recv().await.expect("Failed to receive SUBACK");

    // Publisher with Will message (MessageExpiryInterval=2)
    let publisher_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let publisher_transport = mqtt_ep::transport::TcpTransport::from_stream(publisher_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(publisher_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher");

    // Create CONNECT with Will message (MessageExpiryInterval=2)
    let mei = mqtt_ep::packet::MessageExpiryInterval::new(2).unwrap();
    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .will_message(
            "test/will/expiry",
            b"will message",
            mqtt_ep::packet::Qos::AtMostOnce,
            false,
        )
        .expect("Failed to set will_message")
        .will_props(vec![mqtt_ep::packet::Property::MessageExpiryInterval(mei)])
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = publisher.recv().await.expect("Failed to receive CONNACK");

    // Wait 3 seconds for Will message to expire
    sleep(Duration::from_secs(3)).await;

    // Disconnect publisher abnormally (close without DISCONNECT)
    drop(publisher);

    // Wait a bit for Will message processing
    sleep(Duration::from_millis(500)).await;

    // Subscriber should NOT receive the Will message (expired)
    tokio::select! {
        result = subscriber.recv() => {
            if let Ok(_packet) = result {
                panic!("Received expired Will message!");
            }
        }
        _ = sleep(Duration::from_millis(500)) => {
            println!("✅ Expired Will message was not delivered");
        }
    }

    subscriber
        .close()
        .await
        .expect("Failed to close subscriber");
    // broker is automatically killed on drop
}

/// Test offline message with MessageExpiryInterval (expired)
#[tokio::test]
async fn test_offline_message_expiry_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Subscriber: Connect with persistent session
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    // CONNECT with session_expiry_interval > 0 to keep session
    let sei = mqtt_ep::packet::SessionExpiryInterval::new(60).unwrap();
    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(false)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(sei)])
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Subscribe with QoS 1 for offline message storage
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("test/offline/expiry", sub_opts)
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

    let _suback = subscriber.recv().await.expect("Failed to receive SUBACK");

    // Disconnect subscriber (session will be kept)
    subscriber
        .close()
        .await
        .expect("Failed to close subscriber");

    sleep(Duration::from_millis(500)).await;

    // Publisher: Publish message with MessageExpiryInterval=2
    let publisher_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let publisher_transport = mqtt_ep::transport::TcpTransport::from_stream(publisher_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(publisher_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = publisher.recv().await.expect("Failed to receive CONNACK");

    let mei = mqtt_ep::packet::MessageExpiryInterval::new(2).unwrap();
    let packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/offline/expiry")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .payload(b"offline message".to_vec())
        .props(vec![mqtt_ep::packet::Property::MessageExpiryInterval(mei)])
        .packet_id(packet_id)
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Wait for PUBACK
    let _puback = publisher.recv().await.expect("Failed to receive PUBACK");

    publisher.close().await.expect("Failed to close publisher");

    // Wait 3 seconds for offline message to expire
    sleep(Duration::from_secs(3)).await;

    // Reconnect subscriber
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to reconnect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(false)
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Should NOT receive the offline message (expired)
    tokio::select! {
        result = subscriber.recv() => {
            if let Ok(_packet) = result {
                panic!("Received expired offline message!");
            }
        }
        _ = sleep(Duration::from_millis(500)) => {
            println!("✅ Expired offline message was not delivered");
        }
    }

    subscriber
        .close()
        .await
        .expect("Failed to close subscriber");
    // broker is automatically killed on drop
}

/// Test stored packet (outgoing PUBLISH) with MessageExpiryInterval (expired)
/// Note: This test is actually covered by test_offline_message_expiry_v5_0 since
/// stored packets refer to QoS 1/2 messages stored for offline delivery.
#[tokio::test]
async fn test_stored_packet_expiry_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Subscriber: Connect with persistent session and subscribe to topic
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    // CONNECT with session_expiry_interval > 0 to keep session
    let sei = mqtt_ep::packet::SessionExpiryInterval::new(60).unwrap();
    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(false)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(sei)])
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Subscribe with QoS 1 for offline message storage
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("test/stored/expiry", sub_opts)
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

    let _suback = subscriber.recv().await.expect("Failed to receive SUBACK");

    // Disconnect subscriber without closing session
    subscriber
        .close()
        .await
        .expect("Failed to close subscriber");

    sleep(Duration::from_millis(500)).await;

    // Publisher: Publish message with MessageExpiryInterval=2
    let publisher_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let publisher_transport = mqtt_ep::transport::TcpTransport::from_stream(publisher_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(publisher_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = publisher.recv().await.expect("Failed to receive CONNACK");

    // Publish message with MessageExpiryInterval=2
    let mei = mqtt_ep::packet::MessageExpiryInterval::new(2).unwrap();
    let packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/stored/expiry")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .payload(b"stored message".to_vec())
        .props(vec![mqtt_ep::packet::Property::MessageExpiryInterval(mei)])
        .packet_id(packet_id)
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Wait for PUBACK from broker
    let _puback = publisher.recv().await.expect("Failed to receive PUBACK");

    publisher.close().await.expect("Failed to close publisher");

    // Wait 3 seconds for stored packet to expire
    sleep(Duration::from_secs(3)).await;

    // Reconnect subscriber - offline message should have expired
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to reconnect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(false)
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Should NOT receive the stored packet (offline message expired)
    tokio::select! {
        result = subscriber.recv() => {
            if let Ok(_packet) = result {
                panic!("Received expired stored packet (offline message)!");
            }
        }
        _ = sleep(Duration::from_millis(500)) => {
            println!("✅ Expired stored packet (offline message) was not delivered");
        }
    }

    subscriber
        .close()
        .await
        .expect("Failed to close subscriber");
    // broker is automatically killed on drop
}

/// Test outgoing PUBLISH expiry (QoS 1 packet waiting for PUBACK)
/// This tests Phase 1: timer-based deletion of stored packets
#[tokio::test]
async fn test_outgoing_publish_timer_deletion_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Subscriber: Connect with persistent session and subscribe
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    // CONNECT with session_expiry_interval > 0 to keep session
    let sei = mqtt_ep::packet::SessionExpiryInterval::new(60).unwrap();
    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber_timer")
        .expect("Failed to set client_id")
        .clean_start(false)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(sei)])
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Subscribe with QoS 1
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("test/timer/expiry", sub_opts)
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

    let _suback = subscriber.recv().await.expect("Failed to receive SUBACK");

    // Publisher: Connect and publish message
    let publisher_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let publisher_transport = mqtt_ep::transport::TcpTransport::from_stream(publisher_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(publisher_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_timer")
        .expect("Failed to set client_id")
        .clean_start(true)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = publisher.recv().await.expect("Failed to receive CONNACK");

    // Publish message with MessageExpiryInterval=1 second
    let mei = mqtt_ep::packet::MessageExpiryInterval::new(1).unwrap();
    let packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/timer/expiry")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .payload(b"timer test message".to_vec())
        .props(vec![mqtt_ep::packet::Property::MessageExpiryInterval(mei)])
        .packet_id(packet_id)
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    let _puback = publisher.recv().await.expect("Failed to receive PUBACK");
    publisher.close().await.expect("Failed to close publisher");

    // Subscriber receives PUBLISH
    match subscriber.recv().await {
        Ok(packet) => {
            if let mqtt_ep::packet::Packet::V5_0Publish(_) = packet {
                println!("✅ Received PUBLISH from broker");
                // Close connection immediately WITHOUT sending PUBACK
                // This causes the broker to store the packet in endpoint's stored_packets
                subscriber
                    .close()
                    .await
                    .expect("Failed to close subscriber");
            } else {
                panic!("Expected PUBLISH packet, got: {packet:?}");
            }
        }
        Err(e) => panic!("Failed to receive PUBLISH: {e}"),
    }

    // Wait 3 seconds for the timer to fire (MEI=1, so timer fires after 1 second)
    println!("⏳ Waiting 3 seconds for MessageExpiryInterval timer to fire...");
    sleep(Duration::from_secs(3)).await;

    // Reconnect subscriber - the stored packet should have been deleted by timer
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to reconnect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber_timer")
        .expect("Failed to set client_id")
        .clean_start(false)
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Should NOT receive the stored packet (deleted by timer)
    tokio::select! {
        result = subscriber.recv() => {
            if let Ok(packet) = result {
                panic!("Received expired stored packet that should have been deleted by timer: {packet:?}");
            }
        }
        _ = sleep(Duration::from_millis(500)) => {
            println!("✅ Timer-based deletion worked: expired stored packet was not resent");
        }
    }

    subscriber
        .close()
        .await
        .expect("Failed to close subscriber");
}

/// Test offline message expiry with timer-based deletion
/// This tests Phase 2: timer-based deletion of offline messages
#[tokio::test]
async fn test_offline_message_timer_deletion_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Subscriber: Connect with persistent session and subscribe
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    // CONNECT with session_expiry_interval > 0 to keep session
    let sei = mqtt_ep::packet::SessionExpiryInterval::new(60).unwrap();
    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber_offline_timer")
        .expect("Failed to set client_id")
        .clean_start(false)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(sei)])
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Subscribe with QoS 1
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("test/offline/timer", sub_opts)
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

    let _suback = subscriber.recv().await.expect("Failed to receive SUBACK");

    // Disconnect subscriber
    subscriber
        .close()
        .await
        .expect("Failed to close subscriber");

    println!("✅ Subscriber disconnected");

    sleep(Duration::from_millis(500)).await;

    // Publisher: Publish message with MessageExpiryInterval=1 second while subscriber is offline
    let publisher_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect publisher");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let publisher_transport = mqtt_ep::transport::TcpTransport::from_stream(publisher_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(publisher_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach publisher");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_offline_timer")
        .expect("Failed to set client_id")
        .clean_start(true)
        .build()
        .expect("Failed to build CONNECT");

    publisher
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = publisher.recv().await.expect("Failed to receive CONNACK");

    // Publish message with MessageExpiryInterval=1 second
    let mei = mqtt_ep::packet::MessageExpiryInterval::new(1).unwrap();
    let packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/offline/timer")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .payload(b"offline timer test message".to_vec())
        .props(vec![mqtt_ep::packet::Property::MessageExpiryInterval(mei)])
        .packet_id(packet_id)
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    let _puback = publisher.recv().await.expect("Failed to receive PUBACK");
    publisher.close().await.expect("Failed to close publisher");

    println!("✅ Published message with MEI=1s while subscriber offline");

    // Wait 3 seconds for the timer to fire (MEI=1, so timer fires after 1 second)
    println!("⏳ Waiting 3 seconds for MessageExpiryInterval timer to fire...");
    sleep(Duration::from_secs(3)).await;

    // Reconnect subscriber - the offline message should have been deleted by timer
    let subscriber_stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(Duration::from_secs(10)),
    )
    .await
    .expect("Failed to reconnect subscriber");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let subscriber_transport = mqtt_ep::transport::TcpTransport::from_stream(subscriber_stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(subscriber_transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach subscriber");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber_offline_timer")
        .expect("Failed to set client_id")
        .clean_start(false)
        .build()
        .expect("Failed to build CONNECT");

    subscriber
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    let _connack = subscriber.recv().await.expect("Failed to receive CONNACK");

    // Should NOT receive the offline message (deleted by timer)
    tokio::select! {
        result = subscriber.recv() => {
            if let Ok(packet) = result {
                panic!("Received expired offline message that should have been deleted by timer: {packet:?}");
            }
        }
        _ = sleep(Duration::from_millis(500)) => {
            println!("✅ Timer-based deletion worked: expired offline message was not delivered");
        }
    }

    subscriber
        .close()
        .await
        .expect("Failed to close subscriber");
}
