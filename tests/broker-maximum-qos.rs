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

// Helper function to create and connect a client endpoint
async fn connect_client(port: u16, client_id: &str) -> mqtt_ep::Endpoint<mqtt_ep::role::Client> {
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{port}"),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let endpoint = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);
    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build ConnectionOption");

    endpoint
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

    endpoint
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    endpoint
}

// Test 1: CONNACK with Maximum QoS = 0
#[tokio::test]
async fn test_maximum_qos_0_connack_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--maximum-qos=0"]);
    broker.wait_ready().await;

    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let endpoint = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);
    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build ConnectionOption");

    endpoint
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_client")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    endpoint
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK and check for Maximum QoS property
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );

            // Check that MaximumQos property is present and set to 0
            let maximum_qos = connack.props().iter().find_map(|prop| {
                if let mqtt_ep::packet::Property::MaximumQos(_) = prop {
                    prop.as_u8()
                } else {
                    None
                }
            });
            assert_eq!(
                maximum_qos,
                Some(0),
                "CONNACK should contain Maximum QoS property with value 0"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

// Test 2: CONNACK with Maximum QoS = 1
#[tokio::test]
async fn test_maximum_qos_1_connack_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--maximum-qos=1"]);
    broker.wait_ready().await;

    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let endpoint = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);
    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build ConnectionOption");

    endpoint
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_client")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    endpoint
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK and check for Maximum QoS property
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );

            // Check that MaximumQos property is present and set to 1
            let maximum_qos = connack.props().iter().find_map(|prop| {
                if let mqtt_ep::packet::Property::MaximumQos(_) = prop {
                    prop.as_u8()
                } else {
                    None
                }
            });
            assert_eq!(
                maximum_qos,
                Some(1),
                "CONNACK should contain Maximum QoS property with value 1"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

// Test 3: CONNACK with Maximum QoS = 2 (default - property should NOT be sent)
#[tokio::test]
async fn test_maximum_qos_2_connack_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--maximum-qos=2"]);
    broker.wait_ready().await;

    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let endpoint = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);
    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build ConnectionOption");

    endpoint
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_client")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    endpoint
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK and check that Maximum QoS property is NOT present
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );

            // Check that MaximumQos property is NOT present
            let has_maximum_qos = connack
                .props()
                .iter()
                .any(|prop| matches!(prop, mqtt_ep::packet::Property::MaximumQos(_)));
            assert!(
                !has_maximum_qos,
                "CONNACK should NOT contain Maximum QoS property when set to 2"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

// Test 4: Will QoS exceeds maximum-qos (should reject)
// Note: This test is commented out because the mqtt-endpoint-tokio Connect builder
// doesn't provide a simple way to set Will parameters. The functionality is tested
// internally when the broker receives CONNECT packets with Will from actual clients.
/*
#[tokio::test]
async fn test_will_qos_exceeds_maximum_qos_v5_0() {
    // This would test that CONNECT packets with Will QoS > maximum-qos are rejected
    // but requires access to Will builder APIs not exposed in the current version
}
*/

// Test 5: SUBSCRIBE with QoS adjustment
#[tokio::test]
async fn test_subscribe_qos_adjustment_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--maximum-qos=1"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Subscribe with QoS 2 (should be adjusted to 1)
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::ExactlyOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("test/topic", sub_opts).expect("Failed to create SubEntry");
    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    endpoint
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK - should return adjusted QoS 1
    let packet = endpoint.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(suback) => {
            assert_eq!(suback.packet_id(), packet_id);
            let reason_codes = suback.reason_codes();
            assert_eq!(reason_codes.len(), 1);
            assert_eq!(
                reason_codes[0],
                mqtt_ep::result_code::SubackReasonCode::GrantedQos1,
                "SUBACK should return adjusted QoS 1"
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }
}

// Test 6: PUBLISH QoS exceeds maximum-qos (should disconnect)
#[tokio::test]
async fn test_publish_qos_exceeds_maximum_qos_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--maximum-qos=1"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Try to publish with QoS 2 (exceeds maximum-qos = 1)
    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::ExactlyOnce)
        .packet_id(packet_id)
        .payload(b"test message".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    endpoint
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Should receive DISCONNECT with QoS not supported
    let packet = endpoint.recv().await.expect("Failed to receive DISCONNECT");
    match packet {
        mqtt_ep::packet::Packet::V5_0Disconnect(disconnect) => {
            assert_eq!(
                disconnect.reason_code(),
                Some(mqtt_ep::result_code::DisconnectReasonCode::QosNotSupported),
                "DISCONNECT should have QoS not supported reason code"
            );
        }
        _ => panic!("Expected DISCONNECT, got {packet:?}"),
    }
}

// Test 7: PUBLISH with allowed QoS (should succeed)
#[tokio::test]
async fn test_publish_qos_allowed_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--maximum-qos=1"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Publish with QoS 1 (within maximum-qos = 1)
    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(packet_id)
        .payload(b"test message".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    endpoint
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Should receive PUBACK (not DISCONNECT)
    // The reason code can be Success or NoMatchingSubscribers (since we haven't subscribed)
    let packet = endpoint.recv().await.expect("Failed to receive PUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Puback(puback) => {
            assert_eq!(puback.packet_id(), packet_id);
            let reason_code = puback.reason_code();
            assert!(
                reason_code == Some(mqtt_ep::result_code::PubackReasonCode::Success)
                    || reason_code
                        == Some(mqtt_ep::result_code::PubackReasonCode::NoMatchingSubscribers),
                "Expected Success or NoMatchingSubscribers, got {reason_code:?}"
            );
        }
        _ => panic!("Expected PUBACK, got {packet:?}"),
    }
}

// Test 8: Multiple subscriptions with QoS adjustment
#[tokio::test]
async fn test_multiple_subscribe_qos_adjustment_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--maximum-qos=1"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Subscribe to multiple topics with different QoS values
    let sub_opts1 = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry1 = mqtt_ep::packet::SubEntry::new("test/topic1", sub_opts1)
        .expect("Failed to create SubEntry");

    let sub_opts2 = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry2 = mqtt_ep::packet::SubEntry::new("test/topic2", sub_opts2)
        .expect("Failed to create SubEntry");

    let sub_opts3 = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::ExactlyOnce);
    let sub_entry3 = mqtt_ep::packet::SubEntry::new("test/topic3", sub_opts3)
        .expect("Failed to create SubEntry");

    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry1, sub_entry2, sub_entry3])
        .build()
        .expect("Failed to build SUBSCRIBE");

    endpoint
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    let packet = endpoint.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(suback) => {
            assert_eq!(suback.packet_id(), packet_id);
            let reason_codes = suback.reason_codes();
            assert_eq!(reason_codes.len(), 3);
            // QoS 0 stays QoS 0
            assert_eq!(
                reason_codes[0],
                mqtt_ep::result_code::SubackReasonCode::GrantedQos0
            );
            // QoS 1 stays QoS 1
            assert_eq!(
                reason_codes[1],
                mqtt_ep::result_code::SubackReasonCode::GrantedQos1
            );
            // QoS 2 is adjusted to QoS 1
            assert_eq!(
                reason_codes[2],
                mqtt_ep::result_code::SubackReasonCode::GrantedQos1
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }
}
