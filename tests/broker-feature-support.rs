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

#[tokio::test]
async fn test_retain_support_disabled_connack_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-retain-support=false"]);
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

    // Receive CONNACK and check for RetainAvailable property
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );

            // Check that RetainAvailable property is present
            let has_retain_available = connack
                .props()
                .iter()
                .any(|prop| matches!(prop, mqtt_ep::packet::Property::RetainAvailable(_)));
            assert!(
                has_retain_available,
                "CONNACK should contain RetainAvailable property"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_retain_support_disabled_publish_qos0_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-retain-support=false"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Try to publish with retain flag (QoS 0)
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true)
        .payload(b"test message".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    endpoint
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // For QoS 0 with unsupported retain, broker should disconnect
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Try to receive - should fail as connection is closed
    let result =
        tokio::time::timeout(tokio::time::Duration::from_millis(500), endpoint.recv()).await;

    assert!(
        result.is_err() || matches!(result, Ok(Err(_))),
        "Connection should be closed for QoS 0 retain when not supported"
    );
}

#[tokio::test]
async fn test_retain_support_disabled_publish_qos1_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-retain-support=false"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Acquire packet ID
    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");

    // Try to publish with retain flag (QoS 1)
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(packet_id)
        .retain(true)
        .payload(b"test message".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    endpoint
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Should receive PUBACK with ImplementationSpecificError
    let packet = endpoint.recv().await.expect("Failed to receive PUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Puback(puback) => {
            assert_eq!(puback.packet_id(), packet_id);
            assert_eq!(
                puback.reason_code(),
                Some(mqtt_ep::result_code::PubackReasonCode::ImplementationSpecificError)
            );
        }
        _ => panic!("Expected PUBACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_retain_support_disabled_publish_qos2_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-retain-support=false"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Acquire packet ID
    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");

    // Try to publish with retain flag (QoS 2)
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic")
        .qos(mqtt_ep::packet::Qos::ExactlyOnce)
        .packet_id(packet_id)
        .retain(true)
        .payload(b"test message".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    endpoint
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Should receive PUBREC with ImplementationSpecificError
    let packet = endpoint.recv().await.expect("Failed to receive PUBREC");
    match packet {
        mqtt_ep::packet::Packet::V5_0Pubrec(pubrec) => {
            assert_eq!(pubrec.packet_id(), packet_id);
            assert_eq!(
                pubrec.reason_code(),
                Some(mqtt_ep::result_code::PubrecReasonCode::ImplementationSpecificError)
            );
        }
        _ => panic!("Expected PUBREC, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_shared_sub_support_disabled_connack_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-shared-sub-support=false"]);
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

    // Receive CONNACK and check for SharedSubscriptionAvailable property
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );

            // Check that SharedSubscriptionAvailable property is present
            let has_shared_sub_available = connack.props().iter().any(|prop| {
                matches!(
                    prop,
                    mqtt_ep::packet::Property::SharedSubscriptionAvailable(_)
                )
            });
            assert!(
                has_shared_sub_available,
                "CONNACK should contain SharedSubscriptionAvailable property"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_shared_sub_support_disabled_subscribe_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-shared-sub-support=false"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Acquire packet ID
    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");

    // Try to subscribe to a shared subscription
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("$share/group/test/topic", sub_opts)
        .expect("Failed to create SubEntry");

    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    endpoint
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Should receive SUBACK with SharedSubscriptionsNotSupported
    let packet = endpoint.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(suback) => {
            assert_eq!(suback.packet_id(), packet_id);
            let reason_codes = suback.reason_codes();
            assert_eq!(reason_codes.len(), 1);
            assert_eq!(
                reason_codes[0],
                mqtt_ep::result_code::SubackReasonCode::SharedSubscriptionsNotSupported
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_sub_id_support_disabled_connack_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-sub-id-support=false"]);
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

    // Receive CONNACK and check for SubscriptionIdentifierAvailable property
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );

            // Check that SubscriptionIdentifierAvailable property is present
            let has_sub_id_available = connack.props().iter().any(|prop| {
                matches!(
                    prop,
                    mqtt_ep::packet::Property::SubscriptionIdentifierAvailable(_)
                )
            });
            assert!(
                has_sub_id_available,
                "CONNACK should contain SubscriptionIdentifierAvailable property"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_sub_id_support_disabled_subscribe_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-sub-id-support=false"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Acquire packet ID
    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");

    // Try to subscribe with subscription identifier
    let sub_id_prop = mqtt_ep::packet::Property::SubscriptionIdentifier(
        mqtt_ep::packet::SubscriptionIdentifier::new(42)
            .expect("Failed to create SubscriptionIdentifier"),
    );

    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("test/topic", sub_opts).expect("Failed to create SubEntry");

    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
        .props(vec![sub_id_prop])
        .build()
        .expect("Failed to build SUBSCRIBE");

    endpoint
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Should receive SUBACK with Failure
    let packet = endpoint.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(suback) => {
            assert_eq!(suback.packet_id(), 1);
            let reason_codes = suback.reason_codes();
            assert_eq!(reason_codes.len(), 1);
            // Should be a failure code (not success)
            assert!(
                !reason_codes[0].is_success(),
                "Subscription should fail when subscription identifier is not supported"
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_wc_support_disabled_connack_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-wc-support=false"]);
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

    // Receive CONNACK and check for WildcardSubscriptionAvailable property
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success
            );

            // Check that WildcardSubscriptionAvailable property is present
            let has_wc_available = connack.props().iter().any(|prop| {
                matches!(
                    prop,
                    mqtt_ep::packet::Property::WildcardSubscriptionAvailable(_)
                )
            });
            assert!(
                has_wc_available,
                "CONNACK should contain WildcardSubscriptionAvailable property"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_wc_support_disabled_subscribe_single_level_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-wc-support=false"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Acquire packet ID
    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");

    // Try to subscribe with single-level wildcard
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("test/+/topic", sub_opts)
        .expect("Failed to create SubEntry");

    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    endpoint
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Should receive SUBACK with WildcardSubscriptionsNotSupported
    let packet = endpoint.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(suback) => {
            assert_eq!(suback.packet_id(), packet_id);
            let reason_codes = suback.reason_codes();
            assert_eq!(reason_codes.len(), 1);
            assert_eq!(
                reason_codes[0],
                mqtt_ep::result_code::SubackReasonCode::WildcardSubscriptionsNotSupported
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_wc_support_disabled_subscribe_multi_level_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-wc-support=false"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Acquire packet ID
    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");

    // Try to subscribe with multi-level wildcard
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("test/#", sub_opts).expect("Failed to create SubEntry");

    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    endpoint
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Should receive SUBACK with WildcardSubscriptionsNotSupported
    let packet = endpoint.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(suback) => {
            assert_eq!(suback.packet_id(), packet_id);
            let reason_codes = suback.reason_codes();
            assert_eq!(reason_codes.len(), 1);
            assert_eq!(
                reason_codes[0],
                mqtt_ep::result_code::SubackReasonCode::WildcardSubscriptionsNotSupported
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_wc_support_disabled_subscribe_exact_match_allowed_v5_0() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-wc-support=false"]);
    broker.wait_ready().await;

    let endpoint = connect_client(broker.port(), "test_client").await;

    // Acquire packet ID
    let packet_id = endpoint
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");

    // Subscribe to exact match topic (no wildcards) should still work
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("test/topic", sub_opts).expect("Failed to create SubEntry");

    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    endpoint
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Should receive SUBACK with success
    let packet = endpoint.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(suback) => {
            assert_eq!(suback.packet_id(), packet_id);
            let reason_codes = suback.reason_codes();
            assert_eq!(reason_codes.len(), 1);
            assert!(
                reason_codes[0].is_success(),
                "Exact match subscription should succeed even when wildcards are disabled"
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }
}
