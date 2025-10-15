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

use common::{BrokerProcess, create_connected_endpoint};
use mqtt_endpoint_tokio::mqtt_ep;
use mqtt_endpoint_tokio::mqtt_ep::prelude::*;

// QoS 0 tests

#[tokio::test]
async fn test_pubsub_qos0_v3_1_1() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint (v3.1.1)
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_session(true)
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
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Subscribe to topic "t1" with QoS 0
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("t1", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v3_1_1::Subscribe::builder()
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
        mqtt_ep::packet::Packet::V3_1_1Suback(suback) => {
            assert_eq!(suback.return_codes().len(), 1);
            assert_eq!(
                suback.return_codes()[0],
                mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos0
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Create publisher endpoint (v3.1.1)
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_session(true)
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
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Publish message to topic "t1" with QoS 0
    let publish = mqtt_ep::packet::v3_1_1::Publish::builder()
        .topic_name("t1")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .payload(b"message1".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Subscriber should receive the published message
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH on subscriber");

    match packet {
        mqtt_ep::packet::Packet::V3_1_1Publish(publish) => {
            assert_eq!(publish.topic_name(), "t1");
            assert_eq!(publish.payload().len(), 8);
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtMostOnce);
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_pubsub_qos0_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    // Subscribe to topic "t1" with QoS 0
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("t1", sub_opts).expect("Failed to create SubEntry");
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

    // Create publisher endpoint
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    // Publish message to topic "t1" with QoS 0
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("t1")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .payload(b"message1".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Subscriber should receive the published message
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH on subscriber");

    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "t1");
            assert_eq!(publish.payload().len(), 8);
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtMostOnce);
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

// QoS 1 tests

#[tokio::test]
async fn test_pubsub_qos1_v3_1_1() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint (v3.1.1)
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_session(true)
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
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Subscribe to topic "t1" with QoS 1
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("t1", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v3_1_1::Subscribe::builder()
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
        mqtt_ep::packet::Packet::V3_1_1Suback(suback) => {
            assert_eq!(suback.return_codes().len(), 1);
            assert_eq!(
                suback.return_codes()[0],
                mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos1
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Create publisher endpoint (v3.1.1)
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_session(true)
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
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Publish message to topic "t1" with QoS 1
    let pub_packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v3_1_1::Publish::builder()
        .topic_name("t1")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(pub_packet_id)
        .payload(b"message1".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Publisher should receive PUBACK
    let packet = publisher.recv().await.expect("Failed to receive PUBACK");
    match packet {
        mqtt_ep::packet::Packet::V3_1_1Puback(puback) => {
            assert_eq!(puback.packet_id(), pub_packet_id);
        }
        _ => panic!("Expected PUBACK, got {packet:?}"),
    }

    // Subscriber should receive the published message
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH on subscriber");

    match packet {
        mqtt_ep::packet::Packet::V3_1_1Publish(publish) => {
            assert_eq!(publish.topic_name(), "t1");
            assert_eq!(publish.payload().len(), 8);
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtLeastOnce);
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_pubsub_qos1_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    // Subscribe to topic "t1" with QoS 1
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("t1", sub_opts).expect("Failed to create SubEntry");
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
                mqtt_ep::result_code::SubackReasonCode::GrantedQos1
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Create publisher endpoint
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    // Publish message to topic "t1" with QoS 1
    let pub_packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("t1")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(pub_packet_id)
        .payload(b"message1".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Publisher should receive PUBACK
    let packet = publisher.recv().await.expect("Failed to receive PUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Puback(puback) => {
            assert_eq!(puback.packet_id(), pub_packet_id);
        }
        _ => panic!("Expected PUBACK, got {packet:?}"),
    }

    // Subscriber should receive the published message
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH on subscriber");

    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "t1");
            assert_eq!(publish.payload().len(), 8);
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtLeastOnce);
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

// QoS 2 tests

#[tokio::test]
async fn test_pubsub_qos2_v3_1_1() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint (v3.1.1)
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_session(true)
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
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Subscribe to topic "t1" with QoS 2
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::ExactlyOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("t1", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v3_1_1::Subscribe::builder()
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
        mqtt_ep::packet::Packet::V3_1_1Suback(suback) => {
            assert_eq!(suback.return_codes().len(), 1);
            assert_eq!(
                suback.return_codes()[0],
                mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos2
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Create publisher endpoint (v3.1.1)
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_session(true)
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
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Publish message to topic "t1" with QoS 2
    let pub_packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v3_1_1::Publish::builder()
        .topic_name("t1")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::ExactlyOnce)
        .packet_id(pub_packet_id)
        .payload(b"message1".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // QoS 2 handshake is handled automatically by auto_pub_response=true (default)

    // Subscriber should receive the published message
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH on subscriber");

    match packet {
        mqtt_ep::packet::Packet::V3_1_1Publish(publish) => {
            assert_eq!(publish.topic_name(), "t1");
            assert_eq!(publish.payload().len(), 8);
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::ExactlyOnce);
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_pubsub_qos2_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    // Subscribe to topic "t1" with QoS 2
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::ExactlyOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("t1", sub_opts).expect("Failed to create SubEntry");
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
                mqtt_ep::result_code::SubackReasonCode::GrantedQos2
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Create publisher endpoint
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    // Publish message to topic "t1" with QoS 2
    let pub_packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("t1")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::ExactlyOnce)
        .packet_id(pub_packet_id)
        .payload(b"message1".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // QoS 2 handshake is handled automatically by auto_pub_response=true (default)

    // Subscriber should receive the published message
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH on subscriber");

    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "t1");
            assert_eq!(publish.payload().len(), 8);
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::ExactlyOnce);
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

// QoS downgrade tests

#[tokio::test]
async fn test_qos_downgrade_v3_1_1() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint (v3.1.1) with QoS 0
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_session(true)
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
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Subscribe to topic "t1" with QoS 0
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("t1", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v3_1_1::Subscribe::builder()
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
        mqtt_ep::packet::Packet::V3_1_1Suback(suback) => {
            assert_eq!(suback.return_codes().len(), 1);
            assert_eq!(
                suback.return_codes()[0],
                mqtt_ep::result_code::SubackReturnCode::SuccessMaximumQos0
            );
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Create publisher endpoint (v3.1.1)
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_session(true)
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
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Publish message with QoS 2 (should be downgraded to QoS 0)
    let pub_packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v3_1_1::Publish::builder()
        .topic_name("t1")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::ExactlyOnce)
        .packet_id(pub_packet_id)
        .payload(b"message1".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // QoS 2 handshake is handled automatically by auto_pub_response=true (default)

    // Subscriber should receive the message with downgraded QoS 0
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH on subscriber");

    match packet {
        mqtt_ep::packet::Packet::V3_1_1Publish(publish) => {
            assert_eq!(publish.topic_name(), "t1");
            assert_eq!(publish.payload().len(), 8);
            // QoS should be downgraded to 0
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtMostOnce);
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_qos_downgrade_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint with QoS 0
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("t1", sub_opts).expect("Failed to create SubEntry");
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

    // Create publisher endpoint
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    // Publish message with QoS 2 (should be downgraded to QoS 0)
    let pub_packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("t1")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::ExactlyOnce)
        .packet_id(pub_packet_id)
        .payload(b"message1".to_vec())
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // QoS 2 handshake is handled automatically by auto_pub_response=true (default)

    // Subscriber should receive the message with downgraded QoS 0
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH on subscriber");

    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "t1");
            assert_eq!(publish.payload().len(), 8);
            // QoS should be downgraded to 0
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtMostOnce);
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}
/// Test: Multiple subscriptions with different subscription identifiers matching the same topic
#[tokio::test]
async fn test_pubsub_multiple_subscriptions_with_sub_id_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    // First subscription: a/b/c with no subscription identifier
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("a/b/c", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");

    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send first SUBSCRIBE");

    // Receive SUBACK
    let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Second subscription: a/+/c with subscription identifier 1
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("a/+/c", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
        .props(vec![mqtt_ep::packet::Property::SubscriptionIdentifier(
            mqtt_ep::packet::SubscriptionIdentifier::new(1).unwrap(),
        )])
        .build()
        .expect("Failed to build SUBSCRIBE");

    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send second SUBSCRIBE");

    // Receive SUBACK
    let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Third subscription: a/# with subscription identifier 2
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("a/#", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
        .props(vec![mqtt_ep::packet::Property::SubscriptionIdentifier(
            mqtt_ep::packet::SubscriptionIdentifier::new(2).unwrap(),
        )])
        .build()
        .expect("Failed to build SUBSCRIBE");

    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send third SUBSCRIBE");

    // Receive SUBACK
    let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Create publisher and publish to a/b/c
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("a/b/c")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .payload("test message")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Subscriber should receive 3 PUBLISH messages (one for each matching subscription)
    let mut received_messages = Vec::new();
    for _ in 0..3 {
        let packet = subscriber.recv().await.expect("Failed to receive PUBLISH");
        match packet {
            mqtt_ep::packet::Packet::V5_0Publish(publish) => {
                assert_eq!(publish.topic_name(), "a/b/c");
                assert_eq!(publish.payload().as_slice(), b"test message");

                // Extract subscription identifier from properties
                let sub_id = publish.props().iter().find_map(|prop| match prop {
                    mqtt_ep::packet::Property::SubscriptionIdentifier(_) => prop.as_u32(),
                    _ => None,
                });

                received_messages.push(sub_id);
            }
            _ => panic!("Expected PUBLISH, got {packet:?}"),
        }
    }

    // Verify that we received messages with the correct subscription identifiers
    // Sort the received sub_ids for easier comparison
    received_messages.sort();

    // Expected: None (no sub_id), Some(1), Some(2)
    assert_eq!(received_messages.len(), 3);
    assert_eq!(received_messages[0], None); // First subscription had no sub_id
    assert_eq!(received_messages[1], Some(1)); // Second subscription had sub_id 1
    assert_eq!(received_messages[2], Some(2)); // Third subscription had sub_id 2
}

/// Test: Protocol violation - Client sends PUBLISH with SubscriptionIdentifier
#[tokio::test]
async fn test_protocol_error_subscription_identifier_in_publish_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create publisher endpoint
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    // Try to publish with SubscriptionIdentifier property (Protocol Error)
    // MQTT spec: SubscriptionIdentifier is only sent from Server to Client, never from Client to Server
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/topic")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .payload(b"test message".to_vec())
        .props(vec![mqtt_ep::packet::Property::SubscriptionIdentifier(
            mqtt_ep::packet::SubscriptionIdentifier::new(123).unwrap(),
        )])
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Should receive DISCONNECT with ProtocolError
    let packet = publisher
        .recv()
        .await
        .expect("Failed to receive DISCONNECT");

    match packet {
        mqtt_ep::packet::Packet::V5_0Disconnect(disconnect) => {
            assert_eq!(
                disconnect.reason_code(),
                Some(mqtt_ep::result_code::DisconnectReasonCode::ProtocolError),
                "Expected ProtocolError for PUBLISH with SubscriptionIdentifier"
            );
            println!("✅ Correctly received DISCONNECT with ProtocolError");
        }
        _ => panic!("Expected DISCONNECT, got {packet:?}"),
    }

    // Connection should be closed after DISCONNECT
    let result = publisher.recv().await;
    assert!(
        result.is_err(),
        "Connection should be closed after ProtocolError"
    );
}
