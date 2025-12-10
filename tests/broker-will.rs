// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the software without restriction, including without limitation the rights
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

/// Test: Will message is sent on abnormal disconnect (v3.1.1)
#[tokio::test]
async fn test_will_abnormal_disconnect_v3_1_1() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts1 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts1)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_session(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    subscriber
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match subscriber.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    // Subscribe to will topic
    let packet_id1 = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("will/topic", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v3_1_1::Subscribe::builder()
        .packet_id(packet_id1)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");
    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match subscriber.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V3_1_1Suback(_) => {}
        _ => panic!("Expected SUBACK"),
    }

    // Create publisher endpoint with Will message
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts2 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts2)
        .await
        .expect("Failed to attach transport");

    let connect2 = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("publisher_will")
        .expect("Failed to set client_id")
        .clean_session(true)
        .keep_alive(60)
        .will_message(
            "will/topic",
            b"client died unexpectedly",
            mqtt_ep::packet::Qos::AtLeastOnce,
            false,
        )
        .expect("Failed to set will_message")
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher connected with Will message");

    // Close publisher connection abnormally (without sending DISCONNECT)
    publisher.close().await.expect("Failed to close publisher");

    println!("✅ Publisher closed abnormally (Will should be sent)");

    // Subscriber should receive Will message
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive Will PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V3_1_1Publish(publish) => {
            assert_eq!(publish.topic_name(), "will/topic");
            assert_eq!(publish.payload().as_slice(), b"client died unexpectedly");
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtLeastOnce);
            println!(
                "✅ Received Will message: {:?}",
                String::from_utf8_lossy(publish.payload().as_slice())
            );
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    println!("✅ Will message received successfully on abnormal disconnect");
}

/// Test: Will message is NOT sent on normal disconnect (v3.1.1)
#[tokio::test]
async fn test_will_normal_disconnect_v3_1_1() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts1 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts1)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_session(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    subscriber
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match subscriber.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    // Subscribe to will topic
    let packet_id1 = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("will/topic", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v3_1_1::Subscribe::builder()
        .packet_id(packet_id1)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");
    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match subscriber.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V3_1_1Suback(_) => {}
        _ => panic!("Expected SUBACK"),
    }

    // Create publisher endpoint with Will message
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts2 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts2)
        .await
        .expect("Failed to attach transport");

    let connect2 = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("publisher_will")
        .expect("Failed to set client_id")
        .clean_session(true)
        .keep_alive(60)
        .will_message(
            "will/topic",
            b"client died unexpectedly",
            mqtt_ep::packet::Qos::AtLeastOnce,
            false,
        )
        .expect("Failed to set will_message")
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V3_1_1Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher connected with Will message");

    // Send DISCONNECT packet (normal disconnect)
    let disconnect = mqtt_ep::packet::v3_1_1::Disconnect::builder()
        .build()
        .expect("Failed to build DISCONNECT");
    publisher
        .send(disconnect)
        .await
        .expect("Failed to send DISCONNECT");

    publisher.close().await.expect("Failed to close publisher");

    println!("✅ Publisher disconnected normally (Will should NOT be sent)");

    // Wait a bit to ensure no Will message is sent
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Subscriber should NOT receive Will message
    // Try to receive with timeout
    let result =
        tokio::time::timeout(tokio::time::Duration::from_millis(100), subscriber.recv()).await;

    if result.is_ok() {
        panic!("Received unexpected message (Will should not be sent on normal disconnect)");
    }

    println!("✅ No Will message received on normal disconnect (correct behavior)");
}

/// Test: Will message is sent on abnormal disconnect (v5.0)
#[tokio::test]
async fn test_will_abnormal_disconnect_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts1 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts1)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    subscriber
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match subscriber.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    // Subscribe to will topic
    let packet_id1 = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("will/topic", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id1)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");
    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match subscriber.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK"),
    }

    // Create publisher endpoint with Will message
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts2 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts2)
        .await
        .expect("Failed to attach transport");

    let connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_will")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .will_message(
            "will/topic",
            b"client died unexpectedly (v5.0)",
            mqtt_ep::packet::Qos::AtLeastOnce,
            false,
        )
        .expect("Failed to set will_message")
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher connected with Will message (v5.0)");

    // Close publisher connection abnormally (without sending DISCONNECT)
    publisher.close().await.expect("Failed to close publisher");

    println!("✅ Publisher closed abnormally (Will should be sent)");

    // Subscriber should receive Will message
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive Will PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "will/topic");
            assert_eq!(
                publish.payload().as_slice(),
                b"client died unexpectedly (v5.0)"
            );
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtLeastOnce);
            println!(
                "✅ Received Will message: {:?}",
                String::from_utf8_lossy(publish.payload().as_slice())
            );
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    println!("✅ Will message received successfully on abnormal disconnect (v5.0)");
}

/// Test: Will message is NOT sent on normal disconnect with reason code 0x00 (v5.0)
#[tokio::test]
async fn test_will_normal_disconnect_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts1 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts1)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    subscriber
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match subscriber.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    // Subscribe to will topic
    let packet_id1 = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("will/topic", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id1)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");
    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match subscriber.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK"),
    }

    // Create publisher endpoint with Will message
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts2 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts2)
        .await
        .expect("Failed to attach transport");

    let connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_will")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .will_message(
            "will/topic",
            b"client died unexpectedly (v5.0)",
            mqtt_ep::packet::Qos::AtLeastOnce,
            false,
        )
        .expect("Failed to set will_message")
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher connected with Will message (v5.0)");

    // Send DISCONNECT packet with reason code 0x00 (Normal disconnection)
    let disconnect = mqtt_ep::packet::v5_0::Disconnect::builder()
        .reason_code(mqtt_ep::result_code::DisconnectReasonCode::NormalDisconnection)
        .build()
        .expect("Failed to build DISCONNECT");
    publisher
        .send(disconnect)
        .await
        .expect("Failed to send DISCONNECT");

    publisher.close().await.expect("Failed to close publisher");

    println!("✅ Publisher disconnected normally with reason code 0x00 (Will should NOT be sent)");

    // Wait a bit to ensure no Will message is sent
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Subscriber should NOT receive Will message
    // Try to receive with timeout
    let result =
        tokio::time::timeout(tokio::time::Duration::from_millis(100), subscriber.recv()).await;

    if result.is_ok() {
        panic!("Received unexpected message (Will should not be sent on normal disconnect)");
    }

    println!(
        "✅ No Will message received on normal disconnect with reason code 0x00 (correct behavior)"
    );
}

/// Test: Will message IS sent with DisconnectWithWillMessage reason code (v5.0)
#[tokio::test]
async fn test_will_disconnect_with_will_message_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts1 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts1)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    subscriber
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match subscriber.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    // Subscribe to will topic
    let packet_id1 = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry =
        mqtt_ep::packet::SubEntry::new("will/topic", sub_opts).expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id1)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");
    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match subscriber.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK"),
    }

    // Create publisher endpoint with Will message
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts2 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts2)
        .await
        .expect("Failed to attach transport");

    let connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_will")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .will_message(
            "will/topic",
            b"Will sent with DisconnectWithWillMessage",
            mqtt_ep::packet::Qos::AtLeastOnce,
            false,
        )
        .expect("Failed to set will_message")
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher connected with Will message (v5.0)");

    // Send DISCONNECT packet with reason code 0x04 (DisconnectWithWillMessage)
    let disconnect = mqtt_ep::packet::v5_0::Disconnect::builder()
        .reason_code(mqtt_ep::result_code::DisconnectReasonCode::DisconnectWithWillMessage)
        .build()
        .expect("Failed to build DISCONNECT");
    publisher
        .send(disconnect)
        .await
        .expect("Failed to send DISCONNECT");

    publisher.close().await.expect("Failed to close publisher");

    println!("✅ Publisher disconnected with DisconnectWithWillMessage (Will SHOULD be sent)");

    // Subscriber should receive Will message
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive Will PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "will/topic");
            assert_eq!(
                publish.payload().as_slice(),
                b"Will sent with DisconnectWithWillMessage"
            );
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtLeastOnce);
            println!(
                "✅ Received Will message: {:?}",
                String::from_utf8_lossy(publish.payload().as_slice())
            );
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    println!("✅ Will message received successfully with DisconnectWithWillMessage reason code");
}

/// Test: Will message with retain flag is stored as retained message (v5.0)
#[tokio::test]
async fn test_will_retain_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create publisher endpoint with Will message (retain=true)
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts1 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts1)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_will_retain")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .will_message(
            "will/retained/topic",
            b"Will message with retain=true",
            mqtt_ep::packet::Qos::AtLeastOnce,
            true, // Retain flag set
        )
        .expect("Failed to set will_message")
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher connected with Will message (retain=true)");

    // Close publisher connection abnormally to trigger Will
    publisher.close().await.expect("Failed to close publisher");

    println!("✅ Publisher closed abnormally (Will with retain=true should be sent and stored)");

    // Wait for Will to be processed
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Create subscriber endpoint AFTER Will is sent
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts2 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts2)
        .await
        .expect("Failed to attach transport");

    let connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber_late")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    subscriber
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match subscriber.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    // Subscribe to will topic AFTER Will was sent
    let packet_id2 = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("will/retained/topic", sub_opts)
        .expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id2)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");
    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match subscriber.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK"),
    }

    // Subscriber should receive retained Will message
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive retained Will PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "will/retained/topic");
            assert_eq!(
                publish.payload().as_slice(),
                b"Will message with retain=true"
            );
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtLeastOnce);
            assert!(publish.retain(), "Retain flag should be set");
            println!(
                "✅ Received retained Will message: {:?}",
                String::from_utf8_lossy(publish.payload().as_slice())
            );
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    println!("✅ Will message with retain=true was stored and delivered as retained message");
}

/// Test: Will Delay Interval - timer fires and Will is sent (v5.0)
#[tokio::test]
async fn test_will_delay_interval_timer_fires_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts1 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts1)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    subscriber
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match subscriber.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    // Subscribe to will topic
    let packet_id1 = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("will/delay/topic", sub_opts)
        .expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id1)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");
    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match subscriber.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK"),
    }

    // Create publisher endpoint with Will message and Will Delay Interval
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts2 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts2)
        .await
        .expect("Failed to attach transport");

    // Build CONNECT with Will Delay Interval property
    let will_delay_interval =
        mqtt_ep::packet::WillDelayInterval::new(2).expect("Failed to create WillDelayInterval");
    let will_delay_prop = mqtt_ep::packet::Property::WillDelayInterval(will_delay_interval);

    let session_expiry_interval = mqtt_ep::packet::SessionExpiryInterval::new(10)
        .expect("Failed to create SessionExpiryInterval");
    let session_expiry_prop =
        mqtt_ep::packet::Property::SessionExpiryInterval(session_expiry_interval);

    let connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_will_delay")
        .expect("Failed to set client_id")
        .clean_start(false) // Session will be kept
        .keep_alive(60)
        .props(vec![session_expiry_prop])
        .will_message(
            "will/delay/topic",
            b"delayed will message",
            mqtt_ep::packet::Qos::AtLeastOnce,
            false,
        )
        .expect("Failed to set will_message")
        .will_props(vec![will_delay_prop])
        .build()
        .expect("Failed to build CONNECT");
    publisher
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher connected with Will message and 2s delay");

    // Close publisher connection abnormally (without sending DISCONNECT)
    publisher.close().await.expect("Failed to close publisher");

    println!("✅ Publisher closed abnormally, Will Delay Interval timer should start");

    // Wait for Will Delay Interval to expire (2 seconds + some margin)
    tokio::time::sleep(tokio::time::Duration::from_millis(2200)).await;

    // Now subscriber should receive Will message
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive delayed Will PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "will/delay/topic");
            assert_eq!(publish.payload().as_slice(), b"delayed will message");
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtLeastOnce);
            println!(
                "✅ Received delayed Will message: {:?}",
                String::from_utf8_lossy(publish.payload().as_slice())
            );
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    println!("✅ Will Delay Interval timer fired and Will was sent");
}

/// Test: Will Delay Interval - reconnect with CleanStart:0 cancels Will (v5.0)
#[tokio::test]
async fn test_will_delay_interval_reconnect_cancel_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts1 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts1)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    subscriber
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match subscriber.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    // Subscribe to will topic
    let packet_id1 = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("will/cancel/topic", sub_opts)
        .expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id1)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");
    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match subscriber.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK"),
    }

    // Create first publisher endpoint with Will message and Will Delay Interval
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts2 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher1
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts2)
        .await
        .expect("Failed to attach transport");

    // Build CONNECT with Will Delay Interval property
    let will_delay_interval =
        mqtt_ep::packet::WillDelayInterval::new(3).expect("Failed to create WillDelayInterval");
    let will_delay_prop = mqtt_ep::packet::Property::WillDelayInterval(will_delay_interval);

    let session_expiry_interval = mqtt_ep::packet::SessionExpiryInterval::new(10)
        .expect("Failed to create SessionExpiryInterval");
    let session_expiry_prop =
        mqtt_ep::packet::Property::SessionExpiryInterval(session_expiry_interval);

    let connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_will_cancel")
        .expect("Failed to set client_id")
        .clean_start(false) // Session will be kept
        .keep_alive(60)
        .props(vec![session_expiry_prop])
        .will_message(
            "will/cancel/topic",
            b"this will should be cancelled",
            mqtt_ep::packet::Qos::AtLeastOnce,
            false,
        )
        .expect("Failed to set will_message")
        .will_props(vec![will_delay_prop])
        .build()
        .expect("Failed to build CONNECT");
    publisher1
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher1.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher connected with Will message and 3s delay");

    // Close publisher connection abnormally
    publisher1.close().await.expect("Failed to close publisher");

    println!("✅ Publisher closed abnormally, Will Delay Interval timer should start");

    // Wait a bit, then reconnect with CleanStart:0 BEFORE timer fires
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Reconnect with same client_id and CleanStart:0
    let stream3 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport3 = mqtt_ep::transport::TcpTransport::from_stream(stream3);

    let opts3 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher2
        .attach_with_options(transport3, mqtt_ep::Mode::Client, opts3)
        .await
        .expect("Failed to attach transport");

    let connect3 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_will_cancel")
        .expect("Failed to set client_id")
        .clean_start(false) // Reconnect with existing session
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    publisher2
        .send(connect3)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher2.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert!(connack.session_present(), "Session should be present");
        }
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher reconnected with CleanStart:0, Will should be cancelled");

    // Wait beyond the original Will Delay Interval
    tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;

    // Subscriber should NOT receive Will message (it was cancelled by reconnect)
    let recv_result =
        tokio::time::timeout(tokio::time::Duration::from_millis(500), subscriber.recv()).await;

    assert!(recv_result.is_err(), "Should not receive cancelled Will");
    println!("✅ Will was cancelled by reconnect with CleanStart:0 (correct)");
}

/// Test: Will Delay Interval - reconnect with CleanStart:1 sends Will immediately (v5.0)
#[tokio::test]
async fn test_will_delay_interval_reconnect_cleanstart1_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts1 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts1)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    subscriber
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match subscriber.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    // Subscribe to will topic
    let packet_id1 = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("will/cleanstart1/topic", sub_opts)
        .expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id1)
        .entries(vec![sub_entry])
        .build()
        .expect("Failed to build SUBSCRIBE");
    subscriber
        .send(subscribe)
        .await
        .expect("Failed to send SUBSCRIBE");

    // Receive SUBACK
    match subscriber.recv().await.expect("Failed to receive SUBACK") {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK"),
    }

    // Create first publisher endpoint with Will message and Will Delay Interval
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts2 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher1
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts2)
        .await
        .expect("Failed to attach transport");

    // Build CONNECT with Will Delay Interval property
    let will_delay_interval =
        mqtt_ep::packet::WillDelayInterval::new(10).expect("Failed to create WillDelayInterval");
    let will_delay_prop = mqtt_ep::packet::Property::WillDelayInterval(will_delay_interval);

    let session_expiry_interval = mqtt_ep::packet::SessionExpiryInterval::new(30)
        .expect("Failed to create SessionExpiryInterval");
    let session_expiry_prop =
        mqtt_ep::packet::Property::SessionExpiryInterval(session_expiry_interval);

    let connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_will_cleanstart1")
        .expect("Failed to set client_id")
        .clean_start(false) // Session will be kept
        .keep_alive(60)
        .props(vec![session_expiry_prop])
        .will_message(
            "will/cleanstart1/topic",
            b"will sent by cleanstart1 reconnect",
            mqtt_ep::packet::Qos::AtLeastOnce,
            false,
        )
        .expect("Failed to set will_message")
        .will_props(vec![will_delay_prop])
        .build()
        .expect("Failed to build CONNECT");
    publisher1
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher1.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher connected with Will message and 10s delay");

    // Close publisher connection abnormally
    publisher1.close().await.expect("Failed to close publisher");

    println!("✅ Publisher closed abnormally, Will Delay Interval timer should start");

    // Wait a bit, then reconnect with CleanStart:1 BEFORE timer fires
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Reconnect with same client_id and CleanStart:1 (deletes session)
    let stream3 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport3 = mqtt_ep::transport::TcpTransport::from_stream(stream3);

    let opts3 = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher2
        .attach_with_options(transport3, mqtt_ep::Mode::Client, opts3)
        .await
        .expect("Failed to attach transport");

    let connect3 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher_will_cleanstart1")
        .expect("Failed to set client_id")
        .clean_start(true) // Reconnect with CleanStart:1 (deletes old session)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");
    publisher2
        .send(connect3)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    match publisher2.recv().await.expect("Failed to receive CONNACK") {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert!(!connack.session_present(), "Session should NOT be present");
        }
        _ => panic!("Expected CONNACK"),
    }

    println!("✅ Publisher reconnected with CleanStart:1, Will should be sent immediately");

    // Subscriber should receive Will message immediately (not after 10s delay)
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive Will PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "will/cleanstart1/topic");
            assert_eq!(
                publish.payload().as_slice(),
                b"will sent by cleanstart1 reconnect"
            );
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtLeastOnce);
            println!(
                "✅ Received Will message immediately: {:?}",
                String::from_utf8_lossy(publish.payload().as_slice())
            );
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    println!("✅ Will was sent immediately on CleanStart:1 reconnect (before delay expired)");
}
