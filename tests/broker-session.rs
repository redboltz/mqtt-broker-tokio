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

/// Test: Basic session persistence with clean_start=false
#[tokio::test]
async fn test_session_persistence_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // First connection with clean_start=true (create new session)
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let client1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    client1
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_client")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(
            mqtt_ep::packet::SessionExpiryInterval::new(300).unwrap(),
        )])
        .build()
        .expect("Failed to build CONNECT");

    client1
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = client1.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            // clean_start=true should result in session_present=false
            assert_eq!(connack.session_present(), false);
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Disconnect
    client1.close().await.expect("Failed to close connection");

    // Small delay to ensure disconnect is processed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Second connection with clean_start=false (should restore session)
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let client2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    client2
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_client")
        .expect("Failed to set client_id")
        .clean_start(false)
        .keep_alive(60)
        .props(vec![mqtt_ep::packet::Property::SessionExpiryInterval(
            mqtt_ep::packet::SessionExpiryInterval::new(300).unwrap(),
        )])
        .build()
        .expect("Failed to build CONNECT");

    client2
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = client2.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            // clean_start=false should result in session_present=true (session was preserved)
            assert_eq!(connack.session_present(), true);
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}
/// Test: Session deletion when SessionExpiryInterval=0
#[tokio::test]
async fn test_session_deletion_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // First connection with SessionExpiryInterval=0 (session should be deleted on disconnect)
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let client1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    client1
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_delete")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        // No SessionExpiryInterval property (defaults to 0)
        .build()
        .expect("Failed to build CONNECT");

    client1
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = client1.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(connack.session_present(), false);
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Disconnect
    client1.close().await.expect("Failed to close connection");

    // Small delay to ensure disconnect is processed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Second connection - session should NOT be present (was deleted)
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let client2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    client2
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect2 = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_delete")
        .expect("Failed to set client_id")
        .clean_start(false)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    client2
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = client2.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            // Session should NOT be present (was deleted because SessionExpiryInterval=0)
            assert_eq!(connack.session_present(), false);
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

/// Test: v3.1.1 session persistence with clean_session=false
#[tokio::test]
async fn test_session_persistence_v3_1_1() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // First connection with clean_session=false
    let stream1 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let client1 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport1 = mqtt_ep::transport::TcpTransport::from_stream(stream1);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    client1
        .attach_with_options(transport1, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect1 = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("test_v311")
        .expect("Failed to set client_id")
        .clean_session(false)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    client1
        .send(connect1)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = client1.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V3_1_1Connack(connack) => {
            // First connection: session_present=false
            assert_eq!(connack.session_present(), false);
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Disconnect
    client1.close().await.expect("Failed to close connection");

    // Small delay to ensure disconnect is processed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Second connection with clean_session=false
    let stream2 = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let client2 = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport2 = mqtt_ep::transport::TcpTransport::from_stream(stream2);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    client2
        .attach_with_options(transport2, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect2 = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("test_v311")
        .expect("Failed to set client_id")
        .clean_session(false)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    client2
        .send(connect2)
        .await
        .expect("Failed to send CONNECT");

    // Receive CONNACK
    let packet = client2.recv().await.expect("Failed to receive CONNACK");
    match packet {
        mqtt_ep::packet::Packet::V3_1_1Connack(connack) => {
            // Session should be present (persisted because clean_session=false)
            assert_eq!(connack.session_present(), true);
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}
