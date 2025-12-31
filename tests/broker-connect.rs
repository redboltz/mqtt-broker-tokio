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
use mqtt_endpoint_tokio::mqtt_ep::prelude::PropertyValueAccess;

#[tokio::test]
async fn test_connect_connack_disconnect() {
    let broker = BrokerProcess::start();

    // Wait for broker to be ready
    broker.wait_ready().await;

    // Connect via TCP
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    // Create Endpoint
    let endpoint = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);

    // Create TcpTransport
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    // Attach transport
    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build ConnectionOption");

    endpoint
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    // Create and send CONNECT packet
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

    // Create and send DISCONNECT packet
    let disconnect = mqtt_ep::packet::v5_0::Disconnect::builder()
        .reason_code(mqtt_ep::result_code::DisconnectReasonCode::NormalDisconnection)
        .build()
        .expect("Failed to build DISCONNECT");

    endpoint
        .send(disconnect)
        .await
        .expect("Failed to send DISCONNECT");

    // Verify that the connection is closed (next recv should return an error)
    let result = endpoint.recv().await;
    assert!(result.is_err(), "Connection should be closed");
}

#[tokio::test]
async fn test_auto_clientid_v3_1_1_clean_session_true() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let endpoint = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build ConnectionOption");

    endpoint
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    // Send CONNECT with empty ClientId and clean_session=true
    let connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("") // Empty ClientId
        .expect("Failed to set client_id")
        .clean_session(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    endpoint
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    // Should receive CONNACK with Accepted
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");

    match packet {
        mqtt_ep::packet::Packet::V3_1_1Connack(connack) => {
            assert_eq!(
                connack.return_code(),
                mqtt_ep::result_code::ConnectReturnCode::Accepted,
                "Expected Accepted for empty ClientId with clean_session=true"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_auto_clientid_v3_1_1_clean_session_false() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let endpoint = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V3_1_1);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build ConnectionOption");

    endpoint
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    // Send CONNECT with empty ClientId and clean_session=false
    let connect = mqtt_ep::packet::v3_1_1::Connect::builder()
        .client_id("") // Empty ClientId
        .expect("Failed to set client_id")
        .clean_session(false)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    endpoint
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    // Should receive CONNACK with IdentifierRejected
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");

    match packet {
        mqtt_ep::packet::Packet::V3_1_1Connack(connack) => {
            assert_eq!(
                connack.return_code(),
                mqtt_ep::result_code::ConnectReturnCode::IdentifierRejected,
                "Expected IdentifierRejected for empty ClientId with clean_session=false"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_auto_clientid_v5_0_assigned_client_identifier() {
    let broker = BrokerProcess::start();
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

    // Send CONNECT with empty ClientId
    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("") // Empty ClientId
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(60)
        .build()
        .expect("Failed to build CONNECT");

    endpoint
        .send(connect)
        .await
        .expect("Failed to send CONNECT");

    // Should receive CONNACK with Success and Assigned Client Identifier property
    let packet = endpoint.recv().await.expect("Failed to receive CONNACK");

    match packet {
        mqtt_ep::packet::Packet::V5_0Connack(connack) => {
            assert_eq!(
                connack.reason_code(),
                mqtt_ep::result_code::ConnectReasonCode::Success,
                "Expected Success for empty ClientId"
            );

            // Check for Assigned Client Identifier property
            let mut found_assigned_id = false;
            for prop in connack.props() {
                if let mqtt_ep::packet::Property::AssignedClientIdentifier(assigned_id) = prop {
                    found_assigned_id = true;
                    // The Display format includes JSON structure, extract the value
                    let display_str = format!("{assigned_id}");
                    // Parse the JSON-like output to get the actual value
                    // Format is: {"id": "assigned_client_identifier", "value": "auto-..."}
                    if let Some(value_start) = display_str.find(r#""value": ""#) {
                        let value_offset = value_start + r#""value": ""#.len();
                        if let Some(value_end) = display_str[value_offset..].find('"') {
                            let id_str = &display_str[value_offset..value_offset + value_end];
                            assert!(
                                id_str.starts_with("auto-"),
                                "Assigned ClientId should start with 'auto-', got: {id_str}"
                            );
                            println!("Assigned ClientId: {id_str}");
                        } else {
                            panic!("Failed to parse Assigned Client Identifier value");
                        }
                    } else {
                        panic!("Failed to find value in Assigned Client Identifier: {display_str}");
                    }
                }
            }
            assert!(
                found_assigned_id,
                "Expected Assigned Client Identifier property in CONNACK"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_server_keep_alive() {
    let broker = BrokerProcess::start_with_args(&["--mqtt-server-keep-alive", "2"]);

    // Wait for broker to be ready
    broker.wait_ready().await;

    // Connect via TCP
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    // Create Endpoint
    let endpoint = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);

    // Create TcpTransport
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    // Attach transport
    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build ConnectionOption");

    endpoint
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    // Create and send CONNECT packet with Keep Alive 3
    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("test_client")
        .expect("Failed to set client_id")
        .clean_start(true)
        .keep_alive(3) // Client requests Keep Alive 3
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

            // Check for Server Keep Alive property with value 2
            let mut found_server_keep_alive = false;
            for prop in connack.props() {
                if let mqtt_ep::packet::Property::ServerKeepAlive(_) = prop {
                    // Use the Property's as_u16 method to get the value
                    if let Some(value) = prop.as_u16() {
                        assert_eq!(
                            value, 2,
                            "Server Keep Alive should be 2 as specified in broker args"
                        );
                        found_server_keep_alive = true;
                        break;
                    }
                }
            }
            assert!(
                found_server_keep_alive,
                "Expected Server Keep Alive property in CONNACK"
            );
        }
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn test_unix_socket_connect() {
    let socket_path = "/tmp/mqtt_test_unix.sock";
    let broker = BrokerProcess::start_with_args(&["--unix-socket", socket_path]);

    // Wait for broker to be ready
    broker.wait_ready().await;

    // Connect via Unix socket
    let stream = mqtt_ep::transport::connect_helper::connect_unix(
        socket_path,
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker via Unix socket");

    // Create Endpoint
    let endpoint = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);

    // Create UnixStreamTransport
    let transport = mqtt_ep::transport::UnixStreamTransport::from_stream(stream);

    // Attach transport
    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build ConnectionOption");

    endpoint
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    // Create and send CONNECT packet
    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("unix_test_client")
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

    // Create and send DISCONNECT packet
    let disconnect = mqtt_ep::packet::v5_0::Disconnect::builder()
        .reason_code(mqtt_ep::result_code::DisconnectReasonCode::NormalDisconnection)
        .build()
        .expect("Failed to build DISCONNECT");

    endpoint
        .send(disconnect)
        .await
        .expect("Failed to send DISCONNECT");

    // Verify that the connection is closed
    let result = endpoint.recv().await;
    assert!(result.is_err(), "Connection should be closed");

    // Clean up socket file
    let _ = std::fs::remove_file(socket_path);
}
