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
