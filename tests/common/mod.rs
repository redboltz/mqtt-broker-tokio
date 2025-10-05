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

use mqtt_endpoint_tokio::mqtt_ep;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;

pub const BROKER_PORT: u16 = 1883;

pub struct BrokerProcess {
    child: Child,
}

impl BrokerProcess {
    pub fn start() -> Self {
        let child = Command::new("cargo")
            .args(["run", "--", "--tcp-port", &BROKER_PORT.to_string()])
            .spawn()
            .expect("Failed to start broker");

        BrokerProcess { child }
    }

    pub async fn wait_ready() {
        // Wait for broker to start (max 30 seconds)
        for _ in 0..60 {
            if let Ok(stream) = TcpStream::connect(format!("127.0.0.1:{BROKER_PORT}")).await {
                drop(stream);
                // Wait a bit after connection check
                tokio::time::sleep(Duration::from_millis(100)).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("Broker failed to start within 30 seconds");
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub async fn create_connected_endpoint(
    client_id: &str,
) -> mqtt_ep::Endpoint<mqtt_ep::role::Client> {
    // Connect via TCP
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{BROKER_PORT}"),
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
