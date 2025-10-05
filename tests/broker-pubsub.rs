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

#[tokio::test]
async fn test_pubsub_qos0() {
    let _broker = BrokerProcess::start();

    // Wait for broker to be ready
    BrokerProcess::wait_ready().await;

    // Create subscriber endpoint
    let subscriber = create_connected_endpoint("subscriber").await;

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
    let publisher = create_connected_endpoint("publisher").await;

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
            // Verify payload length matches
            assert_eq!(publish.payload().len(), 8);
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtMostOnce);
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}
