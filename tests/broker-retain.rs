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

use common::{create_connected_endpoint, BrokerProcess};
use mqtt_endpoint_tokio::mqtt_ep;

#[tokio::test]
async fn test_retained_qos0_v3_1_1() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

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

    // Publish retained message with QoS 0
    let publish = mqtt_ep::packet::v3_1_1::Publish::builder()
        .topic_name("retain/test")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true)
        .payload("retained payload")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Wait a bit for the message to be stored
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

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

    // Subscribe to topic
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("retain/test", sub_opts)
        .expect("Failed to create SubEntry");
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

    // Receive retained message
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive retained PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V3_1_1Publish(publish) => {
            assert_eq!(publish.topic_name(), "retain/test");
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtMostOnce);
            assert_eq!(publish.retain(), true);
            assert_eq!(publish.payload().as_slice(), b"retained payload");
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_retained_qos1_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create publisher endpoint (v5.0)
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let publisher = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    publisher
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("publisher")
        .expect("Failed to set client_id")
        .clean_start(true)
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
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Publish retained message with QoS 1
    let packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("retain/qos1")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(packet_id)
        .retain(true)
        .payload("qos1 retained")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Receive PUBACK
    let packet = publisher.recv().await.expect("Failed to receive PUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Puback(_) => {}
        _ => panic!("Expected PUBACK, got {packet:?}"),
    }

    // Wait a bit for the message to be stored
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create subscriber endpoint (v5.0)
    let stream = mqtt_ep::transport::connect_helper::connect_tcp(
        &format!("127.0.0.1:{}", broker.port()),
        Some(tokio::time::Duration::from_secs(10)),
    )
    .await
    .expect("Failed to connect to broker");

    let subscriber = mqtt_ep::Endpoint::<mqtt_ep::role::Client>::new(mqtt_ep::Version::V5_0);
    let transport = mqtt_ep::transport::TcpTransport::from_stream(stream);

    let opts = mqtt_ep::connection_option::ConnectionOption::builder()
        .build()
        .expect("Failed to build connection options");
    subscriber
        .attach_with_options(transport, mqtt_ep::Mode::Client, opts)
        .await
        .expect("Failed to attach transport");

    let connect = mqtt_ep::packet::v5_0::Connect::builder()
        .client_id("subscriber")
        .expect("Failed to set client_id")
        .clean_start(true)
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
        mqtt_ep::packet::Packet::V5_0Connack(_) => {}
        _ => panic!("Expected CONNACK, got {packet:?}"),
    }

    // Subscribe to topic with QoS 0 (QoS downgrade test)
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("retain/qos1", sub_opts)
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

    // Receive retained message (QoS should be downgraded to 0)
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive retained PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "retain/qos1");
            // QoS arbitration: min(retained QoS 1, subscription QoS 0) = QoS 0
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtMostOnce);
            assert_eq!(publish.retain(), true);
            assert_eq!(publish.payload().as_slice(), b"qos1 retained");
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_retained_qos2_qos_arbitration_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber endpoint first (to avoid NoMatchingSubscribers)
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    // Subscribe to topic with QoS 2
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::ExactlyOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("retain/qos2", sub_opts)
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

    // Receive SUBACK
    let _packet = subscriber.recv().await.expect("Failed to receive SUBACK");

    // Create publisher endpoint (v5.0)
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    // Publish retained message with QoS 2
    let packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("retain/qos2")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::ExactlyOnce)
        .packet_id(packet_id)
        .retain(true)
        .payload("qos2 retained")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Receive PUBREC
    let packet = publisher.recv().await.expect("Failed to receive PUBREC");
    let pubrec_packet_id = match packet {
        mqtt_ep::packet::Packet::V5_0Pubrec(pubrec) => {
            assert_eq!(pubrec.packet_id(), packet_id);
            pubrec.packet_id()
        }
        _ => panic!("Expected PUBREC, got {packet:?}"),
    };

    // Send PUBREL
    let pubrel = mqtt_ep::packet::v5_0::Pubrel::builder()
        .packet_id(pubrec_packet_id)
        .reason_code(mqtt_ep::result_code::PubrelReasonCode::Success)
        .build()
        .expect("Failed to build PUBREL");
    publisher
        .send(pubrel)
        .await
        .expect("Failed to send PUBREL");

    // Receive PUBCOMP
    let packet = publisher.recv().await.expect("Failed to receive PUBCOMP");
    match packet {
        mqtt_ep::packet::Packet::V5_0Pubcomp(_) => {}
        _ => panic!("Expected PUBCOMP, got {packet:?}"),
    }

    // Subscriber should receive the published message (QoS 2)
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH on subscriber");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "retain/qos2");
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::ExactlyOnce);
            assert_eq!(publish.retain(), false); // retain flag is false for forwarded messages (default RAP)
            assert_eq!(publish.payload().as_slice(), b"qos2 retained");

            // Send PUBREC for the received message
            let packet_id = publish.packet_id().unwrap();
            let pubrec = mqtt_ep::packet::v5_0::Pubrec::builder()
                .packet_id(packet_id)
                .reason_code(mqtt_ep::result_code::PubrecReasonCode::Success)
                .build()
                .expect("Failed to build PUBREC");
            subscriber.send(pubrec).await.expect("Failed to send PUBREC");

            // Receive PUBREL
            let packet = subscriber.recv().await.expect("Failed to receive PUBREL");
            match packet {
                mqtt_ep::packet::Packet::V5_0Pubrel(pubrel) => {
                    let pubcomp = mqtt_ep::packet::v5_0::Pubcomp::builder()
                        .packet_id(pubrel.packet_id())
                        .reason_code(mqtt_ep::result_code::PubcompReasonCode::Success)
                        .build()
                        .expect("Failed to build PUBCOMP");
                    subscriber.send(pubcomp).await.expect("Failed to send PUBCOMP");
                }
                _ => panic!("Expected PUBREL, got {packet:?}"),
            }
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    // Now disconnect and reconnect subscriber with QoS 1 subscription to test retained message
    drop(subscriber);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Reconnect subscriber with QoS 1 (QoS downgrade test)
    let subscriber = create_connected_endpoint("subscriber2", broker.port()).await;
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("retain/qos2", sub_opts)
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

    // Receive retained message (QoS should be downgraded to 1)
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive retained PUBLISH");
    let retained_packet_id = match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "retain/qos2");
            // QoS arbitration: min(retained QoS 2, subscription QoS 1) = QoS 1
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtLeastOnce);
            assert_eq!(publish.retain(), true);
            assert_eq!(publish.payload().as_slice(), b"qos2 retained");
            publish.packet_id().expect("QoS 1 should have packet_id")
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    };

    // Send PUBACK for the retained message
    let puback = mqtt_ep::packet::v5_0::Puback::builder()
        .packet_id(retained_packet_id)
        .build()
        .expect("Failed to build PUBACK");
    subscriber
        .send(puback)
        .await
        .expect("Failed to send PUBACK");
}

#[tokio::test]
async fn test_retained_message_overwrite_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create publisher endpoint
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    // Publish first retained message
    let publish1 = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("retain/update")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true)
        .payload("original message")
        .props(vec![mqtt_ep::packet::Property::UserProperty(
            mqtt_ep::packet::UserProperty::new("key1", "value1").unwrap(),
        )])
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish1)
        .await
        .expect("Failed to send first PUBLISH");

    // Wait for storage
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Publish second retained message with different payload, QoS, and props
    let packet_id = publisher
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let publish2 = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("retain/update")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtLeastOnce)
        .packet_id(packet_id)
        .retain(true)
        .payload("updated message")
        .props(vec![mqtt_ep::packet::Property::UserProperty(
            mqtt_ep::packet::UserProperty::new("key2", "value2").unwrap(),
        )])
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish2)
        .await
        .expect("Failed to send second PUBLISH");

    // Receive PUBACK for QoS 1
    let _packet = publisher.recv().await.expect("Failed to receive PUBACK");

    // Wait for storage
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create subscriber to verify the retained message was updated
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    // Subscribe to topic
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtLeastOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("retain/update", sub_opts)
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

    // Receive SUBACK
    let _packet = subscriber.recv().await.expect("Failed to receive SUBACK");

    // Receive retained message - should be the UPDATED one
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive retained PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "retain/update");
            assert_eq!(publish.qos(), mqtt_ep::packet::Qos::AtLeastOnce);
            assert_eq!(publish.retain(), true);
            assert_eq!(publish.payload().as_slice(), b"updated message");

            // Verify properties were updated
            let props = publish.props();
            let user_props: Vec<_> = props
                .iter()
                .filter_map(|p| match p {
                    mqtt_ep::packet::Property::UserProperty(up) => Some(up),
                    _ => None,
                })
                .collect();
            assert_eq!(user_props.len(), 1);
            assert_eq!(user_props[0].key(), "key2");
            assert_eq!(user_props[0].val(), "value2");

            // Send PUBACK
            let packet_id = publish.packet_id().expect("QoS 1 should have packet_id");
            let puback = mqtt_ep::packet::v5_0::Puback::builder()
                .packet_id(packet_id)
                .build()
                .expect("Failed to build PUBACK");
            subscriber
                .send(puback)
                .await
                .expect("Failed to send PUBACK");
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_retained_message_clear_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create publisher endpoint
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    // Publish retained message
    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("retain/clear")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true)
        .payload("message to be cleared")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Wait for storage
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Clear retained message by sending empty payload with retain=true
    let clear_publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("retain/clear")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true)
        .payload(vec![]) // Empty payload
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(clear_publish)
        .await
        .expect("Failed to send clear PUBLISH");

    // Wait for removal
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create subscriber to verify retained message was removed
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    // Subscribe to topic
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("retain/clear", sub_opts)
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

    // Receive SUBACK
    let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Try to receive with timeout - should NOT receive any retained message
    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        subscriber.recv()
    )
    .await;

    match result {
        Ok(_) => panic!("Should not receive any retained message after clearing"),
        Err(_) => {
            // Timeout is expected - no retained message should be sent
        }
    }
}

#[tokio::test]
async fn test_retained_not_sent_to_shared_subscription_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create publisher and publish retained message
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("retain/shared")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true)
        .payload("retained for shared test")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Wait for storage
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create subscriber with shared subscription ($share/)
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new().set_qos(mqtt_ep::packet::Qos::AtMostOnce);
    let sub_entry = mqtt_ep::packet::SubEntry::new("$share/group1/retain/shared", sub_opts)
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

    // Receive SUBACK
    let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Try to receive with timeout - should NOT receive retained message for shared subscription
    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        subscriber.recv()
    )
    .await;

    match result {
        Ok(_) => panic!("Should not receive retained message for shared subscription"),
        Err(_) => {
            // Timeout is expected - no retained message for shared subscription
        }
    }
}

#[tokio::test]
async fn test_retained_rh_do_not_send_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create publisher and publish retained message
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("retain/rh")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true)
        .payload("retained for rh test")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Wait for storage
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create subscriber with RH=2 (DoNotSendRetained)
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new()
        .set_qos(mqtt_ep::packet::Qos::AtMostOnce)
        .set_rh(mqtt_ep::packet::RetainHandling::DoNotSendRetained);
    let sub_entry = mqtt_ep::packet::SubEntry::new("retain/rh", sub_opts)
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

    // Receive SUBACK
    let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Try to receive with timeout - should NOT receive retained message with RH=2
    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        subscriber.recv()
    )
    .await;

    match result {
        Ok(_) => panic!("Should not receive retained message with RH=2"),
        Err(_) => {
            // Timeout is expected - no retained message with RH=2
        }
    }
}

#[tokio::test]
async fn test_retained_rh_send_on_new_subscription_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create publisher and publish retained message
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("retain/rh1")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true)
        .payload("retained for rh=1 test")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Wait for storage
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create subscriber with RH=1 (SendRetainedOnNewSubscription)
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    // First subscription with RH=1 - should receive retained message
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new()
        .set_qos(mqtt_ep::packet::Qos::AtMostOnce)
        .set_rh(mqtt_ep::packet::RetainHandling::SendRetainedIfNotExists);
    let sub_entry = mqtt_ep::packet::SubEntry::new("retain/rh1", sub_opts)
        .expect("Failed to create SubEntry");
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

    // Should receive retained message on first subscription
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive retained PUBLISH on first subscription");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "retain/rh1");
            assert_eq!(publish.retain(), true);
            assert_eq!(publish.payload().as_slice(), b"retained for rh=1 test");
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }

    // Second subscription with RH=1 - should NOT receive retained message (subscription already exists)
    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new()
        .set_qos(mqtt_ep::packet::Qos::AtMostOnce)
        .set_rh(mqtt_ep::packet::RetainHandling::SendRetainedIfNotExists);
    let sub_entry = mqtt_ep::packet::SubEntry::new("retain/rh1", sub_opts)
        .expect("Failed to create SubEntry");
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(vec![sub_entry])
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

    // Try to receive with timeout - should NOT receive retained message on re-subscription
    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        subscriber.recv()
    )
    .await;

    match result {
        Ok(_) => panic!("Should not receive retained message on re-subscription with RH=1"),
        Err(_) => {
            // Timeout is expected - no retained message on re-subscription with RH=1
        }
    }
}

#[tokio::test]
async fn test_rap_false_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber with RAP=false (default)
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new()
        .set_qos(mqtt_ep::packet::Qos::AtMostOnce)
        .set_rap(false); // RAP=false: retain flag should be cleared when forwarding
    let sub_entry = mqtt_ep::packet::SubEntry::new("test/rap", sub_opts)
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

    // Receive SUBACK
    let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Create publisher and publish with retain=true
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/rap")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true) // Original retain flag is true
        .payload("rap false test")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Subscriber should receive message with retain=false (because RAP=false)
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "test/rap");
            assert_eq!(publish.retain(), false); // retain should be false due to RAP=false
            assert_eq!(publish.payload().as_slice(), b"rap false test");
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}

#[tokio::test]
async fn test_rap_true_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create subscriber with RAP=true
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new()
        .set_qos(mqtt_ep::packet::Qos::AtMostOnce)
        .set_rap(true); // RAP=true: retain flag should be preserved when forwarding
    let sub_entry = mqtt_ep::packet::SubEntry::new("test/rap/true", sub_opts)
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

    // Receive SUBACK
    let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Create publisher and publish with retain=true
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    let publish = mqtt_ep::packet::v5_0::Publish::builder()
        .topic_name("test/rap/true")
        .expect("Failed to set topic_name")
        .qos(mqtt_ep::packet::Qos::AtMostOnce)
        .retain(true) // Original retain flag is true
        .payload("rap true test")
        .build()
        .expect("Failed to build PUBLISH");

    publisher
        .send(publish)
        .await
        .expect("Failed to send PUBLISH");

    // Subscriber should receive message with retain=true (because RAP=true)
    let packet = subscriber
        .recv()
        .await
        .expect("Failed to receive PUBLISH");
    match packet {
        mqtt_ep::packet::Packet::V5_0Publish(publish) => {
            assert_eq!(publish.topic_name(), "test/rap/true");
            assert_eq!(publish.retain(), true); // retain should be true due to RAP=true
            assert_eq!(publish.payload().as_slice(), b"rap true test");
        }
        _ => panic!("Expected PUBLISH, got {packet:?}"),
    }
}
/// Test: Single wildcard subscription matching multiple retained messages
#[tokio::test]
async fn test_retained_wildcard_single_entry_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create publisher and publish multiple retained messages
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    // Publish 3 retained messages on different topics
    for (i, topic) in ["sensor/temp", "sensor/humidity", "sensor/pressure"]
        .iter()
        .enumerate()
    {
        let publish = mqtt_ep::packet::v5_0::Publish::builder()
            .topic_name(topic)
            .expect("Failed to set topic_name")
            .qos(mqtt_ep::packet::Qos::AtMostOnce)
            .retain(true)
            .payload(format!("value{i}"))
            .build()
            .expect("Failed to build PUBLISH");

        publisher
            .send(publish)
            .await
            .expect("Failed to send PUBLISH");
    }

    // Small delay to ensure messages are stored
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Create subscriber with wildcard subscription
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new()
        .set_qos(mqtt_ep::packet::Qos::AtMostOnce)
        .set_rap(false);
    let sub_entry = mqtt_ep::packet::SubEntry::new("sensor/#", sub_opts)
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

    // Receive SUBACK
    let packet = subscriber.recv().await.expect("Failed to receive SUBACK");
    match packet {
        mqtt_ep::packet::Packet::V5_0Suback(_) => {}
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Subscriber should receive all 3 retained messages
    let mut received_topics = std::collections::HashSet::new();
    for _ in 0..3 {
        let packet = subscriber
            .recv()
            .await
            .expect("Failed to receive PUBLISH");
        match packet {
            mqtt_ep::packet::Packet::V5_0Publish(publish) => {
                assert_eq!(publish.retain(), true);
                received_topics.insert(publish.topic_name().to_string());
            }
            _ => panic!("Expected PUBLISH, got {packet:?}"),
        }
    }

    assert_eq!(received_topics.len(), 3);
    assert!(received_topics.contains("sensor/temp"));
    assert!(received_topics.contains("sensor/humidity"));
    assert!(received_topics.contains("sensor/pressure"));
}

/// Test: Multiple wildcard subscriptions matching different retained messages
#[tokio::test]
async fn test_retained_wildcard_multiple_entries_v5_0() {
    let broker = BrokerProcess::start();
    broker.wait_ready().await;

    // Create publisher and publish multiple retained messages on different topic hierarchies
    let publisher = create_connected_endpoint("publisher", broker.port()).await;

    // Publish retained messages on various topics
    let topics = [
        "home/living/temp",
        "home/living/humidity",
        "home/bedroom/temp",
        "office/desk/light",
        "office/desk/power",
    ];

    for (i, topic) in topics.iter().enumerate() {
        let publish = mqtt_ep::packet::v5_0::Publish::builder()
            .topic_name(topic)
            .expect("Failed to set topic_name")
            .qos(mqtt_ep::packet::Qos::AtMostOnce)
            .retain(true)
            .payload(format!("data{i}"))
            .build()
            .expect("Failed to build PUBLISH");

        publisher
            .send(publish)
            .await
            .expect("Failed to send PUBLISH");
    }

    // Small delay to ensure messages are stored
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Create subscriber with multiple wildcard subscriptions
    let subscriber = create_connected_endpoint("subscriber", broker.port()).await;

    let packet_id = subscriber
        .acquire_packet_id()
        .await
        .expect("Failed to acquire packet_id");
    let sub_opts = mqtt_ep::packet::SubOpts::new()
        .set_qos(mqtt_ep::packet::Qos::AtMostOnce)
        .set_rap(false);
    
    // Subscribe to two different wildcard patterns in one SUBSCRIBE packet
    let entries = vec![
        mqtt_ep::packet::SubEntry::new("home/#", sub_opts).expect("Failed to create SubEntry"),
        mqtt_ep::packet::SubEntry::new("office/desk/+", sub_opts).expect("Failed to create SubEntry"),
    ];
    
    let subscribe = mqtt_ep::packet::v5_0::Subscribe::builder()
        .packet_id(packet_id)
        .entries(entries)
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
            assert_eq!(suback.reason_codes().len(), 2); // 2 subscription entries
        }
        _ => panic!("Expected SUBACK, got {packet:?}"),
    }

    // Subscriber should receive all matching retained messages:
    // - home/# matches: home/living/temp, home/living/humidity, home/bedroom/temp (3 messages)
    // - office/desk/+ matches: office/desk/light, office/desk/power (2 messages)
    // Total: 5 messages
    let mut received_topics = std::collections::HashSet::new();
    for _ in 0..5 {
        let packet = subscriber
            .recv()
            .await
            .expect("Failed to receive PUBLISH");
        match packet {
            mqtt_ep::packet::Packet::V5_0Publish(publish) => {
                assert_eq!(publish.retain(), true);
                received_topics.insert(publish.topic_name().to_string());
            }
            _ => panic!("Expected PUBLISH, got {packet:?}"),
        }
    }

    assert_eq!(received_topics.len(), 5);
    // Check home/# matches
    assert!(received_topics.contains("home/living/temp"));
    assert!(received_topics.contains("home/living/humidity"));
    assert!(received_topics.contains("home/bedroom/temp"));
    // Check office/desk/+ matches
    assert!(received_topics.contains("office/desk/light"));
    assert!(received_topics.contains("office/desk/power"));
}
