# mqtt-broker-tokio

[![CI](https://github.com/redboltz/mqtt-broker-tokio/workflows/CI/badge.svg)](https://github.com/redboltz/mqtt-broker-tokio/actions)
[![codecov](https://codecov.io/gh/redboltz/mqtt-broker-tokio/branch/main/graph/badge.svg)](https://codecov.io/gh/redboltz/mqtt-broker-tokio)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Impotant notice: This software is under construction. Full functionalities below are NOT implemented yet.

A high-performance async MQTT broker implementation built with tokio and [mqtt-endpoint-tokio](https://github.com/redboltz/mqtt-endpoint-tokio).

## Features

- **Full MQTT Protocol Support**: MQTT v5.0 and v3.1.1 compatible
- **Multiple Transport Layers**: TCP, TLS, WebSocket, and WebSocket over TLS
- **High Performance**: Built on tokio for async I/O with configurable worker threads
- **Subscription Management**: Efficient topic filtering and subscription handling
- **Security**: TLS support with configurable certificates
- **Flexible Configuration**: Command-line configuration for ports and certificates

## Quick Start

### Basic Usage

```bash
# TCP only (port 1883)
./mqtt-broker --tcp-port 1883

# Unix socket (Unix/Linux only)
./mqtt-broker --unix-socket /tmp/mqtt.sock

# Multiple transports
./mqtt-broker --tcp-port 1883 --ws-port 8080 --unix-socket /tmp/mqtt.sock

# With TLS (requires certificates)
./mqtt-broker --tcp-port 1883 --tls-port 8883 \
  --server-crt server.crt --server-key server.key

# All transports with TLS
./mqtt-broker --tcp-port 1883 --tls-port 8883 \
  --ws-port 8080 --ws-tls-port 8443 \
  --server-crt server.crt --server-key server.key
```

### Command Line Options

```
Usage: mqtt-broker [OPTIONS]

Tokio Runtime Options:
      --worker-threads <WORKER_THREADS>
          Number of worker threads for async tasks
      --max-blocking-threads <MAX_BLOCKING_THREADS>
          Number of blocking threads for blocking operations
      --thread-keep-alive <THREAD_KEEP_ALIVE>
          Enable thread keep alive for worker threads [possible values: true, false]
      --thread-stack-size <THREAD_STACK_SIZE>
          Thread stack size in bytes
      --global-queue-interval <GLOBAL_QUEUE_INTERVAL>
          Global queue interval for work stealing (microseconds)
      --event-interval <EVENT_INTERVAL>
          Event loop interval for polling (microseconds)

Logging:
      --log-level <LOG_LEVEL>
          Log level [default: info]
          [possible values: error, warn, info, debug, trace]

Network Ports:
      --tcp-port <TCP_PORT>
          TCP port for plain MQTT
      --tls-port <TLS_PORT>
          TLS port for secure MQTT
      --ws-port <WS_PORT>
          WebSocket port for MQTT over WebSocket
      --ws-tls-port <WS_TLS_PORT>
          WebSocket over TLS port for secure MQTT over WebSocket
      --quic-port <QUIC_PORT>
          QUIC port for MQTT over QUIC
      --unix-socket <UNIX_SOCKET>
          Unix socket path for MQTT connections (Unix only)

TLS Configuration:
      --server-crt <SERVER_CRT>
          Path to server certificate file (required when tls_port, ws_tls_port, or quic_port is specified)
      --server-key <SERVER_KEY>
          Path to server private key file (required when tls_port, ws_tls_port, or quic_port is specified)

Socket Options:
      --socket-no-delay <SOCKET_NO_DELAY>
          Enable TCP_NODELAY socket option [possible values: true, false]
      --socket-send-buf-size <SOCKET_SEND_BUF_SIZE>
          TCP socket send buffer size in bytes
      --socket-recv-buf-size <SOCKET_RECV_BUF_SIZE>
          TCP socket receive buffer size in bytes
      --socket-reuseport <SOCKET_REUSEPORT>
          Enable SO_REUSEPORT for load balancing across threads (Linux/macOS/BSD)
          [possible values: true, false]
      --socket-keepalive-time <SOCKET_KEEPALIVE_TIME>
          TCP keepalive time in seconds (0 to disable)

MQTT Endpoint Options:
      --ep-recv-buf-size <EP_RECV_BUF_SIZE>
          MQTT endpoint receive buffer size for tokio stream reads in bytes

Authentication/Authorization:
      --auth-file <AUTH_FILE>
          Path to authentication/authorization configuration file (JSON5 format with comments support)
          [default: auth.json]

MQTT v5.0 Feature Support:
      --mqtt-retain-support <RETAIN_SUPPORT>
          Enable retain message support (MQTT v5.0 Retain Available)
          [default: true] [possible values: true, false]
      --mqtt-shared-sub-support <SHARED_SUB_SUPPORT>
          Enable shared subscription support (MQTT v5.0 Shared Subscription Available)
          [default: true] [possible values: true, false]
      --mqtt-sub-id-support <SUB_ID_SUPPORT>
          Enable subscription identifier support (MQTT v5.0 Subscription Identifier Available)
          [default: true] [possible values: true, false]
      --mqtt-wc-support <WC_SUPPORT>
          Enable wildcard subscription support (MQTT v5.0 Wildcard Subscription Available)
          [default: true] [possible values: true, false]
      --mqtt-maximum-qos <MAXIMUM_QOS>
          Maximum QoS level supported by the broker (MQTT v5.0 Maximum QoS)
          Valid values: 0, 1, or 2
          [default: 2]
      --mqtt-receive-maximum <RECEIVE_MAXIMUM>
          Receive Maximum value (MQTT v5.0 Receive Maximum)
          Valid values: 1-65535
          [default: None - no limit]
      --mqtt-maximum-packet-size <MAXIMUM_PACKET_SIZE>
          Maximum Packet Size (MQTT v5.0 Maximum Packet Size)
          Valid values: 1-4294967295
          [default: None - no limit]
      --mqtt-topic-alias-maximum <TOPIC_ALIAS_MAXIMUM>
          Topic Alias Maximum (MQTT v5.0 Topic Alias Maximum)
          Valid values: 0-65535
          [default: None - no topic alias support]
      --mqtt-auto-map-topic-alias <AUTO_MAP_TOPIC_ALIAS>
          Automatically map topic aliases when sending (MQTT v5.0 Topic Alias)
          When enabled, the broker automatically assigns and uses topic aliases for outgoing PUBLISH packets
          [default: false] [possible values: true, false]
      --mqtt-server-keep-alive <SERVER_KEEP_ALIVE>
          Server Keep Alive (MQTT v5.0 Server Keep Alive)
          Override client's Keep Alive value with this value
          Valid values: 0-65535
          [default: None - use client's Keep Alive]
      --mqtt-session-expiry-interval <SESSION_EXPIRY_INTERVAL>
          Session Expiry Interval (MQTT v5.0 Session Expiry Interval)
          Override client's Session Expiry Interval with this value
          Valid values: 0-65535
          [default: None - use client's value]

Other Options:
  -h, --help
          Print help
  -V, --version
          Print version
```

## Building

```bash
# Build in debug mode
cargo build

# Build optimized release
cargo build --release

# The binary will be available at:
# target/debug/mqtt-broker (debug)
# target/release/mqtt-broker (release)
```

## MQTT v5.0 Feature Support

The broker supports optional disabling of MQTT v5.0 features. By default, all features are enabled.

### Feature Flags

- **`--mqtt-retain-support`**: Control retain message support
  - When disabled (`--mqtt-retain-support=false`), the broker rejects PUBLISH packets with the retain flag set
  - Clients are notified via the `Retain Available` property in CONNACK (value 0 when disabled)
  - QoS 0: Connection is closed
  - QoS 1/2: PUBACK/PUBREC with `ImplementationSpecificError` is returned

- **`--mqtt-shared-sub-support`**: Control shared subscription support
  - When disabled (`--mqtt-shared-sub-support=false`), subscriptions to `$share/` topics are rejected
  - Clients are notified via the `Shared Subscription Available` property in CONNACK
  - SUBACK returns `SharedSubscriptionsNotSupported` (0x9E) for shared subscription attempts

- **`--mqtt-sub-id-support`**: Control subscription identifier support
  - When disabled (`--mqtt-sub-id-support=false`), SUBSCRIBE packets with Subscription Identifier property are rejected
  - Clients are notified via the `Subscription Identifier Available` property in CONNACK
  - All subscription entries in the SUBSCRIBE packet will fail

- **`--mqtt-wc-support`**: Control wildcard subscription support
  - When disabled (`--mqtt-wc-support=false`), subscriptions containing `+` or `#` wildcards are rejected
  - Clients are notified via the `Wildcard Subscription Available` property in CONNACK
  - SUBACK returns `WildcardSubscriptionsNotSupported` (0xA2) for wildcard subscription attempts
  - Exact match subscriptions (without wildcards) continue to work normally

- **`--mqtt-maximum-qos`**: Control maximum QoS level
  - Sets the maximum QoS level supported by the broker (default: 2)
  - Valid values: 0, 1, or 2
  - When set to 0 or 1, clients are notified via the `Maximum QoS` property in CONNACK
  - Will QoS exceeding this limit is rejected with `QoS not supported` (0x9B) in CONNACK
  - Subscribe QoS is automatically adjusted to `min(requested_qos, maximum_qos)`
  - Publish QoS exceeding this limit results in DISCONNECT with `QoS not supported` (0x9B)

- **`--mqtt-receive-maximum`**: Control receive maximum
  - Sets the maximum number of QoS 1 and QoS 2 messages that can be processed concurrently (default: None - no limit)
  - Valid values: 1-65535
  - When set, clients are notified via the `Receive Maximum` property in CONNACK
  - The underlying mqtt-endpoint-tokio and mqtt-protocol-core libraries handle flow control automatically

- **`--mqtt-maximum-packet-size`**: Control maximum packet size
  - Sets the maximum packet size that the broker will accept (default: None - no limit)
  - Valid values: 1-4294967295 (32-bit unsigned integer)
  - When set, clients are notified via the `Maximum Packet Size` property in CONNACK
  - The underlying mqtt-endpoint-tokio and mqtt-protocol-core libraries handle packet size validation automatically

- **`--mqtt-topic-alias-maximum`**: Control topic alias maximum
  - Sets the maximum value of Topic Alias that the broker accepts from clients (default: None - topic aliases not supported)
  - Valid values: 0-65535 (16-bit unsigned integer)
  - When set, clients are notified via the `Topic Alias Maximum` property in CONNACK
  - The underlying mqtt-endpoint-tokio and mqtt-protocol-core libraries handle topic alias management automatically

- **`--mqtt-auto-map-topic-alias`**: Automatically map topic aliases when sending
  - When enabled (default: false), the broker automatically assigns and uses topic aliases for outgoing PUBLISH packets
  - This can reduce bandwidth usage by replacing topic names with small integer aliases in repeated messages
  - The underlying mqtt-endpoint-tokio library handles topic alias assignment and mapping automatically
  - Works in conjunction with `--mqtt-topic-alias-maximum` for bidirectional topic alias support

- **`--mqtt-server-keep-alive`**: Override client's Keep Alive value
  - When set, the broker ignores the client's Keep Alive value in CONNECT and uses this value instead
  - The server sends this value via the `Server Keep Alive` property in CONNACK (MQTT v5.0 only)
  - Valid values: 0-65535 (default: None - use client's Keep Alive)
  - When None, the broker uses the client's Keep Alive value and doesn't send the Server Keep Alive property
  - Useful for enforcing uniform Keep Alive intervals across all clients

- **`--mqtt-session-expiry-interval`**: Override client's Session Expiry Interval
  - When set, the broker ignores the client's Session Expiry Interval in CONNECT and uses this value instead
  - The server sends this value via the `Session Expiry Interval` property in CONNACK (MQTT v5.0 only)
  - Valid values: 0-65535 (default: None - use client's value)
  - When None, the broker uses the client's Session Expiry Interval value and doesn't send the property
  - Value 0 means session expires immediately after disconnect
  - Useful for enforcing uniform session persistence policies across all clients

### Example Usage

```bash
# Disable retain message support
./mqtt-broker --tcp-port 1883 --mqtt-retain-support=false

# Set maximum QoS to 1
./mqtt-broker --tcp-port 1883 --mqtt-maximum-qos=1

# Set receive maximum to 100
./mqtt-broker --tcp-port 1883 --mqtt-receive-maximum=100

# Set maximum packet size to 1MB
./mqtt-broker --tcp-port 1883 --mqtt-maximum-packet-size=1048576

# Set topic alias maximum to 10
./mqtt-broker --tcp-port 1883 --mqtt-topic-alias-maximum=10

# Enable automatic topic alias mapping for outgoing messages
./mqtt-broker --tcp-port 1883 --mqtt-auto-map-topic-alias=true

# Enable bidirectional topic alias support
./mqtt-broker --tcp-port 1883 \
  --mqtt-topic-alias-maximum=10 \
  --mqtt-auto-map-topic-alias=true

# Set server keep alive to 60 seconds
./mqtt-broker --tcp-port 1883 --mqtt-server-keep-alive=60

# Set session expiry interval to 300 seconds (5 minutes)
./mqtt-broker --tcp-port 1883 --mqtt-session-expiry-interval=300

# Disable multiple features
./mqtt-broker --tcp-port 1883 \
  --mqtt-retain-support=false \
  --mqtt-shared-sub-support=false \
  --mqtt-wc-support=false \
  --mqtt-maximum-qos=1 \
  --mqtt-receive-maximum=50 \
  --mqtt-maximum-packet-size=1048576 \
  --mqtt-topic-alias-maximum=10 \
  --mqtt-auto-map-topic-alias=true \
  --mqtt-server-keep-alive=60 \
  --mqtt-session-expiry-interval=300
```

## TLS Configuration

For TLS support, you need a server certificate and private key:

```bash
# Generate self-signed certificate for testing (not for production!)
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 365 -nodes -subj "/CN=localhost"

# Run broker with TLS
./mqtt-broker --tls-port 8883 --server-crt server.crt --server-key server.key
```

## Client Connection Examples

### TCP Connection
```bash
mosquitto_pub -h localhost -p 1883 -t "test/topic" -m "Hello MQTT"
```

### Unix Socket Connection
```bash
# Using mosquitto_pub with Unix socket (if supported)
mosquitto_pub --unix /tmp/mqtt.sock -t "test/topic" -m "Hello MQTT via Unix socket"
```

### WebSocket Connection
```bash
# Using a WebSocket MQTT client
# Connect to ws://localhost:8080/
```

### TLS Connection
```bash
mosquitto_pub -h localhost -p 8883 --cafile server.crt -t "test/topic" -m "Hello Secure MQTT"
```

## Architecture

The broker is built on top of:
- **[mqtt-endpoint-tokio](https://github.com/redboltz/mqtt-endpoint-tokio)**: Provides MQTT protocol handling and transport abstractions
- **[mqtt-protocol-core](https://github.com/redboltz/mqtt-protocol-core)**: Sans-I/O MQTT protocol implementation
- **tokio**: Async runtime and networking

### Key Components

- **BrokerManager**: Central broker logic and client management
- **SubscriptionStore**: Efficient topic-based subscription management
- **Multi-transport Support**: Handles TCP, TLS, WebSocket, and WebSocket over TLS connections
- **Configurable Threading**: Adjustable worker thread count for optimal performance

## Performance

The broker is designed for high performance:
- Lock-free subscription management where possible
- Efficient topic matching using prefix trees
- Configurable worker threads to match your hardware
- Zero-copy message forwarding when feasible

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Free Trial Broker on Cloud

You can connect to `mqtt-broker-tokio.redboltz.net` for testing purposes.

| Port  | Protocol              |
|-------|----------------------|
| 21883 | MQTT (TCP)           |
| 28883 | MQTT (TLS)           |
| 20080 | MQTT (WebSocket)     |
| 20443 | MQTT (WebSocket TLS) |
| 20443 | MQTT (QUIC/UDP)      |

### Important Notice

**Disclaimer:** This broker is provided "as is" without any warranty. The author assumes no responsibility or liability for any issues, data loss, or damages arising from its use.

You are free to use it for any application, but please do not abuse it or rely on it for anything of importance. This server runs on a very low-spec VPS and is not intended to demonstrate any performance characteristics. Please do not publish anything sensitive, as anybody could be listening.
