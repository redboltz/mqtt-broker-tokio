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

# Multiple transports
./mqtt-broker --tcp-port 1883 --ws-port 8080

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

### Example Usage

```bash
# Disable retain message support
./mqtt-broker --tcp-port 1883 --mqtt-retain-support=false

# Set maximum QoS to 1
./mqtt-broker --tcp-port 1883 --mqtt-maximum-qos=1

# Disable multiple features
./mqtt-broker --tcp-port 1883 \
  --mqtt-retain-support=false \
  --mqtt-shared-sub-support=false \
  --mqtt-wc-support=false \
  --mqtt-maximum-qos=1
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
