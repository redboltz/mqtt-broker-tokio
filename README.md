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

Options:
      --cpus <CPUS>                 Number of worker threads (defaults to CPU count)
      --log-level <LOG_LEVEL>       Log level [default: info] [possible values: error, warn, info, debug, trace]
      --tcp-port <TCP_PORT>         TCP port for plain MQTT
      --tls-port <TLS_PORT>         TLS port for secure MQTT
      --ws-port <WS_PORT>           WebSocket port for MQTT over WebSocket
      --ws-tls-port <WS_TLS_PORT>   WebSocket over TLS port for secure MQTT over WebSocket
      --server-crt <SERVER_CRT>     Path to server certificate file (required for TLS ports)
      --server-key <SERVER_KEY>     Path to server private key file (required for TLS ports)
  -h, --help                        Print help
  -V, --version                     Print version
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
