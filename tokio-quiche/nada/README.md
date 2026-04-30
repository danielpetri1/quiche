# *Nada*

An implementation of [RFC 9298](https://datatracker.ietf.org/doc/rfc9298/) (Proxying UDP in HTTP) with tokio-quiche.

This project also implements [RFC 9297](https://datatracker.ietf.org/doc/rfc9297/) (HTTP Datagrams and the Capsule Protocol) and supports HTTP/3’s Extensible Prioritization Scheme (EPS) as defined in [RFC 9218](https://datatracker.ietf.org/doc/rfc9218/).

Multipath support is provided with the [draft implementation](https://github.com/qdeconinck/quiche/pull/4) by Bastien Veuthey and Quentin De Coninck.

## Build

All commands can be run from the repository root using `--manifest-path`:

```
cargo build --manifest-path tokio-quiche/nada/Cargo.toml --release
```

Please regularly format and lint with `cargo fmt` and `cargo clippy`. `cargo test` runs unit tests. 

## Example usage

Run the MASQUE client to open a UDP tunnel to a MASQUE proxy that accesses an endpoint using the inner QUIC connection.
Use `--reliable` on the client to use Datagram Capsules as a substrate. Omit it to directly proxy over QUIC datagrams.

Each `--outer-prio` (or `--op`) flag opens one CONNECT-UDP tunnel with the given EPS priority value. Specify it multiple times to open several tunnels with different priorities. Each tunnel can target a different endpoint via `--endpoint` (or `--ep`). The `--inner-prio` option sets the priority for the inner QUIC connections.

### Starting the MASQUE client:

```
cargo run --manifest-path tokio-quiche/nada/Cargo.toml --release --bin masque-client -- \
  --client-address [fd00:0:0:1::1]:0 \
  --proxy-address [fd00:0:0:3::1]:443 \
  --target-ip fd00:0:0:4::1 \
  --target-port 25565 \
  --results-dir /root/results \
  --log-keys \
  --endpoint /index.html \
  --reliable \
  --outer-prio "u=2,i"
```

Multiple tunnels with different priorities and endpoints:

```
cargo run --manifest-path tokio-quiche/nada/Cargo.toml --release --bin masque-client -- \
  --client-address [fd00:0:0:1::1]:0 \
  --proxy-address [fd00:0:0:3::1]:443 \
  --target-ip fd00:0:0:4::1 \
  --target-port 25565 \
  --reliable \
  --op "u=0,i" --ep /index.html \
  --op "u=3,i" --ep /index.html \
```

### Starting the MASQUE proxy:

```
cargo run --manifest-path tokio-quiche/nada/Cargo.toml --release --bin masque-proxy -- \
  --proxy-address [fd00:0:0:3::1]:443 \
  --results-dir /root/results \
  --log-keys
```

### Server

Running the target server yourself is not strictly necessary, as any reachable HTTP/3 server can be used instead.
For testing, you can use Cloudflare's sample HTTP/3 server:

```
RUST_LOG=info cargo run --release --example async_http3_server -- \
  --address [fd00:0:0:4::1]:25565 \
  --tls-cert-path /root/tokio/src/bin/certs/cert.crt \
  --tls-private-key-path /root/tokio/src/bin/certs/cert.key
```

Several other options are listed under `--help` to, e.g., control the log level, certificate verification, and the storing
of secrets and responses.

## Multipath QUIC

Nada supports multipath QUIC on the outer QUIC connection between the client and the proxy.

Both the client and the proxy must enable multipath for it to take effect. The client binds additional sockets (one per extra address) and, after the QUIC handshake completes, probes each additional path so it can be used for sending and receiving packets.

### Client options


| Flag                     | Description                                                                                                |
| ------------------------ |------------------------------------------------------------------------------------------------------------|
| `--multipath`            | Enable multipath QUIC on the outer connection.                                                             |
| `--extra-address <ADDR>` | An additional local address to bind for multipath. Can be specified multiple times, once per extra path.   |
| `--scheduler <ALG>`      | Packet scheduling algorithm (`lowrtt`, `minrtt`, `roundrobin`, `random`, `lowestlatency`, `ecf`, `sa-ecf`). Default: `lowrtt`. |
| `--probe-timeout <SECS>` | Seconds to wait for path probing to complete before proceeding. Default: `5`.                              |


### Proxy options


| Flag                     | Description                                                                                                |
| ------------------------ |------------------------------------------------------------------------------------------------------------|
| `--multipath`            | Enable multipath QUIC on the proxy.                                                                        |
| `--scheduler <ALG>`      | Packet scheduling algorithm (`lowrtt`, `minrtt`, `roundrobin`, `random`, `lowestlatency`, `ecf`, `sa-ecf`). Default: `lowrtt`. |


### Example

Start the proxy:

```
cargo run --manifest-path tokio-quiche/nada/Cargo.toml --release --bin masque-proxy -- \
  --multipath \
  --proxy-address [fd00:0:0:3::1]:443 \
  --scheduler lowrtt \
  --initial-max-path-id 1 \
  --results-dir /root/results --log-keys
```

Start the client, binding two local addresses that each connect to the proxy:

```
cargo run --manifest-path tokio-quiche/nada/Cargo.toml --release --bin masque-client -- \
  --multipath \
  --client-address [fd00:0:0:1::1]:0 \
  --extra-address [fd00:0:0:2::1]:0 \
  --scheduler lowrtt \
  --probe-timeout 3 \
  --proxy-address [fd00:0:0:3::1]:443 \
  --target-ip fd00:0:0:4::1 \
  --target-port 25565 \
  --outer-prio u=2,i,sched=eps-aware-ecf \
  --endpoint /index.html \
  --reliable \
  --qlog \
  --keep-resp \
  --results-dir /root/results \
  --log-keys
```

The `--client-address` is the primary path. Each `--extra-address` adds an additional path. The number of extra addresses on the client determines the `initial_max_path_id` transport parameter negotiated during the handshake.
Pass `--initial-max-path-id <N>` on either side to override the derived value.

Scheduler names accept the following short aliases: `rr` (roundrobin), `rand` (random), `ll` (lowestlatency), `eps-aware-ecf`.

### Runtime scheduler switching

The outer connection's packet scheduler can be replaced at runtime
via a `sched=<alg>` token in the EPS priority header.
When the proxy receives a CONNECT-UDP request whose `priority` header
carries `sched=`, it swaps the active `BoxedScheduler` on the outer (client-proxy) QUIC connection before forwarding traffic.

```
--outer-prio "u=2,i,sched=eps-aware-ecf"
```