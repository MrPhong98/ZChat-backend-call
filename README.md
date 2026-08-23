# Z-Chat WebRTC Signaling (Rust)

Drop-in replacement for the Flask-SocketIO `server.py`.

**Stack:** Axum + Tokio + [socketioxide](https://github.com/totodore/socketioxide) (Socket.IO v4)

## Why Rust

- Native async WebSocket (no gevent monkey-patch)
- Lower latency / memory under concurrent calls
- Single static binary for deploy

## Build & run

```bash
cargo build --release
PORT=5000 ./target/release/zchat-signaling
```

Or:

```bash
cargo run --release
```

Health check: `GET /` → `{ "ok": true, "service": "Z-Chat WebRTC Signaling", "online": N }`

## Client

Point existing `socket.io-client` at the same URL/port. Event names and payloads match `server.py`:

| Event | Direction |
|-------|-----------|
| `register` | C→S `{ username }` |
| `registered` / `register_error` | S→C |
| `user_online` / `user_offline` | S→C broadcast |
| `call_user` | C→S `{ to, from, offer, callType }` |
| `incoming_call` | S→C |
| `make_answer` | C→S `{ to, from, answer }` |
| `call_answered` | S→C |
| `ice_candidate` | both |
| `end_call` / `reject_call` | C→S |
| `call_ended` | S→C |
| `media_state` | both |
| `call_error` | S→C |

## Deploy (example Dockerfile)

```dockerfile
FROM rust:1.83-bookworm AS build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/zchat-signaling /usr/local/bin/
ENV PORT=5000
EXPOSE 5000
CMD ["zchat-signaling"]
```
