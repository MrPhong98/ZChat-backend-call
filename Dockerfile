# Step 1: Build binary
FROM rust:1.80-alpine as builder
WORKDIR /app
RUN apk add --no-libc-dev musl-dev
COPY . .
RUN cargo build --release

# Step 2: Run binary
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/target/release/zchat-signaling .
EXPOSE 5000
ENV PORT=5000
CMD ["./zchat-signaling"]
