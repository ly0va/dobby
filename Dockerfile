FROM rust:1-slim-buster AS builder

RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /dobby
COPY . .

RUN cargo build --release

FROM debian:buster-slim

COPY --from=builder /dobby/target/release/dobbyd /usr/local/bin/dobbyd
COPY --from=builder /dobby/target/release/dobby /usr/local/bin/dobby
