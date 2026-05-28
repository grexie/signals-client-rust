FROM rust:1-bookworm AS build

WORKDIR /src
COPY . .
WORKDIR /src/examples/signalsbot
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /src/examples/signalsbot/target/release/signalsbot-rust-example /usr/local/bin/signalsbot
RUN mkdir -p /data
ENV SIGNALS_DB_PATH=/data/signalsbot.json
ENTRYPOINT ["/usr/local/bin/signalsbot"]
CMD ["papertrader"]
