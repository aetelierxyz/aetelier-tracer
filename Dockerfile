FROM rust:1.99-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev build-essential && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --release --locked -p tracerd --bin tracerd

FROM debian:bookworm-slim
ARG TARGETARCH
LABEL org.opencontainers.image.source="https://github.com/IteraLabs/aetelier-tracer" \
      org.opencontainers.image.title="aetelier-tracer" \
      org.opencontainers.image.description="Tracer fill-probability instrument: tracerd live daemon, replay engine, REST API" \
      org.opencontainers.image.licenses="Apache-2.0" \
      aetelier.component="aetelier-tracer" \
      aetelier.role="data-plane" \
      aetelier.domain="tracer" \
      aetelier.arch="${TARGETARCH}" \
      aetelier.lifecycle="canonical"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/tracerd /usr/local/bin/tracerd
COPY --from=build /app/params /opt/aetelier/tracer/params
ENTRYPOINT ["tracerd"]
CMD ["--help"]
