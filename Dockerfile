FROM rust:1.88-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config git libssl-dev build-essential cmake protobuf-compiler libprotobuf-dev zlib1g-dev libcurl4-openssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
WORKDIR /app/aetelier-tracer
RUN --mount=type=secret,id=cargo_git_token \
    if [ -s /run/secrets/cargo_git_token ]; then \
      git config --global credential."https://github.com".helper '!f() { echo username=x-access-token; echo "password=$(cat /run/secrets/cargo_git_token)"; }; f'; \
    fi && \
    CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build --release -p tracerd --bin tracerd

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
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libssl3 libcurl4 && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/aetelier-tracer/target/release/tracerd /usr/local/bin/tracerd
COPY --from=build /app/aetelier-tracer/params /opt/aetelier/tracer/params
ENTRYPOINT ["tracerd"]
CMD ["--help"]
