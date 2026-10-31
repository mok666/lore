# SPDX-License-Identifier: MIT
#
# Builds loreserver from a release source tree (a `git archive` of a cf-v* tag
# unpacked into the build context) for linux/amd64. This file is read from the
# workflow checkout, not from the context, so a tag that predates it still builds.
#
#   docker build -f contrib/cloudflare/Dockerfile --build-arg SOURCE_REVISION=<sha> <context>
FROM rust:1.97.1-slim-trixie@sha256:8e8cf8f7fd54a2d23d5a743b3a03f56e26b6c774276c33fa0595111704ebb15c AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cargo build --locked --release -p lore-server \
    && install -m 0755 target/release/loreserver /loreserver \
    && sha256sum /loreserver > /loreserver.sha256

FROM debian:trixie-slim@sha256:f6b08b09a05725f9431502d3b5714f5c4576e82c1a84fa1debe0c24a7e0c3ec4

ARG SOURCE_REVISION
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.source="https://github.com/mok666/lore" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /loreserver /usr/local/bin/loreserver
COPY --from=builder /loreserver.sha256 /usr/share/doc/loreserver/loreserver.sha256
COPY --from=builder /build/LICENSE /usr/share/doc/loreserver/LICENSE
COPY --from=builder /build/lore-server/config/default.toml /etc/lore/config/default.toml

# Every shared library must resolve in this runtime image, and the binary must run.
RUN if ldd /usr/local/bin/loreserver | grep -q "not found"; then \
        ldd /usr/local/bin/loreserver; echo "unresolved shared library" >&2; exit 1; \
    fi \
    && /usr/local/bin/loreserver --version

ENV LORE_CONFIG_PATH=/etc/lore/config
ENV RUST_LOG=info

RUN mkdir -p /data /etc/lore/certs
VOLUME /data

# gRPC (TCP) and QUIC (UDP) share one port; HTTP is separate. Ports follow the mounted config.
EXPOSE 41337/tcp 41337/udp 41339/tcp

ENTRYPOINT ["/usr/local/bin/loreserver"]
CMD ["--config", "/etc/lore/config"]
