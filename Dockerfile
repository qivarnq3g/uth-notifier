FROM rust:1.97-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa AS rust-builder

WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY apps/core-agent ./apps/core-agent
COPY apps/edge-worker ./apps/edge-worker
COPY crates ./crates
COPY migrations ./migrations
RUN cargo build --locked --release -p uth-agent

FROM node:24-bookworm-slim@sha256:6f7b03f7c2c8e2e784dcf9295400527b9b1270fd37b7e9a7285cf83b6951452d AS browser-dependencies

WORKDIR /build/apps/browser-agent
COPY apps/browser-agent/package.json apps/browser-agent/package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force

FROM node:24-bookworm-slim@sha256:6f7b03f7c2c8e2e784dcf9295400527b9b1270fd37b7e9a7285cf83b6951452d

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates chromium dumb-init \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_PATH=/usr/bin/chromium
ENV HOME=/tmp
WORKDIR /app

COPY --from=rust-builder /build/target/release/uth-agent /usr/local/bin/uth-agent
COPY --from=browser-dependencies /build/apps/browser-agent/node_modules ./apps/browser-agent/node_modules
COPY apps/browser-agent/src/post.ts ./apps/browser-agent/src/post.ts
COPY config/classifier-rules.v1.json ./config/classifier-rules.v1.json
COPY deploy/server-entrypoint.sh /usr/local/bin/server-entrypoint

RUN chmod 0555 /usr/local/bin/uth-agent /usr/local/bin/server-entrypoint

USER node
ENTRYPOINT ["dumb-init", "--", "/usr/local/bin/server-entrypoint"]
CMD ["--help"]
