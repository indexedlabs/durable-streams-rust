# syntax=docker/dockerfile:1
#
# Built and published by .github/workflows/docker.yml to
# ghcr.io/pgxsinkit/durable-streams-rust. The build context is the repo root.
#
# Upstream this lived in a monorepo, so the COPY paths were prefixed with
# `packages/durable-streams-rust/` and the context was the monorepo root. This
# repo IS the package, so the paths are repo-relative.

# ---- build stage: compile the release binary (glibc, matches the runtime) ----
# Pinned to the same rustc as rust-toolchain.toml (and as pgxsinkit/electric-circuits)
# so the image build cannot drift from what CI and local builds produce. Upstream
# floated this at `rust:1-bookworm`.
FROM rust:1.96.0-bookworm AS build
WORKDIR /app
# Copy only what the build needs (no target/, no npm/).
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Built with `telemetry` so the production server exports its spans and metrics
# (append and fsync latency, open files against the descriptor limit) over
# OTLP/gRPC to the endpoint the standard `OTEL_*` variables name. An `https://`
# endpoint such as Logfire (`https://logfire-us.pydantic.dev`) is reached over
# TLS, trusting the runtime stage's CA bundle (/etc/ssl/certs, which the
# distroless base ships). A default build exports no metrics. Built with `tier`
# too, so one image serves a store that runs the S3 tier (`--tier s3`) and one that
# leaves it off. With no S3 keys in its environment, the tier signs with the host's
# role (the ECS task role, else the instance profile).
RUN cargo build --release --locked --features telemetry,tier

# ---- runtime stage: distroless (glibc cc), no shell / package manager ----
# Its /etc/ssl/certs/ca-certificates.crt holds the roots the OTLP exporter trusts.
FROM gcr.io/distroless/cc-debian12 AS runtime
COPY --from=build /app/target/release/durable-streams-server /usr/local/bin/durable-streams-server
# Protocol default port (PROTOCOL.md §13.1); override with `--port`.
EXPOSE 4437
ENTRYPOINT ["/usr/local/bin/durable-streams-server"]
# The WAL pilot intentionally keeps this listener on loopback. The access sidecar is the only
# external listener; the task definition supplies an explicit persistent --data-dir and store
# identity arguments.
