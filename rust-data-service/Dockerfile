# Stage 1: Chef (Cache dependencies)
FROM rust:latest AS chef
# Use sparse registry for faster updates
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN cargo install cargo-chef
WORKDIR /app

# Stage 2: Planner
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Builder
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin rust-data-service

# Stage 4: Runtime
FROM debian:bookworm-slim AS runtime
WORKDIR /app
# Install OpenSSL/Ca-certificates if needed (we use rustls so mostly ca-certificates)
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rust-data-service /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/rust-data-service"]
