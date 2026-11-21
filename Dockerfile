FROM rust:1.92-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
RUN cargo build --locked --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
USER 1000:1000
ENTRYPOINT ["/app/shopping-sessions"]
