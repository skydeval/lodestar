# Lodestar: one binary, one image. The role is chosen by the command.
FROM rust:1.88-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
# Root certificates ship inside the binary. Nothing else is needed.
RUN useradd --system --uid 10001 --no-create-home lodestar
COPY --from=build /src/target/release/lodestar /usr/local/bin/lodestar
USER lodestar
ENTRYPOINT ["lodestar"]
