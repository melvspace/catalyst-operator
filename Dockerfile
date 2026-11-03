ARG BUILDER_IMAGE=rust:1.97.1-bookworm
FROM ${BUILDER_IMAGE} AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin catalyst-operator

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /build/target/release/catalyst-operator /catalyst-operator
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/catalyst-operator"]
