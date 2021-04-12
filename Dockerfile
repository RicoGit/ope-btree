# Create a container with statically linked binary (container size ~ 2 MB)

FROM ekidd/rust-musl-builder:stable AS builder

# copy all project, not only server/grpc folder
COPY . .
RUN sudo chown -R rust:rust .
RUN cargo build --release -p server-grpc

FROM scratch

COPY --from=builder /home/rust/src/target/x86_64-unknown-linux-musl/release/server-grpc /app
WORKDIR /project
ENTRYPOINT ["/app"]
