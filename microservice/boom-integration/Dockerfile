# Two-step build: compile Rust binary on the HOST first, then package here.
#
# Step 1 (on host):
#   cd BooMGateway-main
#   cargo build --release -p boom-main
#
# Step 2 (Docker):
#   docker build -t reg.local:32000/boom-gateway:latest .
#
FROM openeuler/openeuler:24.03

COPY target/release/boom-gateway /usr/bin/boom-gateway

EXPOSE 4000

ENTRYPOINT ["boom-gateway"]
