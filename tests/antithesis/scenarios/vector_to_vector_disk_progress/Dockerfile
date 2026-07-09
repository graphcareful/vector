# syntax=docker/dockerfile:1

ARG SANCOV_RUSTFLAGS='["-Cpasses=sancov-module","-Cllvm-args=-sanitizer-coverage-level=3","-Cllvm-args=-sanitizer-coverage-trace-pc-guard","-Clink-args=-Wl,--build-id"]'

############################
# Vector SUT — build stage #
############################
FROM rust:1.92-bookworm AS vector-build
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler cmake perl build-essential pkg-config libssl-dev clang binutils \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml build.rs ./
COPY lib ./lib
COPY src ./src
COPY proto ./proto
COPY benches ./benches
COPY tests ./tests
COPY vdev ./vdev
COPY scripts ./scripts
ARG SANCOV_RUSTFLAGS
# debug=true keeps DWARF for /symbols. lto=false keeps sancov instrumentation
# predictable and stops the optimizer from dropping the force-linked runtime.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --no-default-features \
      --features "sources-http_server,sinks-http,sources-internal_metrics,sinks-prometheus,sources-demo_logs,sources-vector,sinks-vector,antithesis-scenario-disk-progress" \
      --bin vector \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'profile.release.debug = true' \
      --config 'profile.release.lto = false' \
      --config "target.x86_64-unknown-linux-gnu.rustflags = ${SANCOV_RUSTFLAGS}" \
    && cp target/x86_64-unknown-linux-gnu/release/vector /usr/local/bin/vector \
    && echo "validating instrumentation symbols..." \
    && nm /usr/local/bin/vector | grep -q __sanitizer_cov_trace_pc_guard \
    && nm /usr/local/bin/vector | grep -q antithesis_load_libvoidstar \
    && echo "instrumentation OK"

##################################
# Harness (workload) — build stage
##################################
FROM rust:1.92-bookworm AS workload-build
RUN apt-get update && apt-get install -y --no-install-recommends binutils \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml build.rs ./
COPY lib ./lib
COPY src ./src
COPY proto ./proto
COPY benches ./benches
COPY tests ./tests
COPY vdev ./vdev
COPY scripts ./scripts
ARG SANCOV_RUSTFLAGS
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p antithesis-harness \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'profile.release.debug = true' \
      --config "target.x86_64-unknown-linux-gnu.rustflags = ${SANCOV_RUSTFLAGS}" \
    && D=target/x86_64-unknown-linux-gnu/release \
    && cp "$D/oracle" "$D/eventually_disk_progress_check" "$D/parallel_driver_disk_progress" /usr/local/bin/ \
    && echo "validating instrumentation symbols..." \
    && nm /usr/local/bin/oracle | grep -q __sanitizer_cov_trace_pc_guard \
    && nm /usr/local/bin/oracle | grep -q antithesis_load_libvoidstar \
    && echo "instrumentation OK"

#####################################
# Runtime: Vector SUT (one per node) #
#####################################
FROM debian:stable-slim AS vector
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=vector-build /usr/local/bin/vector /usr/bin/vector
COPY tests/antithesis/scenarios/vector_to_vector_disk_progress/head.yaml /etc/vector/head.yaml
RUN mkdir -p /var/lib/vector /symbols && ln -s /usr/bin/vector /symbols/vector
ENV NO_COLOR=1
EXPOSE 8080 9598
ENTRYPOINT ["/usr/bin/vector"]

###################################
# Runtime: workload (oracle + cmds) #
###################################
FROM debian:stable-slim AS workload
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=workload-build /usr/local/bin/oracle /usr/bin/oracle
# parallel_driver_ prefix: the load generator. Antithesis runs it (repeatedly,
# concurrently) during the fault phase, which both drives traffic through the disk
# buffer under faults and satisfies the requirement that a driver start before the
# eventually_ command below can be scheduled.
COPY --from=workload-build /usr/local/bin/parallel_driver_disk_progress /opt/antithesis/test/v1/diskprogress/parallel_driver_disk_progress
# eventually_ prefix: Antithesis runs this once in the terminal window, after it has
# stopped all faults and killed the drivers, so the oversized-record handling is
# judged against a quiet, fault-free (but freshly recovered) pipeline.
COPY --from=workload-build /usr/local/bin/eventually_disk_progress_check /opt/antithesis/test/v1/diskprogress/eventually_disk_progress_check
# Symbolize all instrumented binaries so crashes resolve against /symbols.
RUN mkdir -p /symbols \
 && ln -s /usr/bin/oracle /symbols/oracle \
 && ln -s /opt/antithesis/test/v1/diskprogress/parallel_driver_disk_progress /symbols/parallel_driver_disk_progress \
 && ln -s /opt/antithesis/test/v1/diskprogress/eventually_disk_progress_check /symbols/eventually_disk_progress_check
ENV NO_COLOR=1
ENTRYPOINT ["/usr/bin/oracle"]
