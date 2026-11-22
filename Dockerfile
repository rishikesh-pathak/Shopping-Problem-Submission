# The Rust engine writes Parquet directly. Keep the SQL engine for fallback.
FROM scratch AS cargo-registry

FROM rust:1.91-slim-bookworm AS rust-builder
WORKDIR /src
COPY pipeline/stage.rs pipeline/csv_input.rs /src/
RUN rustc --edition=2021 -C opt-level=3 /src/stage.rs -o /src/stage
COPY pipeline/native /src/native
RUN --mount=type=secret,id=pip_ca --mount=type=bind,from=cargo-registry,target=/cargo-registry \
    --mount=type=tmpfs,target=/src/native/target \
    if [ -f /run/secrets/pip_ca ]; then export CARGO_HTTP_SSL_CAINFO=/run/secrets/pip_ca; fi; \
    if [ -d /cargo-registry/vendor ]; then \
        set -- --offline --config 'source.crates-io.replace-with="vendor"' \
            --config 'source.vendor.directory="/cargo-registry/vendor"'; \
    else set --; fi; \
    cargo build --manifest-path /src/native/Cargo.toml --release --locked "$@" \
    && cp /src/native/target/release/shopping-native /src/shopping-native

FROM python:3.12-slim-bookworm
# Test jemalloc against glibc malloc for 8-thread hash/partition workloads.
RUN apt-get update && apt-get install -y --no-install-recommends libjemalloc2 \
    && rm -rf /var/lib/apt/lists/*
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY requirements.txt /app/requirements.txt
RUN --mount=type=secret,id=pip_ca \
    if [ -f /run/secrets/pip_ca ]; then export PIP_CERT=/run/secrets/pip_ca; fi; \
    pip install --no-cache-dir -r /app/requirements.txt
COPY --from=rust-builder --chmod=0755 /src/stage /app/pipeline/stage
COPY --from=rust-builder --chmod=0755 /src/shopping-native /app/pipeline/shopping-native
COPY --chmod=0644 pipeline/pipeline.py /app/pipeline/pipeline.py
COPY --chmod=0755 run.sh /app/run.sh
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
CMD ["/app/run.sh"]
