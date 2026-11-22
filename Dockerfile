# Fast compiled input stager, proven DuckDB aggregation.
# The builder is discarded; final image contains just the compiled program.
FROM rust:1.91-slim-bookworm AS rust-builder
WORKDIR /src
COPY pipeline/stage.rs /src/stage.rs
RUN rustc --edition=2021 -C opt-level=3 /src/stage.rs -o /src/stage

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY requirements.txt /app/requirements.txt
RUN --mount=type=secret,id=pip_ca \
    if [ -f /run/secrets/pip_ca ]; then export PIP_CERT=/run/secrets/pip_ca; fi; \
    pip install --no-cache-dir -r /app/requirements.txt
COPY --from=rust-builder --chmod=0755 /src/stage /app/pipeline/stage
COPY --chmod=0644 pipeline/pipeline.py /app/pipeline/pipeline.py
COPY --chmod=0755 run.sh /app/run.sh
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
CMD ["/app/run.sh"]
