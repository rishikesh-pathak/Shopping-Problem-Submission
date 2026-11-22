# Databricks Shopping Sessions

Rust validates and stages input shards in parallel. DuckDB resolves the staged identity mapping, deduplicates and sessionizes events, computes attribution and assists, and writes the Parquet tables.

### Uploaded baseline results from the Windows laptop (2026-11-21)

- All **36/36 official edge-case fixtures** passed.
- 1,000,000-event official development set (seed 1) matched **exactly**:
  `user_daily` 117,038 rows and `campaign_daily` 27,400 rows, with zero differing rows.
- A profiled local Windows run reported **4.38 seconds** internal time (**4.76 seconds** script wall time). This is **not** a comparable benchmark to the official 20M-event test.

### Competition runtime

Build the Dockerfile at the repo root for linux/amd64. On execution, `run.sh` starts `pipeline/pipeline.py`, which invokes the compiled Rust input stager and DuckDB to write `/output/user_daily/part-0.parquet` and `/output/campaign_daily/part-0.parquet`.

Runtime input is mounted at `/data` and nothing about seed, row count, event files, or dates is hardcoded. Generated data and output are deliberately not checked in.

### How to check locally

On Windows with Python 3.12, Rust, and Microsoft C++ tools installed:

1. Install dependencies with `python -m pip install -r requirements.txt polars`.
2. Build the Rust stage: `rustc --edition=2021 -C opt-level=3 pipeline\stage.rs -o pipeline\stage.exe`
3. Run fixtures: `python scripts\run_fixtures.py fixtures -- python pipeline\pipeline.py`

The official reference test requires generating the 1M-event dataset and downloading the official answer ZIP. See the competition's rules.

### Linux container checks

Install Polars in the host Python environment for the comparison scripts, then run

```sh
make fixtures
SHOPPING_PROFILE=1 make run-dev DEV=/path/to/official/dev
make check-dev ANSWER=/path/to/unpacked/dev-answer
```

The test container runs as UID 1000 with networking disabled and a read-only root filesystem. The default limits are eight CPUs and 24 GB. Set `CPUS=4` if the host has a four-core quota. For a build behind a TLS proxy, set `PIP_CA=/path/to/ca-bundle.crt` to mount the trusted CA bundle during dependency installation.

### Caution

Local correctness on seed 1 is not a guarantee that hidden seed tests or the 60-million-event final will pass or fit within the time limit. Submit early to get the real 20M benchmark. **Do not claim a subsecond score from these local tests.**
