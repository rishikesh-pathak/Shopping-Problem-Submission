# Databricks Shopping Sessions

The native Rust engine parses input in parallel, links identities, deduplicates and sessionizes events, computes attribution and assists, and writes Parquet. The image also contains the original DuckDB engine for inputs that require the general CSV reader, have ambiguous duplicates, or exceed the native memory estimate.

### Uploaded baseline results from the Windows laptop (2026-11-21)

- All **36/36 official edge-case fixtures** passed.
- 1,000,000-event official development set (seed 1) matched **exactly**:
  `user_daily` 117,038 rows and `campaign_daily` 27,400 rows, with zero differing rows.
- A profiled local Windows run reported **4.38 seconds** internal time (**4.76 seconds** script wall time). This is **not** a comparable benchmark to the official 20M-event test.

### Competition runtime

Build the Dockerfile at the repo root for linux/amd64. The entry point reads `/data` and writes Parquet datasets to `/output/user_daily/` and `/output/campaign_daily/`. Read all Parquet parts in each directory. Set `SHOPPING_FORCE_SQL=1` to run the original engine for comparison.

Runtime input is mounted at `/data` and nothing about seed, row count, event files, or dates is hardcoded. Generated data and output are deliberately not checked in.

### How to check locally

On Windows with Python, Rust, and Microsoft C++ tools installed, build and exercise the native engine with the fixtures.

```powershell
cargo build --release --locked --manifest-path pipeline/native/Cargo.toml
python -m pip install polars
python scripts/run_fixtures.py fixtures -- pipeline/native/target/release/shopping-native.exe
```

The original Python and DuckDB path can also be tested locally.

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

The test container runs as UID 1000 with networking disabled and a read-only root filesystem. The default limits are eight CPUs and 24 GB. Set `CPUS=4` if the host has a four-core quota. The image build downloads the locked Cargo dependencies and the pinned DuckDB wheel. If your build uses a TLS proxy, pass its trusted bundle with `PIP_CA=/path/to/ca-bundle.crt`. If the Docker builder is offline from crates.io, vendor dependencies on a connected machine with `cargo vendor --locked --manifest-path pipeline/native/Cargo.toml /path/to/cache/vendor`, then build with `CARGO_VENDOR=/path/to/cache`.
