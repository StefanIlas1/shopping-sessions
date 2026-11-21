# Shopping Sessions

Rust pipeline for the Databricks Shopping Sessions track at Builders Buildathon.
It uses the organizer's DuckDB starter fixture and comparison scripts and writes
both Parquet tables directly from a native binary.

The pipeline reads newline-aligned file chunks, partitions syntactically valid
rows by ID, and keeps the earliest ingest timestamp before validating events.
It resolves linked users through a disjoint-set forest and sorts valid events
and clicks by person and time. Campaign assists count session starts in merged
seven-day campaign intervals. Last-click credit uses a separate chronological
pass.

## Scorer container

The root Dockerfile builds a Linux binary. The scorer sets `DATA_DIR`,
`OUTPUT_DIR`, and `NCPU`. The process also accepts the input and output
directories as positional arguments. The default worker count is eight.

```sh
make build
make fixtures
make run-dev DEV=/path/to/dev
make check-dev ANSWER=/path/to/dev-answer
```

The fixture and comparison scripts need Python and `pip install -r requirements.txt`.
The [official challenge page](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)
has the generator and expected dev output downloads.

## Native benchmarks

```sh
cargo build --release --locked
python3 scripts/run_fixtures.py fixtures -- target/release/shopping-sessions
python3 scripts/benchmark.py /path/to/practice --threads 8 --runs 3
```

The benchmark reports external process wall time and internal phase times.
`scan` reads the CSV files and counts rows. `dedup` validates CSV structure
and selects unique event and click records. `full` writes both Parquet
outputs. The tool leaves cache state under the host's control, so the first
run and repeat runs may see different input residency. Use the same
hardware, generated seed, thread count, and cache conditions when comparing
branches.
