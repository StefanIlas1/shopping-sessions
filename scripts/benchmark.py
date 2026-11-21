#!/usr/bin/env python3
"""Measure complete native process time and report the pipeline's timed stages."""

import argparse
from collections import defaultdict
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time


def main():
    options = argparse.ArgumentParser(description=__doc__)
    options.add_argument("data", type=Path)
    options.add_argument("--binary", type=Path, default=Path("target/release/shopping-sessions"))
    options.add_argument("--threads", type=int, default=8)
    options.add_argument("--runs", type=int, default=3)
    options.add_argument("--mode", action="append", choices=["scan", "dedup", "full"])
    args = options.parse_args()
    if args.runs < 1 or args.threads < 1:
        options.error("--runs and --threads must be positive")

    binary = args.binary.resolve()
    data = args.data.resolve()
    for mode in args.mode or ("scan", "dedup", "full"):
        elapsed = []
        phases = defaultdict(list)
        for _ in range(args.runs):
            with tempfile.TemporaryDirectory(prefix="shopping-bench-") as output:
                started = time.perf_counter()
                result = subprocess.run(
                    [binary, data, output],
                    env={
                        **os.environ, "SHOPPING_MODE": mode,
                        "SHOPPING_PROFILE": "1", "NCPU": str(args.threads),
                    },
                    capture_output=True, text=True,
                )
                elapsed.append((time.perf_counter() - started) * 1_000)
                if result.returncode:
                    raise SystemExit(result.stderr)
                for line in result.stderr.splitlines():
                    if line.endswith(" ms"):
                        stage, value, _ = line.split()
                        phases[stage].append(float(value))
        print(f"{mode}: median {statistics.median(elapsed):.1f} ms"
              f" ({', '.join(f'{value:.1f}' for value in elapsed)} ms)", flush=True)
        for stage, samples in phases.items():
            print(f"  {stage:26s} {statistics.median(samples):8.1f} ms", flush=True)


if __name__ == "__main__":
    main()
