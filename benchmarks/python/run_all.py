"""Run all benchmarks and generate a comparison report.

Usage:
    python run_all.py              # Run all benchmarks
    python run_all.py triggers     # Run only trigger benchmark
    python run_all.py scheduler    # Run only scheduler ops benchmark
    python run_all.py latency      # Run only wakeup latency benchmark
    python run_all.py memory       # Run only memory benchmark
"""
import subprocess
import sys
import os

BENCHMARKS = {
    "triggers": "bench_triggers.py",
    "scheduler": "bench_scheduler_ops.py",
    "latency": "bench_wakeup_latency.py",
    "memory": "bench_memory.py",
}


def run_benchmark(script):
    """Run a benchmark script and capture output."""
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), script)
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    print(result.stdout)
    if result.stderr:
        print(f"STDERR: {result.stderr}", file=sys.stderr)
    return result.returncode


def main():
    print("=" * 60)
    print("apscheduler-rs Benchmark Suite")
    print("=" * 60)
    print()

    # Determine which benchmarks to run
    if len(sys.argv) > 1:
        selected = sys.argv[1:]
        to_run = []
        for name in selected:
            if name in BENCHMARKS:
                to_run.append((name, BENCHMARKS[name]))
            else:
                print(f"Unknown benchmark: {name}")
                print(f"Available: {', '.join(BENCHMARKS.keys())}")
                sys.exit(1)
    else:
        to_run = list(BENCHMARKS.items())

    failed = []
    for name, script in to_run:
        print(f"--- Running {script} ---")
        rc = run_benchmark(script)
        if rc != 0:
            print(f"  FAILED (exit code {rc})")
            failed.append(name)
        print()

    print("=" * 60)
    if failed:
        print(f"Completed with failures: {', '.join(failed)}")
    else:
        print("All benchmarks complete.")
    print()
    print("To compare against APScheduler, install it in a separate venv")
    print("and run the same benchmarks. Results are saved as JSON files")
    print("in the current directory.")
    print("=" * 60)


if __name__ == "__main__":
    main()
