#!/usr/bin/env python3
"""Run the amended third-wave protocol against an explicitly dedicated Redis."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]


def output(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-container", required=True)
    parser.add_argument("--redis-addr", required=True)
    parser.add_argument("--output", default="research/third-wave/data/final")
    args = parser.parse_args()
    destination = ROOT / args.output
    destination.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GOCACHE="/tmp/taskforge-gocache-wave3",
               TASKFORGE_RUN_BENCHMARKS="1", TASKFORGE_RUN_INTEGRATION="1",
               TASKFORGE_REDIS_ADDR=args.redis_addr, TASKFORGE_REDIS_DB="14")
    env.pop("GOFLAGS", None)
    # Only the container explicitly supplied for this research run is inspected.
    info = output("docker", "exec", args.redis_container, "redis-cli", "INFO", "all")
    (destination / "redis-info-before.txt").write_text(info + "\n")
    metadata = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "go_version": output("go", "version"),
        "go_env": json.loads(output("go", "env", "-json", "GOOS", "GOARCH", "GOAMD64", "GOEXPERIMENT", "CGO_ENABLED", "GOTOOLCHAIN")),
        "kernel": platform.release(), "logical_cpus": os.cpu_count(),
        "cpu_model": next(line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")),
        "redis_image": output("docker", "inspect", "--format", "{{.Image}}", args.redis_container),
        "redis_config": output("docker", "exec", args.redis_container, "redis-cli", "CONFIG", "GET", "save", "appendonly", "databases", "maxmemory", "maxmemory-policy"),
        "redis_topology": "dedicated standalone Docker Redis over loopback TCP; persistence disabled",
        "database": 14, "iterations": 30, "repetitions": 10,
        "arm_order": ["baseline", "treatment"], "sources": {}, "commands": [],
        "protocol_sha256": digest((ROOT / "research/third-wave/analysis-plan.md").read_bytes()),
        "amendment_sha256": digest((ROOT / "research/third-wave/execution-notes.md").read_bytes()),
        "treatment_diff_sha256": digest(subprocess.check_output(["git", "diff", "--binary", "4446ab3", "b2947f3"], cwd=ROOT)),
    }

    def save():
        (destination / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")

    def run(label, source, command):
        print(label, flush=True)
        metadata["commands"].append({"label": label, "command": command})
        save()
        with (destination / (label + ".txt")).open("w") as log:
            result = subprocess.run(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
        metadata["commands"][-1]["exit_code"] = result.returncode
        save()
        data = (destination / (label + ".txt")).read_text()
        if result.returncode or "--- SKIP:" in data or "socket: operation not permitted" in data:
            raise RuntimeError(f"{label} failed or skipped; retained log, no measurement retry")

    with tempfile.TemporaryDirectory(prefix="taskforge-wave3-") as temporary:
        sources = {}
        for arm, revision in [("baseline", "4446ab3"), ("treatment", "b2947f3")]:
            source = Path(temporary) / arm
            source.mkdir()
            archive = subprocess.check_output(["git", "archive", revision], cwd=ROOT)
            with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
                tar.extractall(source, filter="data")
            provenance = {"commit": output("git", "rev-parse", revision),
                          "tree": output("git", "rev-parse", revision + "^{tree}"),
                          "archive_sha256": digest(archive), "harness": {}}
            for name in ["cleanup_benchmark_test.go", "redis_benchmark_test.go"]:
                path = "test/benchmark/" + name
                data = subprocess.check_output(["git", "show", "b2947f3:" + path], cwd=ROOT)
                (source / path).write_bytes(data)
                provenance["harness"][path] = digest(data)
            path = "redis/wave3_setup_key_test.go"
            data = (ROOT / "research/third-wave/harness/setup_key_test.go.txt").read_bytes()
            (source / path).write_bytes(data)
            provenance["harness"][path] = digest(data)
            # Endpoint-only integration overlay, recorded separately from product source.
            path = "test/integration/redis_integration_test.go"
            data = (source / path).read_text().replace("localhost:6379", args.redis_addr)
            (source / path).write_text(data)
            provenance["integration_endpoint_overlay_sha256"] = digest(data.encode())
            metadata["sources"][arm] = provenance
            sources[arm] = source
        save()
        for arm in ("baseline", "treatment"):
            run(arm + "-correctness", sources[arm], ["go", "test", "-count=1", "-v", "./redis", "./worker", "./test/integration"])
        for arm in ("baseline", "treatment"):
            run(arm, sources[arm], ["go", "test", "-run", "^$", "-bench", "^Benchmark(SnapshotCosts|PublishStateCosts|PublishThroughput)$", "-benchmem", "-benchtime=30x", "-count=10", "./test/benchmark"])
        for arm in ("baseline", "treatment"):
            run(arm + "-setup", sources[arm], ["go", "test", "-run", "^$", "-bench", "^BenchmarkSetupKeyCosts$", "-benchmem", "-benchtime=30x", "-count=10", "./redis"])
    (destination / "redis-info-after.txt").write_text(output("docker", "exec", args.redis_container, "redis-cli", "INFO", "all") + "\n")
    metadata["completed_utc"] = datetime.now(timezone.utc).isoformat()
    save()
    print("Raw run complete. Run make third-wave-analysis and inspect the decision gates.", flush=True)


if __name__ == "__main__":
    main()
