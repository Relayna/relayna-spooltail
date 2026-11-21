"""Build and run the benchmark in a private Compose project. Writes results as JSON."""

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
import subprocess
import time
import uuid
from collections import Counter
from pathlib import Path
from statistics import median

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
WORKLOAD_ARGS = {
    "recovery": ("prefix", "offline", "tasks"),
    "random-reads": ("read_tasks", "statuses", "readers", "reads_per_reader", "seed"),
    "load-generators": ("read_tasks", "statuses", "load_shapes", "requests", "seed"),
    "read-resources": ("read_tasks", "statuses", "readers", "rates", "seconds", "seed"),
}


def output(*cmd):
    return subprocess.check_output(cmd, text=True).strip()


def sample(container, memory_detail=False, network=False):
    raw = output(
        "docker",
        "exec",
        container,
        "cat",
        "/sys/fs/cgroup/cpu.stat",
        "/sys/fs/cgroup/io.stat",
        *(["/sys/fs/cgroup/memory.stat"] if memory_detail else []),
        "/sys/fs/cgroup/memory.current",
        *(
            [
                "/sys/class/net/eth0/statistics/rx_bytes",
                "/sys/class/net/eth0/statistics/tx_bytes",
            ]
            if network
            else []
        ),
    )
    cpu, reads, writes = {}, 0, 0
    lines = raw.splitlines()
    if network:
        *lines, rx_bytes, tx_bytes = lines
    memory = lines.pop()
    for line in lines:
        key, *fields = line.split()
        if ":" in key:
            io = dict(field.split("=") for field in fields)
            reads += int(io.get("rbytes", 0))
            writes += int(io.get("wbytes", 0))
        else:
            cpu[key] = int(fields[0])
    counters = {
        "memory_bytes": int(memory),
        "cpu_s": cpu["usage_usec"] / 1e6,
        "throttled_s": cpu.get("throttled_usec", 0) / 1e6,
        "block_read_bytes": reads,
        "block_write_bytes": writes,
    }
    if memory_detail:
        counters.update(anon_bytes=cpu["anon"], file_cache_bytes=cpu["file"])
    if network:
        counters.update(rx_bytes=int(rx_bytes), tx_bytes=int(tx_bytes))
    return counters


def run_trial(compose, ids, args, trial, arm=None):
    cmd = [*compose, "run", "--rm", "--no-deps", "-T"]
    if args.workload != "recovery":
        script = args.workload.replace("-", "_") + ".py"
        cmd.extend(["--entrypoint", "python", "driver", script])
    else:
        cmd.append("driver")
    cmd.extend(["--trial", str(trial)])
    if arm:
        cmd.extend(["--arm", arm])
    for key in (*WORKLOAD_ARGS[args.workload], "timeout"):
        cmd.extend(["--" + key.replace("_", "-"), str(getattr(args, key))])
    result = None
    with subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    ) as proc:
        try:
            for line in proc.stdout:
                msg = json.loads(line)
                if "sample" in msg:
                    names = msg["sample"]
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        counters = dict(
                            zip(
                                names,
                                pool.map(
                                    sample,
                                    [ids[name] for name in names],
                                    [args.workload == "read-resources"] * len(names),
                                ),
                                strict=True,
                            )
                        )
                    print(json.dumps(counters), file=proc.stdin, flush=True)
                elif "await_consumer" in msg:
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        queues = output(
                            "docker",
                            "exec",
                            ids["rabbit"],
                            "rabbitmqctl",
                            "-q",
                            "list_queues",
                            "name",
                            "consumers",
                        )
                        if msg["await_consumer"] + "\t1" in queues.splitlines():
                            break
                        time.sleep(0.05)
                    else:
                        raise RuntimeError("RabbitMQ consumer did not register")
                    print("true", file=proc.stdin, flush=True)
                else:
                    result = msg["result"]
        except BaseException:
            proc.terminate()
            raise
        finally:
            proc.stdin.close()
        if proc.wait() or not result:
            raise RuntimeError(f"benchmark trial {trial + 1} failed")
    return result


def summary(result):
    """Render medians for runs in which every trial supplied the metric."""

    def metric(rows, key):
        values = [row[key] for row in rows]
        return "—" if None in values else f"{median(values):,.1f}"

    lines = [
        "| Recovery arm | Downtime IDs recovered | All-record catch-up (ms) | First missed task status (ms) |",
        "| --- | ---: | ---: | ---: |",
    ]
    runs = result["runs"]
    for mode in runs[0]["modes"]:
        rows = [run["modes"][mode] for run in runs]
        counts = sorted({row["recovered"] for row in rows})
        recovered = str(counts[0]) if len(counts) == 1 else f"{counts[0]}–{counts[-1]}"
        lines.append(
            f"| {mode} | {recovered} / {result['settings']['offline']} | "
            f"{metric(rows, 'catch_up_ms')} | {metric(rows, 'reconnect_to_missed_status_ms')} |"
        )
    return "\n".join(lines)


def reads_summary(result):
    lines = [
        "| Read | Readers | Stack | p50 (ms) | p95 (ms) | p99 (ms) | Reads/s |",
        "| --- | ---: | --- | ---: | ---: | ---: | ---: |",
    ]
    include_failures = result["settings"]["workload"] == "load-generators"
    if include_failures:
        lines[0] += " Failures across trials |"
        lines[1] += " ---: |"
    for scenarios in zip(*(run["scenarios"] for run in result["runs"]), strict=True):
        first = scenarios[0]
        label = first["operation"]
        if "generators" in first:
            label += f" ({first['generators']} generators)"
        for arm in first["arms"]:
            values = []
            for metric in ("p50_ms", "p95_ms", "p99_ms", "requests_per_s"):
                samples = [row["arms"][arm][metric] for row in scenarios]
                values.append("—" if None in samples else f"{median(samples):.2f}")
            if include_failures:
                values.append(
                    str(
                        sum(
                            sum(row["arms"][arm]["errors"].values())
                            for row in scenarios
                        )
                    )
                )
            lines.append(
                f"| {label} | {first['readers']} | {arm} | " + " | ".join(values) + " |"
            )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", choices=WORKLOAD_ARGS, default="recovery")
    parser.add_argument(
        "--rates",
        default="0,100,1000",
        help="offered reads per second for read-resources (0 measures idle)",
    )
    parser.add_argument(
        "--seconds", type=int, default=12, help="seconds per rate on read-resources"
    )
    parser.add_argument(
        "--load-shapes",
        default="1x16,2x16,4x16,8x16,4x64,8x64,8x128",
        help="comma-separated generators x total concurrent readers for load-generators",
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=16000,
        help="timed requests per stack, load shape and trial",
    )
    for key, default in [
        ("prefix", 2000),
        ("offline", 200),
        ("tasks", 50),
        ("trials", 3),
        ("timeout", 3600),
    ]:
        parser.add_argument("--" + key, type=int, default=default)
    parser.add_argument(
        "--read-tasks",
        type=int,
        default=10000,
        help="distinct existing task IDs in the random-read corpus",
    )
    parser.add_argument(
        "--statuses",
        type=int,
        default=6,
        help="retained statuses per task, from 1 to 50",
    )
    parser.add_argument(
        "--readers",
        type=int,
        default=16,
        help="concurrent readers to test alongside one reader",
    )
    parser.add_argument(
        "--reads-per-reader",
        type=int,
        default=160,
        help="timed requests per reader per scenario and stack",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1729,
        help="seed for reproducible random request lists",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.trials, args.timeout) < 1 or (
        args.workload == "recovery" and min(args.prefix, args.offline, args.tasks) < 1
    ):
        parser.error("counts and timeout must be positive")
    if args.workload != "recovery" and (
        args.read_tasks < 2 or not 1 <= args.statuses <= 50
    ):
        parser.error("random reads need at least two tasks and 1..50 statuses")
    if args.workload == "random-reads" and min(args.readers, args.reads_per_reader) < 1:
        parser.error("reader and request counts must be positive")
    if args.workload == "read-resources":
        try:
            rates = list(map(int, args.rates.split(",")))
            if (
                min(rates) < 0
                or len(set(rates)) != len(rates)
                or min(args.readers, args.seconds) < 1
            ):
                raise ValueError
        except ValueError:
            parser.error(
                "use distinct nonnegative rates, positive readers and positive seconds"
            )
    os.environ["BENCH_REDIS_POOL_SIZE"] = "0"
    if args.workload == "load-generators":
        try:
            shapes = [
                tuple(map(int, shape.split("x")))
                for shape in args.load_shapes.split(",")
            ]
            if len(set(shapes)) != len(shapes) or not all(
                1 <= generators <= readers <= args.requests
                for generators, readers in shapes
            ):
                raise ValueError
        except ValueError:
            parser.error(
                "use distinct load shapes such as 2x16,4x64 with 1 <= generators <= readers <= requests"
            )
        os.environ["BENCH_REDIS_POOL_SIZE"] = str(max(readers for _, readers in shapes))
    os.environ["SPOOLTAIL_BENCH_FEATURES"] = (
        "bench-lock-timing" if args.workload == "load-generators" else ""
    )
    sdk = Path(os.environ.get("RELAYNA_SDK_PATH", ROOT.parent / "relayna")).resolve()
    os.environ["RELAYNA_SDK_PATH"] = str(sdk)
    if not (sdk / "uv.lock").is_file():
        parser.error("set RELAYNA_SDK_PATH to the Relayna SDK checkout")
    os.environ["BENCH_TRIALS"] = str(args.trials)
    project = "spoolbench-" + uuid.uuid4().hex[:12]
    compose = ["docker", "compose", "-p", project, "-f", str(HERE / "compose.yaml")]

    def call(*args):
        subprocess.run([*compose, *args], check=True)

    try:
        call("build", "spool", "driver")
        call("up", "-d", "--wait", "rabbit", "redis", "spool", "sdk-api")
        ids = {
            name: output(*compose, "ps", "-q", name)
            for name in ["rabbit", "redis", "spool", "sdk-api"]
        }
        result = {
            "started_utc": datetime.datetime.now(datetime.UTC).isoformat(),
            "settings": {
                k: getattr(args, k)
                for k in (
                    "workload",
                    "trials",
                    "timeout",
                    *WORKLOAD_ARGS[args.workload],
                )
            },
            "sdk_revision": output("git", "-C", str(sdk), "rev-parse", "HEAD"),
            "spooltail_revision": output("git", "-C", str(ROOT), "rev-parse", "HEAD"),
            "broker_source_sha256": {
                p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted((ROOT / "src").glob("*.rs"))
            },
            "engine": json.loads(
                output(
                    "docker",
                    "info",
                    "--format",
                    '{"version":"{{.ServerVersion}}","architecture":"{{.Architecture}}","cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}}}',
                )
            ),
            "service_images": {
                k: output(
                    "docker", "inspect", "--format", "{{.Config.Image}} {{.Image}}", v
                )
                for k, v in ids.items()
            },
            "rabbitmq": output(
                "docker", "exec", ids["rabbit"], "rabbitmqctl", "version"
            ),
            "runs": [],
        }
        if args.workload == "read-resources":
            call(
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--entrypoint",
                "python",
                "driver",
                "read_resources.py",
                "--seed-only",
                "--read-tasks",
                str(args.read_tasks),
                "--statuses",
                str(args.statuses),
                "--timeout",
                str(args.timeout),
            )
        for trial in range(args.trials):
            if args.workload == "read-resources":
                arms = (
                    ("spooltail", "sdk-once")
                    if trial % 2 == 0
                    else ("sdk-once", "spooltail")
                )
                result["runs"].append(
                    {
                        "trial": trial + 1,
                        "arms": {
                            arm: run_trial(compose, ids, args, trial, arm)
                            for arm in arms
                        },
                    }
                )
            else:
                result["runs"].append(run_trial(compose, ids, args, trial))
        if args.workload == "load-generators":
            logs = subprocess.run(
                ["docker", "logs", ids["sdk-api"]],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            result["sdk_exception_counts"] = Counter(
                line
                for line in logs.stdout.splitlines()
                if line.startswith("redis.exceptions.")
            )
            result["wide_pool_connections"] = int(os.environ["BENCH_REDIS_POOL_SIZE"])
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        if args.workload == "read-resources":
            from read_resources_report import summary as resource_summary

            print(resource_summary(result))
        else:
            print(
                reads_summary(result)
                if args.workload != "recovery"
                else summary(result)
            )
    finally:
        call(
            "--profile",
            "bench",
            "down",
            "--volumes",
            "--remove-orphans",
            "--rmi",
            "local",
        )


if __name__ == "__main__":
    main()
