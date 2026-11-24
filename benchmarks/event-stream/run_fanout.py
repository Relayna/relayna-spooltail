"""Run the same-task SSE comparison in an isolated Compose project."""

import argparse
import concurrent.futures
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from run import sample  # Existing benchmark cgroup sampler.


def output(*args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument(
        "--sdk", type=Path, required=True, help="Relayna SDK v1.14.0 checkout"
    )
    parser.add_argument("--publisher", choices=["python", "go"], default="python")
    parser.add_argument(
        "--live-rate", type=int, default=40, help="target events/s for live-N cases"
    )
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument(
        "--live-events", type=int, default=48, help="paced updates per live-N case"
    )
    parser.add_argument(
        "--cases",
        nargs="+",
        default=[
            "live-1",
            "live-16",
            "live-64",
            "live-256",
            "large-32",
            "slow-32",
            "resume-64",
            "resume-256",
            "resume-256-publishing",
        ],
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.trials < 1 or args.output.exists():
        parser.error("use at least one trial and a new output path")
    if not 2 <= args.live_events <= 2048:
        parser.error("--live-events must be between 2 and 2048")
    if args.live_rate < 1:
        parser.error("--live-rate must be positive")
    sdk = args.sdk.resolve()
    project = "spoolfan" + uuid.uuid4().hex[:10]
    env = dict(
        os.environ,
        RELAYNA_SDK_PATH=str(sdk),
        BENCH_REDIS_POOL_SIZE="650",
        BENCH_TRIALS="1",
    )
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(HERE.parent / "compose.yaml"),
    ]
    mount = [
        "-v",
        f"{HERE}:/experiment:ro",
        "-e",
        "PYTHONPATH=/bench",
        "-e",
        f"BENCH_LIVE_EVENTS={args.live_events}",
        "-e",
        f"BENCH_LIVE_RATE={args.live_rate}",
        "--entrypoint",
        "python",
    ]
    results = {
        "started_utc": datetime.now(UTC).isoformat(),
        "trials": args.trials,
        "publisher": args.publisher,
        "cases": args.cases,
        "live_events": args.live_events,
        "live_rate": args.live_rate,
        "spooltail_commit": output("git", "-C", HERE, "rev-parse", "HEAD"),
        "sdk_commit": output("git", "-C", sdk, "rev-parse", "HEAD"),
        "docker": output(
            "docker",
            "info",
            "--format",
            "{{.ServerVersion}} {{.OSType}}/{{.Architecture}} {{.NCPU}} CPUs",
        ),
        "runner_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [Path(__file__), HERE / "fanout.py"]
        },
        "runs": [],
    }

    def run(*cmd):
        subprocess.run([*compose, *cmd], env=env, check=True)

    with (
        tempfile.TemporaryDirectory() as bind,
        concurrent.futures.ThreadPoolExecutor() as pool,
    ):
        if args.publisher == "go":
            platform = output("docker", "version", "--format", "{{.Server.Arch}}")
            sources = HERE / "publisher"
            subprocess.run(
                [
                    "go",
                    "build",
                    "-mod=readonly",
                    "-o",
                    str(Path(bind) / "native-publish"),
                    ".",
                ],
                cwd=sources,
                env=dict(env, GOOS="linux", GOARCH=platform, CGO_ENABLED="0"),
                check=True,
            )
            mount[:0] = ["-v", f"{bind}:/publisher:ro", "-e", "BENCH_PUBLISHER=go"]
            results["go_version"] = output("go", "version")
            results["runner_sha256"].update(
                {
                    f"publisher/{p.name}": hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in sorted(sources.glob("*"))
                    if p.is_file()
                }
            )
        hub = None
        try:
            run("build", "spool", "driver")
            run("up", "-d", "--wait", "spool", "rabbit", "redis", "sdk-api")
            hub = output(
                *compose,
                "run",
                "-d",
                "--no-deps",
                *mount,
                "driver",
                "/experiment/fanout.py",
                "hub",
                env=env,
            )
            ids = {
                name: output(*compose, "ps", "-q", name, env=env)
                for name in ["spool", "rabbit", "redis", "sdk-api"]
            }
            ids["hub"] = hub
            results["images"] = {
                name: output("docker", "inspect", "--format", "{{.Image}}", cid)
                for name, cid in ids.items()
            }
            for trial in range(args.trials):
                pivot = trial % len(args.cases)
                cases = args.cases[pivot:] + args.cases[:pivot]
                for case in cases:
                    arms = ["spooltail", "rabbit-redis"]
                    if (trial + args.cases.index(case)) % 2:
                        arms.reverse()
                    for arm in arms:
                        # Task IDs are identical between stacks, unique between cases and trials.
                        cmd = [
                            *compose,
                            "run",
                            "--rm",
                            "--no-deps",
                            "-T",
                            *mount,
                            "driver",
                            "/experiment/fanout.py",
                            "trial",
                            case,
                            arm,
                            f"t{trial}-{case}",
                        ]
                        row = None
                        with subprocess.Popen(
                            cmd,
                            env=env,
                            text=True,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                        ) as proc:
                            for line in proc.stdout:
                                msg = json.loads(line)
                                if "sample" in msg:
                                    names = msg["sample"]
                                    counters = dict(
                                        zip(
                                            names,
                                            pool.map(sample, [ids[n] for n in names]),
                                            strict=True,
                                        )
                                    )
                                    print(
                                        json.dumps(counters),
                                        file=proc.stdin,
                                        flush=True,
                                    )
                                else:
                                    row = msg["result"]
                            if proc.wait() or row is None:
                                row = {
                                    "case": case,
                                    "arm": arm,
                                    "error": "driver failed; see run stderr",
                                }
                        row["trial"] = trial + 1
                        results["runs"].append(row)
                        args.output.write_text(json.dumps(results, indent=2) + "\n")
                        print(
                            trial + 1,
                            case,
                            arm,
                            row.get("fast_delivery_ms", row),
                            flush=True,
                        )
        finally:
            if hub:
                subprocess.run(["docker", "rm", "-f", hub], check=True)
            subprocess.run(
                [*compose, "down", "-v", "--remove-orphans", "--rmi", "local"],
                env=env,
                check=True,
            )
    if any("error" in row for row in results["runs"]):
        sys.exit("one or more incomplete trials")


if __name__ == "__main__":
    main()
