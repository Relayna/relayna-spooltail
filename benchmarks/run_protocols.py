"""Build and run the transport comparison inside a private Compose project."""

import argparse
import datetime
import hashlib
import json
import subprocess
import uuid

from run import HERE, ROOT, output, sample

WIRES = ("http1", "http-uds", "http2", "grpc", "grpc-uds")


def driver(compose, containers, args, trial, wire):
    cmd = [
        *compose,
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "driver",
        "--trial",
        str(trial),
        "--wire",
        wire,
        *options(args),
    ]
    if args.live_only:
        cmd.append("--live-only")
    result = None
    with subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    ) as proc:
        try:
            for line in proc.stdout:
                message = json.loads(line)
                if "sample" in message:
                    if message["sample"] != wire:
                        raise RuntimeError("sample request named the wrong container")
                    counters = sample(
                        containers[wire], memory_detail=True, network=True
                    )
                    print(json.dumps(counters), file=proc.stdin, flush=True)
                else:
                    result = message["result"]
        except BaseException:
            proc.terminate()
            raise
        finally:
            proc.stdin.close()
        if proc.wait() or not result:
            raise RuntimeError(f"trial {trial + 1}, {wire} failed")
    return result


def options(args):
    result = []
    for key in ("tasks", "large_tasks", "requests", "seconds", "seed", "timeout"):
        result.extend(["--" + key.replace("_", "-"), str(getattr(args, key))])
    return result


def run(args):
    project = f"spoolprotocol-{uuid.uuid4().hex[:10]}"
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(HERE / "protocols/compose.yaml"),
    ]

    def call(*rest):
        subprocess.run([*compose, *rest], check=True)

    try:
        call("build", "http1", "driver")
        call("up", "-d", "--no-build", *args.wires)
        containers = {w: output(*compose, "ps", "-q", w) for w in args.wires}
        result = {
            "started_utc": datetime.datetime.now(datetime.UTC).isoformat(),
            "settings": {k: v for k, v in vars(args).items() if k != "output"},
            "spooltail_revision": output("git", "-C", str(ROOT), "rev-parse", "HEAD"),
            "engine": json.loads(
                output(
                    "docker",
                    "info",
                    "--format",
                    '{"version":"{{.ServerVersion}}","architecture":"{{.Architecture}}","cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}}}',
                )
            ),
            "broker_image": output(
                "docker", "inspect", "--format", "{{.Image}}", containers[args.wires[0]]
            ),
            "source_sha256": {
                str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(
                    [
                        *(ROOT / "src").glob("*.rs"),
                        *(HERE / "protocols").glob("*.py"),
                        ROOT / "proto/spooltail/v1/spooltail.proto",
                        HERE / "protocols/compose.yaml",
                        HERE / "Dockerfile",
                        HERE / "metrics.py",
                        HERE / "run.py",
                        HERE / "run_protocols.py",
                        ROOT / "Cargo.lock",
                        ROOT / "Cargo.toml",
                        ROOT / "build.rs",
                    ]
                )
            },
            "runs": [],
        }
        call(
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "driver",
            "--seed-only",
            "--wires",
            *args.wires,
            *options(args),
        )
        for trial in range(args.trials):
            offset = trial % len(args.wires)
            order = args.wires[offset:] + args.wires[:offset]
            result["runs"].append(
                {
                    "trial": trial + 1,
                    "order": order,
                    "arms": {
                        wire: driver(compose, containers, args, trial, wire)
                        for wire in order
                    },
                }
            )
        call("stop", "--timeout", "2", *args.wires)
        call(
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--entrypoint",
            "python",
            "driver",
            "-c",
            "from pathlib import Path; Path('/sockets/http.sock').unlink(missing_ok=True); Path('/sockets/grpc.sock').unlink(missing_ok=True)",
        )
        call("up", "-d", "--no-build", *args.wires)
        call(
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "driver",
            "--verify-restart",
            "--wires",
            *args.wires,
            *options(args),
        )
        result["restart_checked"] = args.wires
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    except Exception:
        subprocess.run([*compose, "logs", "--tail", "30", *args.wires], check=False)
        raise
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
    from pathlib import Path

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--wires", nargs="+", choices=WIRES, default=list(WIRES))
    p.add_argument("--output", required=True, type=Path)
    p.add_argument(
        "--live-only", action="store_true", help="Run only the live-delivery window"
    )
    for name, default in [
        ("trials", 3),
        ("tasks", 1500),
        ("large-tasks", 250),
        ("requests", 10000),
        ("seconds", 6),
        ("seed", 1729),
        ("timeout", 800),
    ]:
        p.add_argument("--" + name, type=int, default=default)
    args = p.parse_args()
    if (
        min(args.trials, args.tasks, args.large_tasks, args.seconds, args.timeout) < 1
        or args.requests < 64
    ):
        p.error("use positive workload counts and at least 64 requests")
    if len(set(args.wires)) != len(args.wires):
        p.error("list distinct protocols")
    run(args)
