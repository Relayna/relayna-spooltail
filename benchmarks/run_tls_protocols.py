"""Compare verified TLS transports with clients in Docker Desktop and a second Lima VM."""

import argparse
import datetime
import hashlib
import json
import subprocess
import tempfile
import uuid
from pathlib import Path

from run import HERE, ROOT, output, sample
from run_protocols import options

WIRES = ("http1", "http2", "grpc")
PHASES = (
    "empty-rpc",
    "cold-latest",
    "latest-serial",
    "latest-1000",
    "latest-16",
    "latest-64",
    "latest-16k",
    "history-6",
    "replay-6",
    "mixed-10pct-writes",
    "durable-append",
    "worker-lifecycle",
    "live-16",
)
DOCKER = ["docker"]
TLS_PROBE = """
import hashlib
import json
import os
import socket
import ssl

host, port = os.environ["SPOOLTAIL_BENCH_TARGET"].split(":")
context = ssl.create_default_context(cafile="/tmp/spooltail-ca.crt")
expected = os.environ["EXPECTED_ALPN"]
context.set_alpn_protocols([expected])
with socket.create_connection((host, int(port)), timeout=5) as link:
    with context.wrap_socket(link, server_hostname=host) as tls:
        assert tls.selected_alpn_protocol() == expected
        print(json.dumps({
            "peer": tls.getpeername()[0], "version": tls.version(),
            "cipher": tls.cipher()[0], "alpn": tls.selected_alpn_protocol(),
            "leaf_sha256": hashlib.sha256(tls.getpeercert(binary_form=True)).hexdigest(),
        }))
with socket.create_connection((host, int(port)), timeout=5) as link:
    try:
        context.wrap_socket(link, server_hostname="wrong.spooltail.test")
    except ssl.SSLCertVerificationError:
        pass
    else:
        raise RuntimeError("wrong TLS hostname was accepted")
"""


def certificates(directory):
    certs = {
        name: directory / name
        for name in ("ca.key", "ca.crt", "server.key", "server.crt", "server.csr")
    }
    extensions = directory / "leaf.ext"
    extensions.write_text(
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature\n"
        "extendedKeyUsage=serverAuth\n"
        "subjectKeyIdentifier=hash\n"
        "authorityKeyIdentifier=keyid:always\n"
        "subjectAltName=DNS:http1,DNS:http2,DNS:grpc,DNS:host.lima.internal\n"
    )
    commands = [
        [
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-noenc",
            "-days",
            "2",
            "-subj",
            "/CN=Spooltail benchmark CA",
            "-keyout",
            certs["ca.key"],
            "-out",
            certs["ca.crt"],
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-addext",
            "subjectKeyIdentifier=hash",
        ],
        [
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-noenc",
            "-keyout",
            certs["server.key"],
            "-out",
            certs["server.csr"],
            "-subj",
            "/CN=Spooltail benchmark",
        ],
        [
            "x509",
            "-req",
            "-in",
            certs["server.csr"],
            "-CA",
            certs["ca.crt"],
            "-CAkey",
            certs["ca.key"],
            "-CAcreateserial",
            "-days",
            "2",
            "-extfile",
            extensions,
            "-out",
            certs["server.crt"],
        ],
    ]
    for cmd in commands:
        subprocess.run(["openssl", *cmd], check=True, stdout=subprocess.DEVNULL)


def copy_image(image, remote):
    with subprocess.Popen(["docker", "save", image], stdout=subprocess.PIPE) as source:
        try:
            subprocess.run([*remote, "load"], stdin=source.stdout, check=True)
        finally:
            source.stdout.close()
        if source.wait():
            raise RuntimeError("client image export failed")
    local_id = output("docker", "image", "inspect", "--format", "{{.Id}}", image)
    remote_id = output(*remote, "image", "inspect", "--format", "{{.Id}}", image)
    local_layers = json.loads(
        output(
            "docker", "image", "inspect", "--format", "{{json .RootFS.Layers}}", image
        )
    )
    remote_layers = json.loads(
        output(
            *remote, "image", "inspect", "--format", "{{json .RootFS.Layers}}", image
        )
    )
    if not local_layers or local_layers != remote_layers:
        raise RuntimeError("client filesystem layers differ across VMs")
    return {
        "docker-desktop_id": local_id,
        "lima_id": remote_id,
        "rootfs_layers": local_layers,
    }


def machine(command, image):
    info = json.loads(
        output(
            *command,
            "info",
            "--format",
            '{"version":"{{.ServerVersion}}","architecture":"{{.Architecture}}","cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}},"kernel":"{{.KernelVersion}}"}',
        )
    )
    boot = output(
        *command,
        "run",
        "--rm",
        "--entrypoint",
        "cat",
        image,
        "/proc/sys/kernel/random/boot_id",
    )
    info["boot_id"] = boot
    return info


def measure(prefix, image, container, wire, args, trial, topology):
    cmd = [
        *prefix,
        "-i",
        image,
        "--wire",
        wire,
        "--trial",
        str(trial),
        f"--label=-{topology}",
        "--live-rounds",
        str(args.live_rounds),
        "--phases",
        *args.phases,
        *options(args),
    ]
    with subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    ) as proc:
        result = None
        try:
            for line in proc.stdout:
                msg = json.loads(line)
                if msg.get("sample") == wire:
                    counters = sample(container, memory_detail=True, network=True)
                    print(json.dumps(counters), file=proc.stdin, flush=True)
                else:
                    result = msg["result"]
        except BaseException:
            proc.terminate()
            raise
        finally:
            proc.stdin.close()
        if proc.wait() or result is None:
            raise RuntimeError(f"trial {trial + 1} {topology} {wire} failed")
    return result


def run(args, directory):
    name = "spooltls-" + uuid.uuid4().hex[:10]
    image = f"{name}-client"
    broker_image = f"{name}-broker"
    shell = ["limactl", "shell", args.lima]
    remote = [*shell, "sudo", "-n", "docker"]
    remote_tmp = output(*shell, "mktemp", "-d", f"/tmp/{name}.XXXXXX")
    containers = {}
    volumes = []
    network_created = False

    def call(*cmd):
        subprocess.run(cmd, cwd=ROOT, check=True)

    def prefix(wire, topology, port):
        target = (
            f"{wire}:7474" if topology == "same-vm" else f"host.lima.internal:{port}"
        )
        command = DOCKER if topology == "same-vm" else remote
        network = name if topology == "same-vm" else "host"
        ca = directory / "ca.crt" if topology == "same-vm" else f"{remote_tmp}/ca.crt"
        return [
            *command,
            "run",
            "--rm",
            "--cpuset-cpus",
            "0-1",
            "--network",
            network,
            "--volume",
            f"{ca}:/tmp/spooltail-ca.crt:ro",
            "--env",
            "SPOOLTAIL_BENCH_TLS_CA=/tmp/spooltail-ca.crt",
            "--env",
            f"SPOOLTAIL_BENCH_TARGET={target}",
        ]

    try:
        certificates(directory)
        call(
            "docker",
            "build",
            "--target",
            "transport-broker",
            "--build-arg",
            "SPOOLTAIL_TRANSPORT_FEATURES=bench-tls",
            "-f",
            "benchmarks/Dockerfile",
            "-t",
            broker_image,
            ".",
        )
        call(
            "docker",
            "build",
            "--target",
            "transport-driver",
            "-f",
            "benchmarks/Dockerfile",
            "-t",
            image,
            ".",
        )
        image_id = copy_image(image, remote)
        call(
            "limactl",
            "copy",
            str(directory / "ca.crt"),
            f"{args.lima}:{remote_tmp}/ca.crt",
        )
        hosts = {
            "docker-desktop": machine(DOCKER, image),
            "lima": machine(remote, image),
        }
        if hosts["docker-desktop"]["boot_id"] == hosts["lima"]["boot_id"]:
            raise RuntimeError("benchmark guests share a Linux kernel")
        call("docker", "network", "create", name)
        network_created = True
        ports = {}
        for wire in WIRES:
            container = f"{name}-{wire}"
            volume = container + "-data"
            call("docker", "volume", "create", volume)
            volumes.append(volume)
            grpc = (
                ["--env", "SPOOLTAIL_BENCH_GRPC=0.0.0.0:7474"] if wire == "grpc" else []
            )
            containers[wire] = container
            call(
                "docker",
                "run",
                "-d",
                "--name",
                container,
                "--network",
                name,
                "--network-alias",
                wire,
                "--publish",
                "127.0.0.1::7474",
                "--volume",
                f"{volume}:/data",
                "--volume",
                f"{directory / 'server.crt'}:/tls/server.crt:ro",
                "--volume",
                f"{directory / 'server.key'}:/tls/server.key:ro",
                "--env",
                "SPOOLTAIL_BENCH_CERT=/tls/server.crt",
                "--env",
                "SPOOLTAIL_BENCH_KEY=/tls/server.key",
                *grpc,
                broker_image,
            )
            ports[wire] = output("docker", "port", container, "7474/tcp").rsplit(
                ":", 1
            )[-1]
        result = {
            "started_utc": datetime.datetime.now(datetime.UTC).isoformat(),
            "settings": {
                **{k: v for k, v in vars(args).items() if k != "output"},
                "wires": list(WIRES),
            },
            "hosts": hosts,
            "client_image": image_id,
            "broker_image": output(
                "docker", "image", "inspect", "--format", "{{.Id}}", broker_image
            ),
            "source_sha256": {
                str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in [
                    *(ROOT / "src").glob("*.rs"),
                    *(HERE / "protocols").glob("*.py"),
                    ROOT / "proto/spooltail/v1/spooltail.proto",
                    HERE / "Dockerfile",
                    HERE / "metrics.py",
                    HERE / "run.py",
                    HERE / "run_protocols.py",
                    Path(__file__).resolve(),
                    ROOT / "Cargo.toml",
                    ROOT / "Cargo.lock",
                    ROOT / "build.rs",
                ]
            },
            "tls_probes": {},
            "runs": [],
        }
        topologies = ("same-vm", "cross-vm")
        for topology in topologies:
            result["tls_probes"][topology] = {}
            for wire in WIRES:
                alpn = "http/1.1" if wire == "http1" else "h2"
                probe = output(
                    *prefix(wire, topology, ports[wire]),
                    "--env",
                    f"EXPECTED_ALPN={alpn}",
                    "--entrypoint",
                    "python",
                    image,
                    "-c",
                    TLS_PROBE,
                )
                result["tls_probes"][topology][wire] = json.loads(probe)
        fingerprints = {
            probe["leaf_sha256"]
            for probes in result["tls_probes"].values()
            for probe in probes.values()
        }
        if len(fingerprints) != 1:
            raise RuntimeError("the protocols negotiated different TLS certificates")
        for wire in WIRES:
            call(
                *prefix(wire, "same-vm", ports[wire]),
                image,
                "--seed-only",
                "--wires",
                wire,
                *options(args),
            )
        for trial in range(args.trials):
            record = {"trial": trial + 1, "topologies": {}}
            offset = trial % len(WIRES)
            order = WIRES[offset:] + WIRES[:offset]
            for topology in topologies[:: 1 if trial % 2 == 0 else -1]:
                print(f"Trial {trial + 1}, {topology}", flush=True)
                record["topologies"][topology] = {
                    "arms": {
                        wire: measure(
                            prefix(wire, topology, ports[wire]),
                            image,
                            containers[wire],
                            wire,
                            args,
                            trial,
                            topology,
                        )
                        for wire in order
                    }
                }
            result["runs"].append(record)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
    except BaseException:
        for container in containers.values():
            subprocess.run(["docker", "logs", "--tail", "15", container], check=False)
        raise
    finally:
        cleanup = [
            *(["docker", "rm", "-f", container] for container in containers.values()),
            *(["docker", "volume", "rm", volume] for volume in volumes),
            *(["docker", "network", "rm", name] for _ in range(network_created)),
            ["docker", "image", "rm", image, broker_image],
            [*remote, "image", "rm", image],
            [*shell, "rm", "-f", f"{remote_tmp}/ca.crt"],
            [*shell, "rmdir", remote_tmp],
        ]
        for cmd in cleanup:
            subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lima",
        required=True,
        help="running Linux client VM with Docker and passwordless sudo",
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--phases", nargs="+", choices=PHASES, default=PHASES)
    for key, default in [
        ("trials", 3),
        ("tasks", 1000),
        ("large-tasks", 150),
        ("requests", 5000),
        ("seconds", 4),
        ("seed", 1729),
        ("timeout", 800),
        ("live-rounds", 6),
    ]:
        parser.add_argument("--" + key, type=int, default=default)
    args = parser.parse_args()
    if (
        min(
            args.trials,
            args.tasks,
            args.large_tasks,
            args.seconds,
            args.timeout,
            args.live_rounds,
        )
        < 1
        or args.requests < 64
    ):
        parser.error("use positive workload counts and at least 64 requests")
    with tempfile.TemporaryDirectory(prefix="spooltail-tls-") as tmp:
        run(args, Path(tmp).resolve())
