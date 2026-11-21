"""Regenerate the SDK's private bindings from the broker schema."""

from pathlib import Path
from tempfile import TemporaryDirectory

from grpc_tools import protoc

PROJECT = Path(__file__).resolve().parent
SCHEMA = PROJECT.parent / "proto/spooltail/v1"
WIRE = PROJECT / "src/relayna_spooltail/_wire"

if __name__ == "__main__":
    with TemporaryDirectory() as temp:
        code = protoc.main(
            [
                "protoc",
                f"-I{SCHEMA}",
                f"--python_out={temp}",
                f"--grpc_python_out={temp}",
                str(SCHEMA / "spooltail.proto"),
            ]
        )
        if code:
            raise SystemExit(code)
        # Keep the descriptor's source name used by standalone broker stubs.
        # Adjust Python module paths so the SDK also works as an installed package.
        for name, old, new in [
            (
                "spooltail_pb2.py",
                "'spooltail_pb2'",
                "'relayna_spooltail._wire.spooltail_pb2'",
            ),
            (
                "spooltail_pb2_grpc.py",
                "import spooltail_pb2 as ",
                "from . import spooltail_pb2 as ",
            ),
        ]:
            source = (Path(temp) / name).read_text(encoding="utf-8")
            if old not in source:
                raise SystemExit(f"Unexpected import layout in generated {name}")
            source = source.replace(old, new, 1).replace(
                "# Generated ", "# ruff: noqa\n# fmt: off\n# Generated ", 1
            )
            (WIRE / name).write_text(source, encoding="utf-8")
