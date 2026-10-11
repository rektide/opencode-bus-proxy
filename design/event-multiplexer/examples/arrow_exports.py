"""Optional offline Polars adapter for the state_exports Rust example.

Usage: python arrow_exports.py inspection.json output-directory
Explicit schemas preserve types even for empty tables. No payload bytes exported.
"""
import json
import sys
from pathlib import Path

import polars as pl


def export(source: Path, output: Path) -> None:
    image = json.loads(source.read_text())
    if image["schema_version"] != 1 or image["consistency"] != "observational":
        raise ValueError("unsupported inspection envelope")
    output.mkdir(parents=True, exist_ok=True)
    tables = {
        "subscriptions": (
            image["subscriptions"],
            {"id": pl.String, "mode": pl.String, "active": pl.Boolean,
             "interest_count": pl.UInt64, "backlog_frames": pl.UInt64,
             "backlog_bytes": pl.UInt64},
        ),
        "memberships": (
            image["memberships"], {"id": pl.String, "session": pl.String},
        ),
    }
    for name, (rows, schema) in tables.items():
        plan = (
            pl.DataFrame(rows, schema=schema).lazy()
            .with_columns(pl.lit(image["revision"], dtype=pl.UInt64).alias("revision"))
        )
        plan.collect_schema()  # validate before execution
        result = plan.collect()  # the data, not the lazy plan, is serialized
        result.write_ipc(output / f"{name}.arrow", compression="uncompressed")
        result.write_ipc_stream(output / f"{name}.arrows", compression="uncompressed")
        restored_file = pl.scan_ipc(output / f"{name}.arrow").collect()
        restored_stream = pl.read_ipc_stream(output / f"{name}.arrows")
        assert restored_file.equals(result)
        assert restored_stream.equals(result)
    summary = (
        pl.scan_ipc(output / "subscriptions.arrow")
        .group_by("mode")
        .agg(pl.len().alias("subscriptions"), pl.col("backlog_bytes").sum())
        .collect()
    )
    (output / "manifest.json").write_text(json.dumps({
        "schema_version": 1, "revision": image["revision"],
        "consistency": image["consistency"], "polars_python": pl.__version__,
        "tables": list(tables),
    }, indent=2) + "\n")
    print(summary)


if __name__ == "__main__":
    export(Path(sys.argv[1]), Path(sys.argv[2]))
