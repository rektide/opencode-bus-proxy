"""Optional interoperability checks: Polars 2.0.0 and PyArrow 26.0.0."""
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow.ipc as ipc

from arrow_exports import export


class Exports(unittest.TestCase):
    def check_image(self, subscriptions, memberships):
        # TemporaryDirectory is placed under the explicitly approved scratch home.
        with tempfile.TemporaryDirectory(dir="/home/rektide/tmp-opencode") as root:
            root = Path(root)
            source = root / "inspection.json"
            source.write_text(json.dumps({
                "schema_version": 1, "revision": 2, "consistency": "observational",
                "subscriptions": subscriptions, "memberships": memberships,
            }))
            output = root / "arrow"
            export(source, output)
            for name in ("subscriptions", "memberships"):
                with ipc.open_file(output / f"{name}.arrow") as reader:
                    file_table = reader.read_all()
                with ipc.open_stream(output / f"{name}.arrows") as reader:
                    stream_table = reader.read_all()
                self.assertTrue(file_table.equals(stream_table))
                self.assertEqual(file_table.num_rows, len(
                    subscriptions if name == "subscriptions" else memberships))
                self.assertEqual(str(file_table.schema.field("revision").type), "uint64")
                if name == "subscriptions":
                    self.assertEqual(file_table["backlog_bytes"].to_pylist(),
                                     [row["backlog_bytes"] for row in subscriptions])

    def test_empty_tables_keep_schema(self):
        self.check_image([], [])

    def test_large_unsigned_values_and_unicode(self):
        self.check_image([{
            "id": "evsub_demo", "mode": "controlled", "active": True,
            "interest_count": 1, "backlog_frames": 1,
            "backlog_bytes": 2**64 - 1,
        }], [{"id": "evsub_demo", "session": "会話"}])


if __name__ == "__main__":
    unittest.main()
