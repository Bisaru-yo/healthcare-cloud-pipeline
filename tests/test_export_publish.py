import base64
import hashlib
import json
import sqlite3
import unittest
from contextlib import closing

import test_pipeline as fixtures
from pipeline.export import content_identity, export_snapshot, sha256_file, verify_batch
from pipeline.publish import publish_batch
from pipeline.quality import read_and_validate
from pipeline.storage import connect, load_snapshot


class FakeS3Error(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class FakeS3:
    """In-memory contract double. These tests make no AWS calls."""
    def __init__(self):
        self.objects = {}
        self.put_order = []
        self.fail_on = None

    def put_object(self, **kwargs):
        key = kwargs["Key"]
        if self.fail_on and key.endswith(self.fail_on):
            raise FakeS3Error("ServiceUnavailable")
        if key in self.objects:
            raise FakeS3Error("PreconditionFailed")
        assert kwargs["IfNoneMatch"] == "*"
        assert kwargs["ServerSideEncryption"] == "AES256"
        digest = hashlib.sha256(kwargs["Body"]).digest()
        assert kwargs["ChecksumSHA256"] == base64.b64encode(digest).decode("ascii")
        self.objects[key] = kwargs
        self.put_order.append(key)
        return {}

    def head_object(self, **kwargs):
        record = self.objects[kwargs["Key"]]
        assert kwargs["ChecksumMode"] == "ENABLED"
        return {"ContentLength": len(record["Body"]), "Metadata": record["Metadata"],
                "ChecksumSHA256": base64.b64encode(hashlib.sha256(record["Body"]).digest()).decode("ascii")}


class ExportPublishTests(unittest.TestCase):
    def setUp(self):
        fixture = fixtures.PipelineTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.root = fixture.root
        self.database = str(self.root / "source.db")
        datasets, _ = read_and_validate(fixture.source)
        with closing(connect(self.database)) as connection:
            load_snapshot(connection, datasets)
        self.output = self.root / "batches"

    def export(self, batch_id="test_batch"):
        return export_snapshot(self.database, "sqlite", self.output, batch_id)

    def test_export_reconciles_counts_amounts_and_row_hashes(self):
        directory, manifest = self.export()
        self.assertEqual(manifest, verify_batch(directory))
        self.assertEqual(manifest["tables"]["encounters"]["row_count"], 1)
        self.assertEqual(manifest["tables"]["encounters"]["amount_totals_cents"],
                         {"total_claim_cost_cents": 10025, "payer_coverage_cents": 8020})
        self.assertFalse(list(self.output.glob(".partial_*")))

    def test_same_snapshot_has_stable_payload_hashes_across_new_batches(self):
        _, first = self.export("first")
        _, second = self.export("second")
        self.assertEqual(first["content_sha256"], second["content_sha256"])
        self.assertEqual(first["tables"], second["tables"])
        self.assertNotEqual(first["batch_id"], second["batch_id"])

    def test_existing_batch_is_never_overwritten(self):
        directory, _ = self.export()
        before = (directory / "manifest.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.export()
        self.assertEqual((directory / "manifest.json").read_bytes(), before)

    def test_unsafe_batch_identifier_is_rejected(self):
        with self.assertRaises(ValueError):
            self.export("../outside")

    def test_source_tampering_prevents_publication_and_cleans_partial_files(self):
        with closing(sqlite3.connect(self.database)) as connection:
            with connection:
                connection.execute("UPDATE source_payers SET name='unexpected change'")
        with self.assertRaises(ValueError):
            self.export()
        self.assertFalse((self.output / "test_batch").exists())
        self.assertFalse(list(self.output.glob(".partial_*")))

    def test_local_file_corruption_is_rejected_before_upload(self):
        directory, _ = self.export()
        path = directory / "patients.csv"
        path.write_bytes(path.read_bytes() + b"corrupt")
        client = FakeS3()
        with self.assertRaises(ValueError):
            publish_batch(directory, "test-healthcare-bucket", client=client)
        self.assertEqual(client.objects, {})

    def test_rehashed_csv_still_must_match_original_row_content_hashes(self):
        directory, manifest = self.export()
        path = directory / "payers.csv"
        path.write_bytes(path.read_bytes().replace(b"Synthetic Test Payer", b"Corrupted Test Payer"))
        metadata = manifest["tables"]["payers"]
        metadata.update(sha256=sha256_file(path), bytes=path.stat().st_size)
        manifest["content_sha256"] = content_identity(manifest["tables"])
        (directory / "manifest.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "Row content hash"):
            verify_batch(directory)

    def test_completion_manifest_is_uploaded_last(self):
        directory, _ = self.export()
        client = FakeS3()
        receipt = publish_batch(directory, "test-healthcare-bucket", client=client)
        self.assertEqual(receipt["status"], "passed")
        self.assertEqual(len(client.objects), 4)
        self.assertTrue(client.put_order[-1].endswith("/manifest.json"))
        self.assertTrue((directory / "s3_receipt.json").is_file())

    def test_same_batch_retry_reuses_all_objects_without_overwrite(self):
        directory, _ = self.export()
        client = FakeS3()
        publish_batch(directory, "test-healthcare-bucket", client=client)
        receipt = publish_batch(directory, "test-healthcare-bucket", client=client)
        self.assertEqual(len(client.put_order), 4)
        self.assertEqual({obj["action"] for obj in receipt["objects"]}, {"reused"})

    def test_partial_upload_has_no_manifest_and_retry_finishes_it(self):
        directory, _ = self.export()
        client = FakeS3()
        client.fail_on = "/payers.csv"
        with self.assertRaises(FakeS3Error):
            publish_batch(directory, "test-healthcare-bucket", client=client)
        self.assertEqual(len(client.objects), 1)
        self.assertFalse(any(key.endswith("manifest.json") for key in client.objects))
        self.assertFalse((directory / "s3_receipt.json").exists())
        client.fail_on = None
        receipt = publish_batch(directory, "test-healthcare-bucket", client=client)
        self.assertEqual(sum(obj["action"] == "reused" for obj in receipt["objects"]), 1)
        self.assertEqual(len(client.objects), 4)

    def test_conflicting_remote_object_stops_completion(self):
        directory, _ = self.export()
        client = FakeS3()
        client.objects["healthcare/test_batch/patients.csv"] = {"Body": b"wrong", "Metadata": {"sha256": "wrong"}}
        with self.assertRaisesRegex(ValueError, "conflicts"):
            publish_batch(directory, "test-healthcare-bucket", client=client)
        self.assertFalse(any(key.endswith("manifest.json") for key in client.objects))

    def test_failed_manifest_upload_is_recoverable(self):
        directory, _ = self.export()
        client = FakeS3()
        client.fail_on = "/manifest.json"
        with self.assertRaises(FakeS3Error):
            publish_batch(directory, "test-healthcare-bucket", client=client)
        self.assertEqual(len(client.objects), 3)
        client.fail_on = None
        receipt = publish_batch(directory, "test-healthcare-bucket", client=client)
        self.assertEqual(sum(obj["action"] == "reused" for obj in receipt["objects"]), 3)


if __name__ == "__main__":
    unittest.main()
