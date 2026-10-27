"""Publish a verified local batch to S3, with its completion manifest written last."""
import argparse
import base64
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from .export import verify_batch
from .storage import TABLES


def publish_batch(directory, bucket, prefix="healthcare", client=None, region=None):
    directory = Path(directory)
    manifest = verify_batch(directory)
    manifest_payload = (directory / "manifest.json").read_bytes()
    if json.loads(manifest_payload) != manifest:
        raise ValueError("Local manifest changed after batch validation")
    manifest_digest = hashlib.sha256(manifest_payload).hexdigest()
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Supply a bucket name, not an S3 URL")
    prefix = prefix.strip("/")
    if not prefix or any(part in ("", ".", "..") for part in prefix.split("/")):
        raise ValueError("Invalid project prefix")
    if client is None:
        import boto3
        from botocore.config import Config
        client = boto3.client("s3", region_name=region,
                              config=Config(retries={"mode": "standard", "max_attempts": 3},
                                            connect_timeout=10, read_timeout=60))
    batch_prefix = prefix + "/" + manifest["batch_id"]
    receipt = {"batch_id": manifest["batch_id"], "bucket": bucket,
               "prefix": batch_prefix, "objects": [], "status": "in_progress"}

    def put_verified(name, expected_digest):
        payload = (directory / name).read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if digest != expected_digest:
            raise ValueError("Local file changed after batch validation")
        key = batch_prefix + "/" + name
        state = "uploaded"
        try:
            client.put_object(Bucket=bucket, Key=key, Body=payload, IfNoneMatch="*",
                              ContentType="application/json" if name.endswith(".json") else "text/csv",
                              ServerSideEncryption="AES256",
                              ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode("ascii"),
                              Metadata={"sha256": digest, "batch-id": manifest["batch_id"]})
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code not in ("PreconditionFailed", "412"):
                raise
            state = "reused"
        head = client.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
        checksum = base64.b64encode(bytes.fromhex(digest)).decode("ascii")
        if (head["ContentLength"] != len(payload) or head.get("Metadata", {}).get("sha256") != digest
                or head.get("ChecksumSHA256") != checksum):
            raise ValueError("An S3 object conflicts with this batch; no overwrite attempted")
        receipt["objects"].append({"key": key, "bytes": len(payload), "sha256": digest, "action": state})

    for table in TABLES:
        metadata = manifest["tables"][table]
        put_verified(metadata["file"], metadata["sha256"])
    # Downstream readers consider a batch available only when this manifest exists.
    put_verified("manifest.json", manifest_digest)
    receipt.update(status="passed", completed_at=datetime.now(timezone.utc).isoformat(),
                   manifest_uri=f"s3://{bucket}/{batch_prefix}/manifest.json")
    receipt_path = directory / "s3_receipt.json"
    temporary = directory / ".s3_receipt.partial"
    temporary.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    temporary.replace(receipt_path)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="healthcare")
    parser.add_argument("--region", default=None)
    args = parser.parse_args()
    try:
        receipt = publish_batch(args.batch, args.bucket, args.prefix, region=args.region)
        print(json.dumps({"status": "passed", "manifest_uri": receipt["manifest_uri"],
                          "uploaded": sum(o["action"] == "uploaded" for o in receipt["objects"]),
                          "reused": sum(o["action"] == "reused" for o in receipt["objects"])}))
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__, "aws_error_code": code}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
