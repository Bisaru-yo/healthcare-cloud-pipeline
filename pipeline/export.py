"""Export one consistent source snapshot into a verified, immutable local batch."""
import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .quality import fingerprint
from .storage import COLUMNS, TABLES, connect

SCHEMA_VERSION = 1
AMOUNTS = ("total_claim_cost_cents", "payer_coverage_cents")


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_identity(tables):
    return fingerprint(tables)


def verify_batch(directory):
    """Validate bytes, CSV content, keys, relationships, amounts, and row hashes."""
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("status") != "complete":
        raise ValueError("Unsupported or incomplete batch")
    batch_id = manifest.get("batch_id", "")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", batch_id):
        raise ValueError("Invalid batch identifier")
    tables = manifest.get("tables", {})
    if set(tables) != set(TABLES):
        raise ValueError("Batch must contain exactly the three contracted tables")
    ids = {}
    relationships = []
    for table in TABLES:
        metadata = tables[table]
        columns = COLUMNS[table] + ("row_hash",)
        if metadata.get("file") != f"{table}.csv" or metadata.get("columns") != list(columns):
            raise ValueError(f"Unexpected file or columns for {table}")
        path = directory / metadata["file"]
        if path.stat().st_size != metadata["bytes"] or sha256_file(path) != metadata["sha256"]:
            raise ValueError(f"File integrity check failed for {table}")
        count = 0
        totals = {key: 0 for key in AMOUNTS} if table == "encounters" else {}
        ids[table] = set()
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != list(columns):
                raise ValueError(f"CSV header mismatch for {table}")
            for row in reader:
                if None in row or any(value is None for value in row.values()):
                    raise ValueError(f"Malformed CSV row in {table}")
                digest = row.pop("row_hash")
                if not row["id"] or row["id"] in ids[table]:
                    raise ValueError(f"Empty or duplicate key in {table}")
                ids[table].add(row["id"])
                for key in totals:
                    row[key] = int(row[key])
                    if row[key] < 0:
                        raise ValueError("Negative monetary amount")
                    totals[key] += row[key]
                if fingerprint(row) != digest:
                    raise ValueError(f"Row content hash mismatch in {table}")
                if table == "encounters":
                    relationships.append((row["patient"], row["payer"]))
                count += 1
        if count == 0 or count != metadata["row_count"] or totals != metadata["amount_totals_cents"]:
            raise ValueError(f"Counts or monetary reconciliation failed for {table}")
    for patient, payer in relationships:
        if patient not in ids["patients"] or payer not in ids["payers"]:
            raise ValueError("Orphan relationship in batch")
    if content_identity(tables) != manifest.get("content_sha256"):
        raise ValueError("Batch content identity mismatch")
    return manifest


def export_snapshot(database, engine="postgres", output="data/batches", batch_id=None):
    if engine not in ("postgres", "sqlite"):
        raise ValueError("Unsupported source engine")
    if engine == "sqlite" and not Path(database).is_file():
        raise FileNotFoundError("SQLite source database is missing; run the local pipeline first")
    batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:12]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", batch_id):
        raise ValueError("Invalid batch identifier")
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    destination = root / batch_id
    if destination.exists():
        raise FileExistsError("This local batch already exists; it will not be overwritten")
    working = root / (".partial_" + uuid4().hex)
    working.mkdir()
    connection = None
    try:
        connection = connect(database, engine)
        transaction = connection.transaction() if engine == "postgres" else connection
        manifest = {"schema_version": SCHEMA_VERSION, "batch_id": batch_id, "status": "complete",
                    "source_engine": engine, "extraction_mode": "full_snapshot",
                    "started_at": datetime.now(timezone.utc).isoformat(), "tables": {}}
        with transaction:
            cursor = connection.cursor()
            try:
                if engine == "postgres":
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                else:
                    cursor.execute("BEGIN")
                for table in TABLES:
                    columns = COLUMNS[table] + ("row_hash",)
                    names = ", ".join(f'"{column}"' for column in columns)
                    sums = ", ".join(f"COALESCE(SUM({key}), 0)" for key in AMOUNTS)
                    query = f"SELECT COUNT(*){', ' + sums if table == 'encounters' else ''} FROM source_{table}"
                    cursor.execute(query)
                    expected = cursor.fetchone()
                    path = working / f"{table}.csv"
                    count = 0
                    totals = {key: 0 for key in AMOUNTS} if table == "encounters" else {}
                    cursor.execute(f"SELECT {names} FROM source_{table} ORDER BY id")
                    with path.open("w", encoding="utf-8", newline="") as handle:
                        writer = csv.writer(handle, lineterminator="\n")
                        writer.writerow(columns)
                        while True:
                            rows = cursor.fetchmany(1000)
                            if not rows:
                                break
                            for values in rows:
                                row = dict(zip(COLUMNS[table], values[:-1]))
                                if fingerprint(row) != values[-1]:
                                    raise ValueError(f"Stored row hash differs from content in {table}")
                                writer.writerow(values)
                                for key in totals:
                                    totals[key] += row[key]
                                count += 1
                    if count == 0 or count != expected[0]:
                        raise ValueError(f"Source-to-export count mismatch or empty table: {table}")
                    if table == "encounters" and list(totals.values()) != [int(v) for v in expected[1:]]:
                        raise ValueError("Source-to-export monetary totals differ")
                    manifest["tables"][table] = {
                        "file": path.name, "columns": list(columns), "row_count": count,
                        "bytes": path.stat().st_size, "sha256": sha256_file(path),
                        "amount_totals_cents": totals,
                    }
            finally:
                cursor.close()
        manifest["content_sha256"] = content_identity(manifest["tables"])
        manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        (working / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        verify_batch(working)
        # Both paths share one filesystem. Only a fully verified batch is published.
        working.rename(destination)
        return destination, manifest
    except BaseException:
        shutil.rmtree(working, ignore_errors=True)
        raise
    finally:
        if connection is not None:
            connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("postgres", "sqlite"), default="postgres")
    parser.add_argument("--output", default="data/batches")
    parser.add_argument("--batch-id", default=None)
    args = parser.parse_args()
    database = os.environ.get("DATABASE_URL") if args.engine == "postgres" else "data/local.db"
    if not database:
        parser.error("Set DATABASE_URL in this terminal first")
    try:
        directory, manifest = export_snapshot(database, args.engine, args.output, args.batch_id)
        print(json.dumps({"status": "passed", "engine": args.engine, "batch_directory": str(directory),
                          "row_counts": {key: value["row_count"] for key, value in manifest["tables"].items()},
                          "content_sha256": manifest["content_sha256"]}))
    except Exception as exc:
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
