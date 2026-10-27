"""Run with: python -m pipeline.run --help"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from .quality import QualityError, read_and_validate
from .storage import connect, load_snapshot, monthly_report


def run(source, database, evidence="evidence", engine="sqlite"):
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8]
    output = Path(evidence) / run_id
    output.mkdir(parents=True)
    report = {"run_id": run_id, "engine": engine, "stage": "source_validation_and_upsert",
              "started_at": datetime.now(timezone.utc).isoformat(), "status": "running"}
    connection = None
    try:
        datasets, quality = read_and_validate(source)
        report["quality"] = quality
        connection = connect(database, engine)
        report["load"] = load_snapshot(connection, datasets, engine)
        fields, rows = monthly_report(connection)
        with (output / "monthly_utilization.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        report["monthly_report_groups"] = len(rows)
        report["status"] = "passed"
    except QualityError as exc:
        report.update(status="failed", quality=exc.report, error_type="QualityError")
        raise
    except Exception as exc:
        # Never serialize connection strings or connector exception text into public evidence.
        report.update(status="failed", error_type=type(exc).__name__)
        raise
    finally:
        if connection is not None:
            connection.close()
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        (output / "run.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"status": report["status"], "evidence": str(output / "run.json")}))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="data/source")
    parser.add_argument("--engine", choices=("sqlite", "postgres"), default="sqlite")
    parser.add_argument("--database", default=None, help="SQLite path. PostgreSQL uses DATABASE_URL env var.")
    parser.add_argument("--evidence", default="evidence")
    args = parser.parse_args()
    database = args.database or (os.environ.get("DATABASE_URL") if args.engine == "postgres" else "data/local.db")
    if not database:
        parser.error("Set DATABASE_URL locally for PostgreSQL")
    try:
        run(args.source, database, args.evidence, args.engine)
    except QualityError:
        return 2
    except Exception as exc:
        print(f"Load failed ({type(exc).__name__}); inspect local service configuration.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
