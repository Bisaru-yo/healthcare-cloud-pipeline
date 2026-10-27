"""Run the real selected Synthea sample through clean/replay/fail/recover/correct scenarios."""
import csv
import argparse
import json
import shutil
import sqlite3
import sys
import tempfile
from contextlib import closing
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.quality import QualityError
from pipeline.run import run


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=None, help="New evidence directory; existing scenarios will not be overwritten")
    args = parser.parse_args()
    evidence = (Path(args.output).resolve() if args.output else root / "evidence" /
                ("proof_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8]))
    evidence.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        source = work / "source"
        shutil.copytree(root / "data/source", source)
        database = str(work / "proof.db")
        scenarios = []

        def scenario(name, expected_failure=False):
            location = evidence / name
            if location.exists():
                raise FileExistsError(f"Evidence exists: {location}; preserve it or choose a new evidence directory")
            try:
                result = run(source, database, str(location))
            except QualityError:
                if not expected_failure:
                    raise
                reports = list(location.glob("*/run.json"))
                result = json.loads(reports[0].read_text())
            if expected_failure and result["status"] != "failed":
                raise AssertionError("The deliberately invalid batch was accepted")
            scenarios.append({"scenario": name, "status": result["status"],
                              "source_counts": result.get("quality", {}).get("source_counts"),
                              "load": result.get("load"),
                              "violations": result.get("quality", {}).get("violations_by_rule", {})})
            return result

        first = scenario("01_initial_load")
        second = scenario("02_identical_rerun")
        for table in first["load"]:
            assert first["load"][table]["inserted"] == first["load"][table]["source_rows"]
            assert second["load"][table]["inserted"] == 0
            assert second["load"][table]["updated"] == 0
            assert second["load"][table]["unchanged"] == first["load"][table]["source_rows"]

        path = source / "encounters.csv"
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            rows = list(reader)
        patient_key = next(key for key in fields if key.lower() == "patient")
        cost_key = next(key for key in fields if key.lower() == "total_claim_cost")
        id_key = next(key for key in fields if key.lower() == "id")
        original_patient = rows[0][patient_key]
        original_cost = rows[0][cost_key]

        def rewrite():
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)

        rows[0][patient_key] = str(uuid4())
        rewrite()
        failed = scenario("03_orphan_patient_rejected", expected_failure=True)
        assert failed["quality"]["violations_by_rule"]["encounters.orphan_patient"] == 1
        with closing(sqlite3.connect(database)) as db:
            actual_patient = db.execute("SELECT patient FROM source_encounters WHERE id=?", (rows[0][id_key],)).fetchone()[0]
        assert actual_patient == original_patient

        rows[0][patient_key] = original_patient
        rewrite()
        recovered = scenario("04_corrected_source_recovery")
        assert recovered["load"]["encounters"]["updated"] == 0

        rows[0][cost_key] = str(Decimal(original_cost) + Decimal("1.00"))
        rewrite()
        corrected = scenario("05_late_cost_correction")
        assert corrected["load"]["encounters"]["updated"] == 1
        assert corrected["load"]["encounters"]["inserted"] == 0
        assert corrected["load"]["encounters"]["target_total_rows"] == first["load"]["encounters"]["target_total_rows"]
        with closing(sqlite3.connect(database)) as db:
            actual_cost = db.execute("SELECT total_claim_cost_cents FROM source_encounters WHERE id=?", (rows[0][id_key],)).fetchone()[0]
        assert actual_cost == int(Decimal(rows[0][cost_key]) * 100)
        summary = {"scope": "Local SQLite source proof only; PostgreSQL/AWS/Snowflake/dbt/Airflow not executed",
                   "all_expected_behaviors_passed": True, "scenarios": scenarios}
        (evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
