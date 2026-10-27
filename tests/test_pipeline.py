import csv
import json
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from contextlib import closing
from pathlib import Path

from pipeline.quality import FIELDS, QualityError, read_and_validate
from pipeline.run import run
from pipeline.storage import connect, load_snapshot, monthly_report

PATIENT = "11111111-1111-4111-8111-111111111111"
PAYER = "22222222-2222-4222-8222-222222222222"
ENCOUNTER = "33333333-3333-4333-8333-333333333333"


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.data = {
            "patients": [{"id": PATIENT, "birthdate": "1980-01-01", "deathdate": "",
                          "gender": "M", "city": "Test City", "state": "Massachusetts"}],
            "payers": [{"id": PAYER, "name": "Synthetic Test Payer", "ownership": "Private"}],
            "encounters": [{"id": ENCOUNTER, "start": "2020-01-01T10:00:00Z", "stop": "",
                            "patient": PATIENT, "payer": PAYER, "encounterclass": "ambulatory",
                            "code": "185349003", "total_claim_cost": "100.25", "payer_coverage": "80.20"}],
        }
        self.write()

    def write(self):
        for table, rows in self.data.items():
            with (self.source / f"{table}.csv").open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS[table])
                writer.writeheader()
                writer.writerows(rows)

    def validate(self):
        self.write()
        return read_and_validate(self.source)

    def assert_rule(self, rule):
        with self.assertRaises(QualityError) as caught:
            self.validate()
        self.assertIn(rule, caught.exception.report["violations_by_rule"])

    def test_valid_source_accepts_open_encounter_and_blank_deathdate(self):
        data, report = self.validate()
        self.assertEqual(report["status"], "passed")
        self.assertEqual(data["encounters"][0]["total_claim_cost_cents"], 10025)

    def test_duplicate_source_key_is_rejected_instead_of_silently_deduplicated(self):
        self.data["encounters"].append(deepcopy(self.data["encounters"][0]))
        self.assert_rule("encounters.duplicate_id")

    def test_both_foreign_keys_checked(self):
        self.data["encounters"][0]["patient"] = PAYER
        self.data["encounters"][0]["payer"] = PATIENT
        with self.assertRaises(QualityError) as caught:
            self.validate()
        rules = caught.exception.report["violations_by_rule"]
        self.assertIn("encounters.orphan_patient", rules)
        self.assertIn("encounters.orphan_payer", rules)

    def test_negative_nonfinite_and_subcent_costs_rejected(self):
        for invalid in ("-1", "NaN", "Infinity", "0.001", "", "abc"):
            with self.subTest(value=invalid):
                self.data["encounters"][0]["total_claim_cost"] = invalid
                self.assert_rule("encounters.money:total_claim_cost")

    def test_bad_and_reversed_dates_rejected(self):
        self.data["encounters"][0]["stop"] = "2019-12-31T23:59:00Z"
        self.assert_rule("encounters.stop_before_start")
        self.data["encounters"][0]["stop"] = "invalid"
        self.assert_rule("encounters.timestamp:stop")

    def test_timezone_free_timestamp_rejected(self):
        self.data["encounters"][0]["start"] = "2020-01-01T10:00:00"
        self.assert_rule("encounters.timestamp:start")

    def test_death_before_birth_rejected(self):
        self.data["patients"][0]["deathdate"] = "1970-01-01"
        self.assert_rule("patients.death_before_birth")

    def test_required_value_rejected(self):
        self.data["payers"][0]["name"] = ""
        self.assert_rule("payers.required:name")

    def test_missing_required_header_rejected(self):
        path = self.source / "patients.csv"
        path.write_text("id,birthdate\n" + PATIENT + ",1980-01-01\n")
        with self.assertRaises(QualityError) as caught:
            read_and_validate(self.source)
        self.assertIn("patients.missing_column:gender", caught.exception.report["violations_by_rule"])

    def test_legacy_sample_without_payer_ownership_is_explicitly_supported(self):
        (self.source / "payers.csv").write_text("Id,NAME\n" + PAYER + ",Synthetic Test Payer\n")
        data, report = read_and_validate(self.source)
        self.assertEqual(data["payers"][0]["ownership"], "")
        self.assertEqual(report["schema_adaptations"][0]["column"], "ownership")

    def test_empty_and_missing_files_rejected(self):
        self.data["encounters"] = []
        self.assert_rule("encounters.empty_table")
        (self.source / "patients.csv").unlink()
        with self.assertRaises(QualityError) as caught:
            read_and_validate(self.source)
        self.assertIn("patients.missing_file", caught.exception.report["violations_by_rule"])

    def test_rerun_has_no_extra_rows_and_late_correction_updates_existing_id(self):
        db = connect(str(self.root / "proof.db"))
        self.addCleanup(db.close)
        data, _ = self.validate()
        first = load_snapshot(db, data)
        replay = load_snapshot(db, data)
        self.assertEqual(first["encounters"]["inserted"], 1)
        self.assertEqual(replay["encounters"]["inserted"], 0)
        self.assertEqual(replay["encounters"]["unchanged"], 1)
        self.data["encounters"][0]["total_claim_cost"] = "102.25"
        data, _ = self.validate()
        correction = load_snapshot(db, data)
        self.assertEqual(correction["encounters"]["updated"], 1)
        self.assertEqual(correction["encounters"]["target_total_rows"], 1)
        self.assertEqual(db.execute("SELECT total_claim_cost_cents FROM source_encounters").fetchone()[0], 10225)

    def test_database_failure_rolls_back_all_entities(self):
        db = connect(str(self.root / "proof.db"))
        self.addCleanup(db.close)
        data, _ = self.validate()
        load_snapshot(db, data)
        corrupted = deepcopy(data)
        corrupted["payers"][0]["name"] = "Should roll back"
        corrupted["encounters"][0]["payer"] = PATIENT
        with self.assertRaises(sqlite3.IntegrityError):
            load_snapshot(db, corrupted)
        self.assertEqual(db.execute("SELECT name FROM source_payers").fetchone()[0], "Synthetic Test Payer")

    def test_payers_with_identical_names_remain_separate_report_groups(self):
        second_payer = "44444444-4444-4444-8444-444444444444"
        self.data["payers"].append({"id": second_payer, "name": "Synthetic Test Payer", "ownership": "Private"})
        encounter = deepcopy(self.data["encounters"][0])
        encounter.update(id="55555555-5555-4555-8555-555555555555", payer=second_payer)
        self.data["encounters"].append(encounter)
        data, _ = self.validate()
        db = connect(str(self.root / "proof.db"))
        self.addCleanup(db.close)
        load_snapshot(db, data)
        _, rows = monthly_report(db)
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["payer_id"] for row in rows}, {PAYER, second_payer})
        self.assertEqual(sum(row["total_claim_cost_cents"] for row in rows), 20050)

    def test_invalid_batch_retains_failure_evidence_and_keeps_prior_target(self):
        database = str(self.root / "proof.db")
        evidence = str(self.root / "evidence")
        run(self.source, database, evidence)
        self.data["encounters"][0]["patient"] = PAYER
        self.write()
        with self.assertRaises(QualityError):
            run(self.source, database, evidence)
        reports = [json.loads(path.read_text()) for path in (self.root / "evidence").glob("*/run.json")]
        self.assertEqual(sorted(r["status"] for r in reports), ["failed", "passed"])
        with closing(sqlite3.connect(database)) as db:
            self.assertEqual(db.execute("SELECT patient FROM source_encounters").fetchone()[0], PATIENT)
        # SQLite's connection context manager only manages the transaction.
        # Check explicit closure even on systems that allow deleting open files.
        with self.assertRaises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")


if __name__ == "__main__":
    unittest.main()
