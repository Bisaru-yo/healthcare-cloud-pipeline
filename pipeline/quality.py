"""Validate the entire selected source snapshot before opening a load transaction."""
import csv
import hashlib
import json
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import UUID


FIELDS = {
    "patients": ("id", "birthdate", "deathdate", "gender", "city", "state"),
    "payers": ("id", "name", "ownership"),
    "encounters": ("id", "start", "stop", "patient", "payer", "encounterclass", "code",
                   "total_claim_cost", "payer_coverage"),
}
REQUIRED = {
    "patients": ("id", "birthdate", "gender"),
    "payers": ("id", "name"),
    "encounters": ("id", "start", "patient", "payer", "encounterclass", "code",
                   "total_claim_cost", "payer_coverage"),
}
MONEY = ("total_claim_cost", "payer_coverage")
# The official April 2020 sample predates the optional payer Ownership column.
OPTIONAL_HEADERS = {"payers": {"ownership"}}


class QualityError(ValueError):
    def __init__(self, report):
        self.report = report
        super().__init__(f"Source rejected: {report['violation_count']} violations; see quality report")


def money_cents(value):
    amount = Decimal(value)
    if not amount.is_finite() or amount < 0 or amount != amount.quantize(Decimal("0.01")):
        raise ValueError("Expected a finite, nonnegative amount with at most two decimal places")
    cents = int(amount * 100)
    if cents > 9223372036854775807:
        raise ValueError("Amount exceeds signed BIGINT cents range")
    return cents


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timezone required")
    return parsed.astimezone(timezone.utc)


def fingerprint(row):
    return hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def read_and_validate(directory):
    directory = Path(directory)
    errors = []
    violations = Counter()
    datasets = {}
    checksums = {}
    counts = {}
    adaptations = []

    def fail(table, row_number, rule):
        violations[f"{table}.{rule}"] += 1
        # Bound report size, while retaining exact total counts per rule.
        if len(errors) < 100:
            errors.append({"table": table, "csv_row": row_number, "rule": rule})

    for table, columns in FIELDS.items():
        path = directory / f"{table}.csv"
        datasets[table] = []
        if not path.is_file():
            fail(table, 1, "missing_file")
            counts[table] = 0
            continue
        payload = path.read_bytes()
        checksums[path.name] = hashlib.sha256(payload).hexdigest()
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            headers = [h.strip().lower() for h in reader.fieldnames or []]
            if len(headers) != len(set(headers)):
                fail(table, 1, "duplicate_header")
            absent = set(columns) - set(headers)
            missing = absent - OPTIONAL_HEADERS.get(table, set())
            for column in sorted(absent - missing):
                adaptations.append({"table": table, "column": column, "action": "default_blank_legacy_header"})
            if missing:
                for column in sorted(missing):
                    fail(table, 1, f"missing_column:{column}")
                counts[table] = 0
                continue
            reader.fieldnames = headers
            seen = set()
            for number, raw in enumerate(reader, 2):
                if None in raw or any(value is None for value in raw.values()):
                    fail(table, number, "column_count_mismatch")
                row = {key: (raw.get(key) or "").strip() for key in columns}
                for key in REQUIRED[table]:
                    if not row[key]:
                        fail(table, number, f"required:{key}")
                for key in ("id", "patient", "payer"):
                    if key in row and row[key]:
                        try:
                            row[key] = str(UUID(row[key]))
                        except ValueError:
                            fail(table, number, f"uuid:{key}")
                if row["id"] in seen:
                    fail(table, number, "duplicate_id")
                seen.add(row["id"])
                if table == "patients":
                    parsed = {}
                    for key in ("birthdate", "deathdate"):
                        if row[key]:
                            try:
                                parsed[key] = date.fromisoformat(row[key])
                                row[key] = parsed[key].isoformat()
                            except ValueError:
                                fail(table, number, f"date:{key}")
                    if len(parsed) == 2 and parsed["deathdate"] < parsed["birthdate"]:
                        fail(table, number, "death_before_birth")
                if table == "encounters":
                    parsed = {}
                    for key in ("start", "stop"):
                        if row[key]:
                            try:
                                parsed[key] = timestamp(row[key])
                                row[key] = parsed[key].isoformat()
                            except ValueError:
                                fail(table, number, f"timestamp:{key}")
                    if len(parsed) == 2 and parsed["stop"] < parsed["start"]:
                        fail(table, number, "stop_before_start")
                    for key in MONEY:
                        try:
                            row[key + "_cents"] = money_cents(row.pop(key))
                        except (InvalidOperation, ValueError, OverflowError):
                            fail(table, number, f"money:{key}")
                datasets[table].append(row)
        counts[table] = len(datasets[table])
        if not datasets[table]:
            fail(table, 1, "empty_table")

    patient_ids = {row["id"] for row in datasets["patients"]}
    payer_ids = {row["id"] for row in datasets["payers"]}
    for number, row in enumerate(datasets["encounters"], 2):
        if row["patient"] not in patient_ids:
            fail("encounters", number, "orphan_patient")
        if row["payer"] not in payer_ids:
            fail("encounters", number, "orphan_payer")
    report = {
        "status": "failed" if violations else "passed",
        "source_counts": counts,
        "source_sha256": checksums,
        "schema_adaptations": adaptations,
        "violation_count": sum(violations.values()),
        "violations_by_rule": dict(violations),
        "first_100_violations": errors,
    }
    if violations:
        raise QualityError(report)
    return datasets, report
