"""One transaction for all entities; supports SQLite proof and PostgreSQL source."""
import sqlite3
from .quality import fingerprint

TABLES = ("patients", "payers", "encounters")
COLUMNS = {
    "patients": ("id", "birthdate", "deathdate", "gender", "city", "state"),
    "payers": ("id", "name", "ownership"),
    "encounters": ("id", "start", "stop", "patient", "payer", "encounterclass", "code",
                   "total_claim_cost_cents", "payer_coverage_cents"),
}


def connect(database, engine="sqlite"):
    if engine == "postgres":
        import psycopg
        return psycopg.connect(database)
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS source_patients (
        id TEXT PRIMARY KEY, birthdate TEXT NOT NULL, deathdate TEXT,
        gender TEXT NOT NULL, city TEXT, state TEXT, row_hash TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS source_payers (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, ownership TEXT, row_hash TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS source_encounters (
        id TEXT PRIMARY KEY, start TEXT NOT NULL, stop TEXT,
        patient TEXT NOT NULL REFERENCES source_patients(id),
        payer TEXT NOT NULL REFERENCES source_payers(id),
        encounterclass TEXT NOT NULL, code TEXT NOT NULL,
        total_claim_cost_cents BIGINT NOT NULL CHECK (total_claim_cost_cents >= 0),
        payer_coverage_cents BIGINT NOT NULL CHECK (payer_coverage_cents >= 0),
        row_hash TEXT NOT NULL)''')


def load_snapshot(connection, datasets, engine="sqlite"):
    placeholder = "%s" if engine == "postgres" else "?"
    metrics = {}
    # Psycopg's connection context closes the connection on exit; SQLite's
    # context only ends the transaction. Keep both open for reporting in run().
    transaction = connection.transaction() if engine == "postgres" else connection
    with transaction:
        cursor = connection.cursor()
        initialize(cursor)
        for table in TABLES:
            target = "source_" + table
            existing = dict(cursor.execute(f"SELECT id, row_hash FROM {target}").fetchall())
            columns = COLUMNS[table] + ("row_hash",)
            updates = ", ".join(f'"{key}" = excluded."{key}"' for key in columns[1:])
            names = ", ".join(f'"{key}"' for key in columns)
            values = ", ".join([placeholder] * len(columns))
            statement = (f"INSERT INTO {target} ({names}) VALUES ({values}) "
                         f"ON CONFLICT(id) DO UPDATE SET {updates} "
                         f"WHERE {target}.row_hash <> excluded.row_hash")
            counts = {"inserted": 0, "updated": 0, "unchanged": 0}
            for row in datasets[table]:
                digest = fingerprint(row)
                state = ("inserted" if row["id"] not in existing else
                         "unchanged" if existing[row["id"]] == digest else "updated")
                counts[state] += 1
                cursor.execute(statement, [row[key] for key in columns[:-1]] + [digest])
            # Reconcile the exact batch keys AND content, including unchanged records.
            actual = dict(cursor.execute(f"SELECT id, row_hash FROM {target}").fetchall())
            reconciled = sum(actual.get(row["id"]) == fingerprint(row) for row in datasets[table])
            if reconciled != len(datasets[table]):
                raise ValueError(f"Content reconciliation failed for {table}")
            counts.update(source_rows=len(datasets[table]), reconciled_rows=reconciled,
                          target_total_rows=len(actual))
            metrics[table] = counts
        cursor.close()
    return metrics


def monthly_report(connection):
    cursor = connection.cursor()
    cursor.execute('''SELECT substr(e.start, 1, 7) AS encounter_month,
        e.encounterclass, p.id AS payer_id, p.name AS payer_name,
        COUNT(*) AS encounter_count, COUNT(DISTINCT e.patient) AS unique_patients,
        SUM(e.total_claim_cost_cents) AS total_claim_cost_cents,
        SUM(e.payer_coverage_cents) AS payer_coverage_cents
        FROM source_encounters e JOIN source_payers p ON p.id = e.payer
        GROUP BY substr(e.start, 1, 7), e.encounterclass, p.id, p.name
        ORDER BY encounter_month, e.encounterclass, payer_id''')
    fields = [col[0] for col in cursor.description]
    rows = [dict(zip(fields, row)) for row in cursor.fetchall()]
    cursor.close()
    return fields, rows
