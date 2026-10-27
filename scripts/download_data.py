"""Fetch the official April 2020 sample and extract only the three selected CSVs."""
import hashlib
import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

URL = "https://raw.githubusercontent.com/synthetichealth/synthea-sample-data/main/downloads/synthea_sample_data_csv_apr2020.zip"


def main():
    target = Path("data/source")
    target.mkdir(parents=True, exist_ok=True)
    provenance = target / "provenance.json"
    previous = json.loads(provenance.read_text()) if provenance.exists() else {}
    with urlopen(URL, timeout=60) as response:
        payload = response.read()
    digest = hashlib.sha256(payload).hexdigest()
    if previous.get("archive_sha256") and digest != previous["archive_sha256"]:
        raise ValueError("Upstream archive changed; review before replacing the recorded source.")
    archive = zipfile.ZipFile(io.BytesIO(payload))
    manifest = {"source_url": URL, "archive_sha256": digest,
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "description": "Official Synthea April 2020 synthetic CSV sample; selected files only", "files": {}}
    selected = {}
    for name in ("patients.csv", "encounters.csv", "payers.csv"):
        matches = [member for member in archive.namelist() if Path(member).name == name]
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one {name}")
        selected[name] = archive.read(matches[0])
    for name, body in selected.items():
        (target / name).write_bytes(body)
        manifest["files"][name] = {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
    provenance.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print("Selected source files saved to data/source")


if __name__ == "__main__":
    main()
