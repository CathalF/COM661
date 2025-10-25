"""Utility script to transform OpenFootball datasets into MongoDB documents.

This script is intentionally lightweight; it demonstrates the expected loading
workflow without depending on the actual OpenFootball data files.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any, Iterable

from pymongo.errors import BulkWriteError

from goalline_api.app.db import collection

DATASETS = {
    "competitions": "data/competitions.json",
    "seasons": "data/seasons.json",
    "teams": "data/teams.json",
    "players": "data/players.json",
    "matches": "data/matches.json",
}


def load_json(path: pathlib.Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
        if isinstance(data, list):
            return data
        raise ValueError(f"Expected list in {path}")


def import_dataset(name: str, path: pathlib.Path) -> None:
    docs = load_json(path)
    coll = collection(name)
    if isinstance(docs, list):
        if docs:
            try:
                coll.insert_many(docs, ordered=False)
                print(f"Imported {len(docs)} documents into {name}")
            except BulkWriteError as exc:
                inserted = exc.details.get("nInserted", 0) if exc.details else 0
                print(f"Inserted {inserted} documents into {name}; duplicates skipped")
    else:
        raise ValueError(f"Invalid dataset for {name}")


def main() -> None:
    base_path = pathlib.Path.cwd()
    for name, relative in DATASETS.items():
        path = base_path / relative
        if not path.exists():
            print(f"Skipping {name}: {path} not found")
            continue
        import_dataset(name, path)


if __name__ == "__main__":
    main()
