from __future__ import annotations

from typing import Any, Dict

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from .config import config

_client: MongoClient | None = None

def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(config.MONGO_URI)
    return _client


def get_db() -> Database:
    client = get_client()
    db_name = config.MONGO_URI.rsplit("/", 1)[-1]
    return client[db_name]


def collection(name: str) -> Collection:
    return get_db()[name]


def ensure_indexes() -> Dict[str, list[tuple[tuple[str, int], Dict[str, Any]]]]:
    """Create indexes for the collections and return index information."""
    db = get_db()
    indexes: Dict[str, list[tuple[tuple[str, int], Dict[str, Any]]]] = {
        "matches": [
            ((("season_id", 1), ("competition_id", 1), ("date", 1)), {"name": "season_competition_date"}),
            (("home_team_id", 1), {}),
            (("away_team_id", 1), {}),
        ],
        "teams": [
            (("name", "text"), {}),
            (("venue.location", "2dsphere"), {}),
        ],
        "players": [
            (("current_team_id", 1), {}),
            (("name", "text"), {}),
        ],
        "match_notes": [
            (("match_id", 1), {}),
            (("created_at", -1), {}),
        ],
        "users": [
            (("email", 1), {"unique": True}),
        ],
    }

    created: Dict[str, list[tuple[tuple[str, int], Dict[str, Any]]]] = {}
    for coll_name, index_list in indexes.items():
        coll = db[coll_name]
        created[coll_name] = []
        for keys, options in index_list:
            if keys and isinstance(keys[0], tuple):
                key_spec = list(keys)
            else:
                key_spec = [keys]
            if any(direction == "text" for _, direction in key_spec):
                key_spec = [(field, direction) for field, direction in key_spec]
            coll.create_index(key_spec, **options)
            created[coll_name].append((keys, options))
    return created
