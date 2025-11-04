from __future__ import annotations

import uuid
from typing import Any

from flask import Blueprint, jsonify, request
from marshmallow import ValidationError

from ..auth import require_auth
from ..db import collection
from ..utils import error_response, pagination_envelope, parse_pagination, parse_sort
from ..validators import TeamSchema

teams_bp = Blueprint("teams", __name__, url_prefix="/api/v1")


@teams_bp.get("/teams")
def list_teams():
    teams_coll = collection("teams")
    page, page_size = parse_pagination()
    filters: dict[str, Any] = {}
    if q := request.args.get("q"):
        filters["name"] = {"$regex": q, "$options": "i"}
    if country := request.args.get("country"):
        filters["country"] = country

    if competition := request.args.get("competition"):
        season_filter: dict[str, Any] = {"competition_id": competition}
        if season := request.args.get("season"):
            season_filter["season_id"] = season
        matches_coll = collection("matches")
        team_ids = matches_coll.distinct("home_team_id", season_filter)
        team_ids += matches_coll.distinct("away_team_id", season_filter)
        filters["_id"] = {"$in": list(set(team_ids))}

    cursor = teams_coll.find(filters)
    sort_spec = parse_sort("name")
    cursor = cursor.sort(sort_spec)
    total = teams_coll.count_documents(filters)
    items = cursor.skip((page - 1) * page_size).limit(page_size)
    data = [serialize_team(doc) for doc in items]
    return pagination_envelope(data, page, page_size, total)


@teams_bp.get("/teams/<team_id>")
def get_team(team_id: str):
    teams_coll = collection("teams")
    doc = teams_coll.find_one({"_id": team_id})
    if not doc:
        return error_response("NOT_FOUND", "Team not found", 404)
    return jsonify(serialize_team(doc))


@teams_bp.get("/venues/near")
def venues_near():
    lon = request.args.get("lon")
    lat = request.args.get("lat")
    if lon is None or lat is None:
        return error_response("VALIDATION_ERROR", "lon and lat query parameters are required", 422)
    try:
        coordinates = [float(lon), float(lat)]
        max_km = float(request.args.get("max_km", 50))
    except ValueError:
        return error_response("VALIDATION_ERROR", "lon and lat must be floats", 422)

    teams_coll = collection("teams")
    query = {
        "venue.location": {
            "$near": {
                "$geometry": {"type": "Point", "coordinates": coordinates},
                "$maxDistance": max_km * 1000,
            }
        }
    }
    venues = []
    for doc in teams_coll.find(query).limit(50):
        venues.append(
            {
                "team_id": doc.get("_id"),
                "team_name": doc.get("name"),
                "venue": doc.get("venue"),
            }
        )
    return jsonify({"data": venues})


@teams_bp.post("/teams")
@require_auth("admin")
def create_team():
    data = request.get_json(silent=True) or {}
    schema = TeamSchema()
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid team payload", 422, details)

    teams_coll = collection("teams")
    payload.setdefault("_id", str(uuid.uuid4()))
    if teams_coll.find_one({"_id": payload["_id"]}):
        return error_response("DUPLICATE", "Team ID already exists", 409)
    teams_coll.insert_one(payload)
    return jsonify(serialize_team(payload)), 201


@teams_bp.put("/teams/<team_id>")
@require_auth("admin")
def update_team(team_id: str):
    data = request.get_json(silent=True) or {}
    schema = TeamSchema(partial=True)
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid team payload", 422, details)

    teams_coll = collection("teams")
    result = teams_coll.update_one({"_id": team_id}, {"$set": payload})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Team not found", 404)
    doc = teams_coll.find_one({"_id": team_id})
    return jsonify(serialize_team(doc))


@teams_bp.delete("/teams/<team_id>")
@require_auth("admin")
def delete_team(team_id: str):
    teams_coll = collection("teams")
    result = teams_coll.delete_one({"_id": team_id})
    if not result.deleted_count:
        return error_response("NOT_FOUND", "Team not found", 404)
    return ("", 204)


def serialize_team(doc: dict) -> dict:
    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "short_name": doc.get("short_name"),
        "country": doc.get("country"),
        "venue": doc.get("venue"),
        "founded": doc.get("founded"),
    }
