from __future__ import annotations

import uuid
from typing import Any

from flask import Blueprint, jsonify, request
from marshmallow import ValidationError

from ..auth import require_auth
from ..db import collection
from ..utils import error_response, iso_to_datetime, pagination_envelope, parse_pagination, parse_sort
from ..validators import EventSchema, LineupPlayerSchema, MatchSchema

matches_bp = Blueprint("matches", __name__, url_prefix="/api/v1/matches")


@matches_bp.get("")
def list_matches():
    matches_coll = collection("matches")
    page, page_size = parse_pagination()
    filters: dict[str, Any] = {}
    if competition := request.args.get("competition"):
        filters["competition_id"] = competition
    if season := request.args.get("season"):
        filters["season_id"] = season
    if team_id := request.args.get("team_id"):
        filters["$or"] = [{"home_team_id": team_id}, {"away_team_id": team_id}]
    if date_from := iso_to_datetime(request.args.get("date_from")):
        filters.setdefault("date", {})["$gte"] = date_from
    if date_to := iso_to_datetime(request.args.get("date_to")):
        filters.setdefault("date", {})["$lte"] = date_to

    cursor = matches_coll.find(filters)
    sort_spec = parse_sort("-date")
    cursor = cursor.sort(sort_spec)
    total = matches_coll.count_documents(filters)
    items = cursor.skip((page - 1) * page_size).limit(page_size)
    data = [serialize_match(doc, include_nested=False) for doc in items]
    return pagination_envelope(data, page, page_size, total)


@matches_bp.get("/<match_id>")
def get_match(match_id: str):
    matches_coll = collection("matches")
    doc = matches_coll.find_one({"_id": match_id})
    if not doc:
        return error_response("NOT_FOUND", "Match not found", 404)
    include_notes = request.args.get("include_notes", "true").lower() == "true"
    match = serialize_match(doc, include_nested=True)
    if include_notes:
        notes_coll = collection("match_notes")
        notes = list(notes_coll.find({"match_id": match_id}).sort([("created_at", -1)]))
        match["notes"] = [serialize_note(note) for note in notes]
    return jsonify(match)


@matches_bp.post("")
@require_auth("admin")
def create_match():
    data = request.get_json(silent=True) or {}
    schema = MatchSchema()
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid match payload", 422, details)

    matches_coll = collection("matches")
    payload.setdefault("_id", str(uuid.uuid4()))
    if matches_coll.find_one({"_id": payload["_id"]}):
        return error_response("DUPLICATE", "Match ID already exists", 409)
    payload.setdefault("events", [])
    payload.setdefault("lineups", {})
    payload.setdefault("stats", {})
    matches_coll.insert_one(payload)
    return jsonify(serialize_match(payload, include_nested=True)), 201


@matches_bp.put("/<match_id>")
@require_auth("admin")
def update_match(match_id: str):
    data = request.get_json(silent=True) or {}
    schema = MatchSchema(partial=True)
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid match payload", 422, details)

    matches_coll = collection("matches")
    result = matches_coll.update_one({"_id": match_id}, {"$set": payload})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Match not found", 404)
    doc = matches_coll.find_one({"_id": match_id})
    return jsonify(serialize_match(doc, include_nested=True))


@matches_bp.delete("/<match_id>")
@require_auth("admin")
def delete_match(match_id: str):
    matches_coll = collection("matches")
    result = matches_coll.delete_one({"_id": match_id})
    if not result.deleted_count:
        return error_response("NOT_FOUND", "Match not found", 404)
    collection("match_notes").delete_many({"match_id": match_id})
    return ("", 204)


@matches_bp.post("/<match_id>/events")
@require_auth("admin")
def add_event(match_id: str):
    data = request.get_json(silent=True) or {}
    schema = EventSchema()
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid event payload", 422, details)

    matches_coll = collection("matches")
    payload["_id"] = str(uuid.uuid4())
    result = matches_coll.update_one({"_id": match_id}, {"$push": {"events": payload}})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Match not found", 404)
    doc = matches_coll.find_one({"_id": match_id})
    return jsonify(serialize_match(doc, include_nested=True))


@matches_bp.put("/<match_id>/events/<event_id>")
@require_auth("admin")
def update_event(match_id: str, event_id: str):
    data = request.get_json(silent=True) or {}
    schema = EventSchema(partial=True)
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid event payload", 422, details)

    matches_coll = collection("matches")
    update_fields = {f"events.$.{k}": v for k, v in payload.items()}
    result = matches_coll.update_one({"_id": match_id, "events._id": event_id}, {"$set": update_fields})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Match or event not found", 404)
    doc = matches_coll.find_one({"_id": match_id})
    return jsonify(serialize_match(doc, include_nested=True))


@matches_bp.delete("/<match_id>/events/<event_id>")
@require_auth("admin")
def delete_event(match_id: str, event_id: str):
    matches_coll = collection("matches")
    result = matches_coll.update_one({"_id": match_id}, {"$pull": {"events": {"_id": event_id}}})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Match not found", 404)
    doc = matches_coll.find_one({"_id": match_id})
    return jsonify(serialize_match(doc, include_nested=True))


@matches_bp.post("/<match_id>/lineups")
@require_auth("admin")
def create_lineups(match_id: str):
    data = request.get_json(silent=True) or {}
    schema = LineupPlayerSchema()
    try:
        home = [schema.load(player) for player in data.get("home", [])]
        away = [schema.load(player) for player in data.get("away", [])]
    except ValidationError as exc:
        details = [{"field": "lineups", "issue": str(exc)}]
        return error_response("VALIDATION_ERROR", "Invalid lineup payload", 422, details)

    matches_coll = collection("matches")
    result = matches_coll.update_one({"_id": match_id}, {"$set": {"lineups.home": home, "lineups.away": away}})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Match not found", 404)
    doc = matches_coll.find_one({"_id": match_id})
    return jsonify(serialize_match(doc, include_nested=True))


@matches_bp.put("/<match_id>/lineups")
@require_auth("admin")
def update_lineups(match_id: str):
    return create_lineups(match_id)


@matches_bp.delete("/<match_id>/lineups")
@require_auth("admin")
def delete_lineups(match_id: str):
    matches_coll = collection("matches")
    result = matches_coll.update_one({"_id": match_id}, {"$unset": {"lineups": ""}})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Match not found", 404)
    doc = matches_coll.find_one({"_id": match_id})
    return jsonify(serialize_match(doc, include_nested=True))


@matches_bp.put("/<match_id>/stats")
@require_auth("admin")
def update_stats(match_id: str):
    data = request.get_json(silent=True) or {}
    matches_coll = collection("matches")
    result = matches_coll.update_one({"_id": match_id}, {"$set": {"stats": data}})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Match not found", 404)
    doc = matches_coll.find_one({"_id": match_id})
    return jsonify(serialize_match(doc, include_nested=True))


def serialize_match(doc: dict, include_nested: bool = False) -> dict:
    base = {
        "_id": doc.get("_id"),
        "competition_id": doc.get("competition_id"),
        "season_id": doc.get("season_id"),
        "date": doc.get("date"),
        "venue": doc.get("venue"),
        "home_team_id": doc.get("home_team_id"),
        "away_team_id": doc.get("away_team_id"),
        "score": doc.get("score"),
    }
    if include_nested:
        base["lineups"] = doc.get("lineups", {})
        base["events"] = doc.get("events", [])
        base["stats"] = doc.get("stats", {})
    return base


def serialize_note(doc: dict) -> dict:
    return {
        "_id": doc.get("_id"),
        "match_id": doc.get("match_id"),
        "user_id": doc.get("user_id"),
        "rating": doc.get("rating"),
        "comment": doc.get("comment"),
        "created_at": doc.get("created_at"),
    }
