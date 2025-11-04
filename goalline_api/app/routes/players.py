from __future__ import annotations

import uuid
from typing import Any

from flask import Blueprint, jsonify, request
from marshmallow import ValidationError

from ..auth import require_auth
from ..db import collection
from ..utils import error_response, pagination_envelope, parse_pagination, parse_sort
from ..validators import PlayerSchema

players_bp = Blueprint("players", __name__, url_prefix="/api/v1/players")


@players_bp.get("")
def list_players():
    players_coll = collection("players")
    page, page_size = parse_pagination()
    filters: dict[str, Any] = {}
    if q := request.args.get("q"):
        filters["name"] = {"$regex": q, "$options": "i"}
    if team_id := request.args.get("team_id"):
        filters["current_team_id"] = team_id
    if nationality := request.args.get("nationality"):
        filters["nationality"] = nationality
    if position := request.args.get("position"):
        filters["positions"] = position

    cursor = players_coll.find(filters)
    sort_spec = parse_sort("name")
    cursor = cursor.sort(sort_spec)
    total = players_coll.count_documents(filters)
    items = cursor.skip((page - 1) * page_size).limit(page_size)
    data = [serialize_player(doc) for doc in items]
    return pagination_envelope(data, page, page_size, total)


@players_bp.get("/<player_id>")
def get_player(player_id: str):
    players_coll = collection("players")
    doc = players_coll.find_one({"_id": player_id})
    if not doc:
        return error_response("NOT_FOUND", "Player not found", 404)
    return jsonify(serialize_player(doc))


@players_bp.post("")
@require_auth("admin")
def create_player():
    data = request.get_json(silent=True) or {}
    schema = PlayerSchema()
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid player payload", 422, details)

    players_coll = collection("players")
    payload.setdefault("_id", str(uuid.uuid4()))
    if players_coll.find_one({"_id": payload["_id"]}):
        return error_response("DUPLICATE", "Player ID already exists", 409)
    players_coll.insert_one(payload)
    return jsonify(serialize_player(payload)), 201


@players_bp.put("/<player_id>")
@require_auth("admin")
def update_player(player_id: str):
    data = request.get_json(silent=True) or {}
    schema = PlayerSchema(partial=True)
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid player payload", 422, details)

    players_coll = collection("players")
    result = players_coll.update_one({"_id": player_id}, {"$set": payload})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Player not found", 404)
    doc = players_coll.find_one({"_id": player_id})
    return jsonify(serialize_player(doc))


@players_bp.delete("/<player_id>")
@require_auth("admin")
def delete_player(player_id: str):
    players_coll = collection("players")
    result = players_coll.delete_one({"_id": player_id})
    if not result.deleted_count:
        return error_response("NOT_FOUND", "Player not found", 404)
    return ("", 204)


def serialize_player(doc: dict) -> dict:
    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "dob": doc.get("dob"),
        "nationality": doc.get("nationality"),
        "positions": doc.get("positions", []),
        "current_team_id": doc.get("current_team_id"),
    }
