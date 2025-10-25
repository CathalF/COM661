from __future__ import annotations

import uuid

from flask import Blueprint, jsonify, request
from marshmallow import ValidationError

from ..auth import require_auth
from ..db import collection
from ..utils import error_response, pagination_envelope, parse_pagination, parse_sort
from ..validators import SeasonSchema

seasons_bp = Blueprint("seasons", __name__, url_prefix="/api/v1/seasons")


@seasons_bp.get("")
def list_seasons():
    coll = collection("seasons")
    page, page_size = parse_pagination()
    filters: dict[str, object] = {}
    if competition_id := request.args.get("competition_id"):
        filters["competition_id"] = competition_id
    cursor = coll.find(filters)
    sort_spec = parse_sort("-year")
    cursor = cursor.sort(sort_spec)
    total = coll.count_documents(filters)
    items = cursor.skip((page - 1) * page_size).limit(page_size)
    data = [serialize_season(doc) for doc in items]
    return pagination_envelope(data, page, page_size, total)


@seasons_bp.get("/<season_id>")
def get_season(season_id: str):
    coll = collection("seasons")
    doc = coll.find_one({"_id": season_id})
    if not doc:
        return error_response("NOT_FOUND", "Season not found", 404)
    return jsonify(serialize_season(doc))


@seasons_bp.post("")
@require_auth("admin")
def create_season():
    data = request.get_json(silent=True) or {}
    schema = SeasonSchema()
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid season payload", 422, details)

    coll = collection("seasons")
    payload.setdefault("_id", str(uuid.uuid4()))
    if coll.find_one({"_id": payload["_id"]}):
        return error_response("DUPLICATE", "Season ID already exists", 409)
    coll.insert_one(payload)
    return jsonify(serialize_season(payload)), 201


@seasons_bp.put("/<season_id>")
@require_auth("admin")
def update_season(season_id: str):
    data = request.get_json(silent=True) or {}
    schema = SeasonSchema(partial=True)
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid season payload", 422, details)

    coll = collection("seasons")
    result = coll.update_one({"_id": season_id}, {"$set": payload})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Season not found", 404)
    doc = coll.find_one({"_id": season_id})
    return jsonify(serialize_season(doc))


@seasons_bp.delete("/<season_id>")
@require_auth("admin")
def delete_season(season_id: str):
    coll = collection("seasons")
    result = coll.delete_one({"_id": season_id})
    if not result.deleted_count:
        return error_response("NOT_FOUND", "Season not found", 404)
    return ("", 204)


def serialize_season(doc: dict) -> dict:
    return {
        "_id": doc.get("_id"),
        "competition_id": doc.get("competition_id"),
        "year": doc.get("year"),
        "start_date": doc.get("start_date"),
        "end_date": doc.get("end_date"),
    }
