from __future__ import annotations

import uuid

from flask import Blueprint, jsonify, request
from marshmallow import ValidationError

from ..auth import require_auth
from ..db import collection
from ..utils import error_response, pagination_envelope, parse_pagination, parse_sort
from ..validators import CompetitionSchema

competitions_bp = Blueprint("competitions", __name__, url_prefix="/api/v1/competitions")


@competitions_bp.get("")
def list_competitions():
    coll = collection("competitions")
    page, page_size = parse_pagination()
    filters: dict[str, object] = {}
    if q := request.args.get("q"):
        filters["$text"] = {"$search": q}
    if country := request.args.get("country"):
        filters["country"] = country

    cursor = coll.find(filters)
    sort_spec = parse_sort("name")
    cursor = cursor.sort(sort_spec)
    total = coll.count_documents(filters)
    items = cursor.skip((page - 1) * page_size).limit(page_size)
    data = [serialize_competition(doc) for doc in items]
    return pagination_envelope(data, page, page_size, total)


@competitions_bp.get("/<competition_id>")
def get_competition(competition_id: str):
    coll = collection("competitions")
    doc = coll.find_one({"_id": competition_id})
    if not doc:
        return error_response("NOT_FOUND", "Competition not found", 404)
    return jsonify(serialize_competition(doc))


@competitions_bp.get("/<competition_id>/seasons")
def competition_seasons(competition_id: str):
    seasons_coll = collection("seasons")
    page, page_size = parse_pagination()
    query = {"competition_id": competition_id}
    cursor = seasons_coll.find(query).sort([("year", -1)])
    total = seasons_coll.count_documents(query)
    items = cursor.skip((page - 1) * page_size).limit(page_size)
    return pagination_envelope([serialize_season(doc) for doc in items], page, page_size, total)


@competitions_bp.post("")
@require_auth("admin")
def create_competition():
    data = request.get_json(silent=True) or {}
    schema = CompetitionSchema()
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid competition payload", 422, details)

    coll = collection("competitions")
    payload.setdefault("_id", str(uuid.uuid4()))
    if coll.find_one({"_id": payload["_id"]}):
        return error_response("DUPLICATE", "Competition ID already exists", 409)
    coll.insert_one(payload)
    return jsonify(serialize_competition(payload)), 201


@competitions_bp.put("/<competition_id>")
@require_auth("admin")
def update_competition(competition_id: str):
    data = request.get_json(silent=True) or {}
    schema = CompetitionSchema(partial=True)
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid competition payload", 422, details)

    coll = collection("competitions")
    result = coll.update_one({"_id": competition_id}, {"$set": payload})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Competition not found", 404)
    doc = coll.find_one({"_id": competition_id})
    return jsonify(serialize_competition(doc))


@competitions_bp.delete("/<competition_id>")
@require_auth("admin")
def delete_competition(competition_id: str):
    coll = collection("competitions")
    result = coll.delete_one({"_id": competition_id})
    if not result.deleted_count:
        return error_response("NOT_FOUND", "Competition not found", 404)
    return ("", 204)


def serialize_competition(doc: dict) -> dict:
    return {
        "_id": doc.get("_id"),
        "code": doc.get("code"),
        "name": doc.get("name"),
        "country": doc.get("country"),
    }


def serialize_season(doc: dict) -> dict:
    return {
        "_id": doc.get("_id"),
        "competition_id": doc.get("competition_id"),
        "year": doc.get("year"),
        "start_date": doc.get("start_date"),
        "end_date": doc.get("end_date"),
    }
