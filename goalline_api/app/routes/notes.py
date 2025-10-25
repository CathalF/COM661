from __future__ import annotations

import uuid
from datetime import datetime, timezone

from flask import Blueprint, jsonify, g, request
from marshmallow import ValidationError

from ..auth import require_auth
from ..db import collection
from ..utils import error_response, pagination_envelope, parse_pagination
from ..validators import MatchNoteSchema

notes_bp = Blueprint("notes", __name__, url_prefix="/api/v1")


@notes_bp.get("/matches/<match_id>/notes")
@require_auth()
def list_notes(match_id: str):
    notes_coll = collection("match_notes")
    page, page_size = parse_pagination()
    query = {"match_id": match_id}
    cursor = notes_coll.find(query).sort([("created_at", -1)])
    total = notes_coll.count_documents(query)
    items = cursor.skip((page - 1) * page_size).limit(page_size)
    data = [serialize_note(doc) for doc in items]
    return pagination_envelope(data, page, page_size, total)


@notes_bp.post("/matches/<match_id>/notes")
@require_auth()
def create_note(match_id: str):
    data = request.get_json(silent=True) or {}
    schema = MatchNoteSchema()
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid note payload", 422, details)

    notes_coll = collection("match_notes")
    note = {
        "_id": str(uuid.uuid4()),
        "match_id": match_id,
        "user_id": str(g.current_user["_id"]),
        "rating": payload["rating"],
        "comment": payload["comment"],
        "created_at": datetime.now(timezone.utc),
    }
    notes_coll.insert_one(note)
    return jsonify(serialize_note(note)), 201


@notes_bp.put("/matches/<match_id>/notes/<note_id>")
@require_auth()
def update_note(match_id: str, note_id: str):
    data = request.get_json(silent=True) or {}
    schema = MatchNoteSchema(partial=True)
    try:
        payload = schema.load(data)
    except ValidationError as exc:
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in exc.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid note payload", 422, details)

    notes_coll = collection("match_notes")
    query = {"_id": note_id, "match_id": match_id, "user_id": str(g.current_user["_id"])}
    result = notes_coll.update_one(query, {"$set": payload})
    if not result.matched_count:
        return error_response("NOT_FOUND", "Note not found", 404)
    doc = notes_coll.find_one({"_id": note_id})
    return jsonify(serialize_note(doc))


@notes_bp.delete("/matches/<match_id>/notes/<note_id>")
@require_auth()
def delete_note(match_id: str, note_id: str):
    notes_coll = collection("match_notes")
    query = {"_id": note_id, "match_id": match_id, "user_id": str(g.current_user["_id"])}
    result = notes_coll.delete_one(query)
    if not result.deleted_count:
        return error_response("NOT_FOUND", "Note not found", 404)
    return ("", 204)


def serialize_note(doc: dict) -> dict:
    return {
        "_id": doc.get("_id"),
        "match_id": doc.get("match_id"),
        "user_id": doc.get("user_id"),
        "rating": doc.get("rating"),
        "comment": doc.get("comment"),
        "created_at": doc.get("created_at"),
    }
