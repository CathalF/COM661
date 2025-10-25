from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Optional

from flask import jsonify, request

from .config import config


def parse_pagination() -> tuple[int, int]:
    default = config.PAGINATION_DEFAULT
    page = max(int(request.args.get("page", 1)), 1)
    page_size = int(request.args.get("page_size", default))
    page_size = max(1, min(page_size, config.PAGINATION_MAX))
    return page, page_size


def pagination_envelope(data: Iterable[Any], page: int, page_size: int, total: int):
    total_pages = (total + page_size - 1) // page_size if page_size else 1
    return jsonify(
        {
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "total_items": total,
            "data": list(data),
        }
    )


def parse_sort(default: str = "_id") -> list[tuple[str, int]]:
    sort_param = request.args.get("sort", default)
    sort_fields: list[tuple[str, int]] = []
    for field in sort_param.split(","):
        direction = -1 if field.startswith("-") else 1
        field_name = field[1:] if field.startswith("-") else field
        sort_fields.append((field_name, direction))
    return sort_fields


def error_response(code: str, message: str, status: int, details: Optional[list[dict[str, Any]]] = None):
    payload: dict[str, Any] = {
        "error": {
            "code": code,
            "message": message,
        }
    }
    if details:
        payload["error"]["details"] = details
    return jsonify(payload), status


def iso_to_datetime(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
