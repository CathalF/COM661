from __future__ import annotations

import functools
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Optional

import jwt
from flask import Blueprint, g, jsonify, request
from passlib.context import CryptContext

from .config import config
from .db import collection
from .utils import error_response

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

auth_bp = Blueprint("auth", __name__, url_prefix="/api/v1/auth")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def create_token(user: dict[str, Any]) -> str:
    payload = {
        "sub": str(user["_id"]),
        "role": user.get("role", "user"),
        "exp": datetime.now(timezone.utc) + config.JWT_EXPIRATION,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, config.JWT_SECRET, algorithm="HS256")


def decode_token(token: str) -> dict[str, Any]:
    return jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])


def require_auth(role: Optional[Literal["user", "admin"]] = None) -> Callable:
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            auth_header = request.headers.get("Authorization", "")
            if not auth_header.startswith("Bearer "):
                return error_response("UNAUTHENTICATED", "Authentication required", 401)
            token = auth_header.split(" ", 1)[1]
            try:
                payload = decode_token(token)
            except jwt.ExpiredSignatureError:
                return error_response("TOKEN_EXPIRED", "Token has expired", 401)
            except jwt.InvalidTokenError:
                return error_response("INVALID_TOKEN", "Invalid authentication token", 401)

            users_coll = collection("users")
            user = users_coll.find_one({"_id": payload["sub"]})
            if not user:
                return error_response("UNAUTHENTICATED", "User no longer exists", 401)
            if role == "admin" and user.get("role") != "admin":
                return error_response("UNAUTHORISED", "Admin privileges required", 403)
            g.current_user = user
            return func(*args, **kwargs)

        return wrapper

    return decorator


@auth_bp.post("/register")
def register():
    data = request.get_json(silent=True) or {}
    required = {"email", "password"}
    if not required.issubset(data):
        return error_response("VALIDATION_ERROR", "Email and password are required", 422)

    users_coll = collection("users")
    if users_coll.find_one({"email": data["email"].lower()}):
        return error_response("DUPLICATE", "Email already registered", 409)

    user_id = str(uuid.uuid4())
    doc = {
        "_id": user_id,
        "email": data["email"].lower(),
        "password_hash": hash_password(data["password"]),
        "role": "user",
        "created_at": datetime.now(timezone.utc),
    }
    users_coll.insert_one(doc)
    token = create_token(doc)
    return jsonify({"token": token, "user": {"_id": user_id, "email": doc["email"], "role": doc["role"]}}), 201


@auth_bp.post("/login")
def login():
    data = request.get_json(silent=True) or {}
    if "email" not in data or "password" not in data:
        return error_response("VALIDATION_ERROR", "Email and password are required", 422)

    users_coll = collection("users")
    user = users_coll.find_one({"email": data["email"].lower()})
    if not user or not verify_password(data["password"], user.get("password_hash", "")):
        return error_response("UNAUTHENTICATED", "Invalid email or password", 401)

    token = create_token(user)
    return jsonify({"token": token, "user": {"_id": str(user["_id"]), "email": user["email"], "role": user.get("role", "user")}})
