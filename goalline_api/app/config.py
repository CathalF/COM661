from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


@dataclass
class Config:
    """Application configuration derived from environment variables."""

    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017/goalline")
    JWT_SECRET: str = os.getenv("JWT_SECRET", "dev-secret")
    JWT_EXPIRATION: timedelta = timedelta(hours=int(os.getenv("JWT_EXP_HOURS", 12)))
    PAGINATION_DEFAULT: int = _int_env("PAGINATION_DEFAULT", 20)
    PAGINATION_MAX: int = _int_env("PAGINATION_MAX", 100)
    DEBUG: bool = os.getenv("FLASK_DEBUG", "0") == "1"


config = Config()
