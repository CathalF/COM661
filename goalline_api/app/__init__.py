from __future__ import annotations

from flask import Flask
from marshmallow import ValidationError

from .auth import auth_bp
from .config import config
from .routes.analytics import analytics_bp
from .routes.competitions import competitions_bp
from .routes.matches import matches_bp
from .routes.notes import notes_bp
from .routes.players import players_bp
from .routes.seasons import seasons_bp
from .routes.teams import teams_bp
from .utils import error_response


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_mapping(
        MONGO_URI=config.MONGO_URI,
        JWT_SECRET=config.JWT_SECRET,
        PAGINATION_DEFAULT=config.PAGINATION_DEFAULT,
        PAGINATION_MAX=config.PAGINATION_MAX,
    )

    app.register_blueprint(auth_bp)
    app.register_blueprint(competitions_bp)
    app.register_blueprint(seasons_bp)
    app.register_blueprint(teams_bp)
    app.register_blueprint(players_bp)
    app.register_blueprint(matches_bp)
    app.register_blueprint(notes_bp)
    app.register_blueprint(analytics_bp)

    @app.errorhandler(ValidationError)
    def handle_validation(error: ValidationError):  # type: ignore[override]
        details = [{"field": key, "issue": ", ".join(map(str, value))} for key, value in error.messages.items()]
        return error_response("VALIDATION_ERROR", "Invalid request payload", 422, details)

    @app.errorhandler(404)
    def handle_not_found(_: Exception):
        return error_response("NOT_FOUND", "Resource not found", 404)

    @app.errorhandler(500)
    def handle_server_error(_: Exception):
        return error_response("SERVER_ERROR", "An unexpected error occurred", 500)

    return app


def main():
    app = create_app()
    app.run(debug=config.DEBUG)


if __name__ == "__main__":
    main()
