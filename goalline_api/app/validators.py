from __future__ import annotations

from typing import Any, Dict

from marshmallow import Schema, ValidationError, fields, validate


class CompetitionSchema(Schema):
    _id = fields.String(required=False)
    code = fields.String(required=True)
    name = fields.String(required=True)
    country = fields.String(required=True)


class SeasonSchema(Schema):
    _id = fields.String(required=False)
    competition_id = fields.String(required=True)
    year = fields.Integer(required=True)
    start_date = fields.Date(required=True)
    end_date = fields.Date(required=True)


class VenueSchema(Schema):
    name = fields.String(required=True)
    capacity = fields.Integer(required=False)
    location = fields.Dict(keys=fields.String(), values=fields.Raw())


class TeamSchema(Schema):
    _id = fields.String(required=False)
    name = fields.String(required=True)
    short_name = fields.String(required=False)
    country = fields.String(required=True)
    venue = fields.Nested(VenueSchema, required=False)
    founded = fields.Integer(required=False)


class PlayerSchema(Schema):
    _id = fields.String(required=False)
    name = fields.String(required=True)
    dob = fields.Date(required=True)
    nationality = fields.String(required=True)
    positions = fields.List(fields.String(), required=True)
    current_team_id = fields.String(required=False)


class EventSchema(Schema):
    minute = fields.Integer(required=True, validate=validate.Range(min=0))
    type = fields.String(required=True)
    team_id = fields.String(required=True)
    player_id = fields.String(required=True)
    assist_id = fields.String(required=False, allow_none=True)


class LineupPlayerSchema(Schema):
    player_id = fields.String(required=True)
    position = fields.String(required=True)


class MatchSchema(Schema):
    _id = fields.String(required=False)
    competition_id = fields.String(required=True)
    season_id = fields.String(required=True)
    date = fields.Date(required=True)
    venue = fields.Nested(VenueSchema, required=False)
    home_team_id = fields.String(required=True)
    away_team_id = fields.String(required=True)
    score = fields.Dict(required=True)
    lineups = fields.Dict(
        keys=fields.String(),
        values=fields.List(fields.Nested(LineupPlayerSchema)),
        required=False,
    )
    events = fields.List(fields.Nested(EventSchema), required=False)
    stats = fields.Dict(required=False)


class MatchNoteSchema(Schema):
    rating = fields.Integer(required=True, validate=validate.Range(min=1, max=5))
    comment = fields.String(required=True, validate=validate.Length(min=1, max=500))


class UserSchema(Schema):
    _id = fields.String(required=True)
    email = fields.Email(required=True)
    password_hash = fields.String(required=True)
    role = fields.String(required=True, validate=validate.OneOf(["user", "admin"]))


def validate_payload(schema: Schema, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return schema.load(payload)
    except ValidationError as exc:
        raise
