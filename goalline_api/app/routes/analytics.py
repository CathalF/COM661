from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from flask import Blueprint, jsonify, request

from ..db import collection
from ..utils import error_response

analytics_bp = Blueprint("analytics", __name__, url_prefix="/api/v1")


@dataclass
class TeamStanding:
    played: int = 0
    wins: int = 0
    draws: int = 0
    losses: int = 0
    goals_for: int = 0
    goals_against: int = 0

    @property
    def goal_diff(self) -> int:
        return self.goals_for - self.goals_against

    @property
    def points(self) -> int:
        return self.wins * 3 + self.draws


@analytics_bp.get("/tables")
def league_table():
    competition = request.args.get("competition")
    season = request.args.get("season")
    if not competition or not season:
        return error_response("VALIDATION_ERROR", "competition and season are required", 422)

    matches_coll = collection("matches")
    query = {"competition_id": competition, "season_id": season}
    matches = list(matches_coll.find(query))

    standings: Dict[str, TeamStanding] = defaultdict(TeamStanding)

    for match in matches:
        home = match.get("home_team_id")
        away = match.get("away_team_id")
        home_goals, away_goals = parse_score(match.get("score", {}))
        if home is None or away is None:
            continue
        home_row = standings[home]
        away_row = standings[away]
        home_row.played += 1
        away_row.played += 1
        home_row.goals_for += home_goals
        home_row.goals_against += away_goals
        away_row.goals_for += away_goals
        away_row.goals_against += home_goals
        if home_goals > away_goals:
            home_row.wins += 1
            away_row.losses += 1
        elif home_goals < away_goals:
            away_row.wins += 1
            home_row.losses += 1
        else:
            home_row.draws += 1
            away_row.draws += 1

    table = [
        {
            "team_id": team_id,
            "played": row.played,
            "wins": row.wins,
            "draws": row.draws,
            "losses": row.losses,
            "goals_for": row.goals_for,
            "goals_against": row.goals_against,
            "goal_difference": row.goal_diff,
            "points": row.points,
        }
        for team_id, row in standings.items()
    ]
    table.sort(key=lambda r: (-r["points"], -r["goal_difference"], -r["goals_for"]))

    return jsonify({"competition_id": competition, "season_id": season, "table": table})


@analytics_bp.get("/leaders/scorers")
def top_scorers():
    competition = request.args.get("competition")
    season = request.args.get("season")
    limit = int(request.args.get("limit", 20))

    matches_coll = collection("matches")
    query = {}
    if competition:
        query["competition_id"] = competition
    if season:
        query["season_id"] = season

    matches = matches_coll.find(query, {"events": 1})
    goal_counts: Dict[str, int] = defaultdict(int)
    for match in matches:
        for event in match.get("events", []):
            if event.get("type") == "goal" and event.get("player_id"):
                goal_counts[event["player_id"]] += 1

    if not goal_counts:
        return jsonify({"leaders": []})

    players_coll = collection("players")
    player_docs = {
        doc["_id"]: doc for doc in players_coll.find({"_id": {"$in": list(goal_counts.keys())}}, {"name": 1})
    }
    leaders = [
        {
            "player_id": player_id,
            "name": player_docs.get(player_id, {}).get("name"),
            "goals": goals,
        }
        for player_id, goals in goal_counts.items()
    ]
    leaders.sort(key=lambda row: row["goals"], reverse=True)
    return jsonify({"leaders": leaders[:limit]})


@analytics_bp.get("/streaks")
def streaks():
    competition = request.args.get("competition")
    season = request.args.get("season")
    streak_type = request.args.get("type")
    if streak_type not in {"win", "loss", "clean_sheet"}:
        return error_response("VALIDATION_ERROR", "type must be win, loss, or clean_sheet", 422)

    matches_coll = collection("matches")
    query = {}
    if competition:
        query["competition_id"] = competition
    if season:
        query["season_id"] = season

    matches = list(matches_coll.find(query).sort([("date", 1)]))
    team_results: Dict[str, List[str]] = defaultdict(list)
    for match in matches:
        home = match.get("home_team_id")
        away = match.get("away_team_id")
        home_goals, away_goals = parse_score(match.get("score", {}))
        if home is None or away is None:
            continue
        if streak_type == "clean_sheet":
            team_results[home].append("C" if away_goals == 0 else "N")
            team_results[away].append("C" if home_goals == 0 else "N")
        else:
            team_results[home].append(result_symbol(home_goals, away_goals))
            team_results[away].append(result_symbol(away_goals, home_goals))

    streaks_data = []
    for team_id, results in team_results.items():
        target = "W" if streak_type == "win" else "L" if streak_type == "loss" else "C"
        longest = longest_streak(results, target)
        streaks_data.append({"team_id": team_id, "streak": longest})

    streaks_data.sort(key=lambda r: r["streak"], reverse=True)
    return jsonify({"type": streak_type, "streaks": streaks_data})


@analytics_bp.get("/h2h")
def head_to_head():
    team_a = request.args.get("team_a")
    team_b = request.args.get("team_b")
    limit = int(request.args.get("limit", 20))
    if not team_a or not team_b:
        return error_response("VALIDATION_ERROR", "team_a and team_b are required", 422)

    matches_coll = collection("matches")
    query = {
        "$or": [
            {"home_team_id": team_a, "away_team_id": team_b},
            {"home_team_id": team_b, "away_team_id": team_a},
        ]
    }
    matches = list(matches_coll.find(query).sort([("date", -1)]).limit(limit))
    data = [serialize_match(doc) for doc in matches]
    return jsonify({"team_a": team_a, "team_b": team_b, "matches": data})


def parse_score(score: Dict[str, Any]) -> Tuple[int, int]:
    fulltime = score.get("fulltime") if isinstance(score, dict) else score
    if isinstance(fulltime, dict):
        home = int(fulltime.get("home", 0) or 0)
        away = int(fulltime.get("away", 0) or 0)
        return home, away
    if isinstance(fulltime, str):
        try:
            home_str, away_str = fulltime.split("-")
            return int(home_str), int(away_str)
        except (ValueError, AttributeError):
            return 0, 0
    return 0, 0


def result_symbol(goals_for: int, goals_against: int) -> str:
    if goals_for > goals_against:
        return "W"
    if goals_for < goals_against:
        return "L"
    return "D"


def longest_streak(sequence: List[str], target: str) -> int:
    longest = current = 0
    for value in sequence:
        if value == target:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def serialize_match(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "_id": doc.get("_id"),
        "date": doc.get("date"),
        "competition_id": doc.get("competition_id"),
        "season_id": doc.get("season_id"),
        "home_team_id": doc.get("home_team_id"),
        "away_team_id": doc.get("away_team_id"),
        "score": doc.get("score"),
    }
