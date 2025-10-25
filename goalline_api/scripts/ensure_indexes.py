"""CLI utility to create MongoDB indexes for the GoalLine API collections."""

from goalline_api.app.db import ensure_indexes


def main() -> None:
    created = ensure_indexes()
    for coll, indexes in created.items():
        for spec, options in indexes:
            print(f"Created index on {coll}: {spec} options={options}")


if __name__ == "__main__":
    main()
