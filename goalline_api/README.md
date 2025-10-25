# GoalLine API

GoalLine API is a RESTful football data service exposing competitions, seasons, teams, players, matches, and user match notes. The project uses Flask with MongoDB and follows the coursework specification for COM661.

## Getting Started

1. Create and activate a Python 3.11 virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and adjust the values for your environment.
4. Run the development server:
   ```bash
   flask --app goalline_api.app:create_app run --debug
   ```

## Project Structure

```
goalline_api/
├── app
│   ├── __init__.py
│   ├── auth.py
│   ├── config.py
│   ├── db.py
│   ├── utils.py
│   ├── validators.py
│   └── routes/
│       ├── analytics.py
│       ├── competitions.py
│       ├── matches.py
│       ├── notes.py
│       ├── players.py
│       ├── seasons.py
│       └── teams.py
├── requirements.txt
├── .env.example
├── scripts/
│   ├── ensure_indexes.py
│   └── load_openfootball.py
└── tests/
    ├── newman_env.json
    └── postman_collection.json
```

## Dataset

The API is intended to work with data derived from the [OpenFootball](https://github.com/openfootball) datasets. Use `scripts/load_openfootball.py` to transform and import the raw datasets into MongoDB collections.

## Testing

A Postman collection and environment file are provided under `tests/` to cover standard and error scenarios. Use Newman to run the automated test suite:

```bash
newman run tests/postman_collection.json -e tests/newman_env.json
```

## License

This coursework project is intended for educational purposes as part of COM661.
