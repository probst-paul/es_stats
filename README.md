## Quickstart (Postgres)

Requires Python 3.13+.

Create a venv and install:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

Set your database URL:

```bash
export ES_STATS_DATABASE_URL="postgresql://USER:PASSWORD@HOST:5432/DBNAME"
```

Replace placeholders with real values before running DB-backed tests.

Initialize schema:

```bash
python -m es_stats.cli.main init-db
```

Run the web app:

```bash
uvicorn es_stats.web.main:app --host 0.0.0.0 --port 8000
```

### Homebrew Postgres one-command test run

If you use local Homebrew Postgres, run everything (start DB, ensure DB exists, init schema, run tests) with:

```bash
make dev-test
```

Defaults used by `make dev-test`:
- formula: `postgresql@16`
- URL: `postgresql://$USER@localhost:5432/es_stats`

Override if needed:

```bash
DB_NAME=my_db DB_USER=my_user make dev-test
```

Other useful targets:

```bash
make db-up    # start Postgres service and create DB if missing
make db-init  # initialize schema in ES_STATS_DATABASE_URL
make test     # run pytest with ES_STATS_DATABASE_URL
```

## Render

Use a Web Service with this start command:

```bash
uvicorn es_stats.web.main:app --host 0.0.0.0 --port $PORT
```

Render supplies `DATABASE_URL` automatically when you attach a Postgres instance.
The app also supports `ES_STATS_DATABASE_URL` if you want to override it.
