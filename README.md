# Entrusted Data Warehouse — REST API

Read-only REST API for the Entrusted data warehouse, backed by MotherDuck. All endpoints (except `/health`) require a Bearer token.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (no auth) |
| GET | `/tables` | List available tables |
| GET | `/tables/{name}` | Describe a table's columns |
| POST | `/query` | Run a read-only SQL query |

## Authentication

All data endpoints require a `Bearer` token in the `Authorization` header:

```
Authorization: Bearer <your-token>
```

## Query example

```bash
curl -X POST https://your-app.up.railway.app/query \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM leads LIMIT 10"}'
```

## Environment variables

| Variable | Description |
|----------|-------------|
| `MOTHERDUCK_ACCESS_TOKEN` | MotherDuck access token |
| `API_BEARER_TOKEN` | Bearer token clients must provide |
| `PORT` | Server port (default: 8080) |

## Local development

```bash
cp .env.local .env
# edit .env with your real tokens
pip install -r requirements.txt
python server.py
```
