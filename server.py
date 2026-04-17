import os
import json
import logging
from datetime import date, datetime
from decimal import Decimal
from contextlib import asynccontextmanager

import duckdb
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

log = logging.getLogger(__name__)

ALLOWED_TABLES: dict[str, str] = {
    "leads": "entrusted_dw.semantic.Leads",
    "opportunities": "entrusted_dw.semantic.opportunities",
    "services": "entrusted_dw.semantic.services",
}

MAX_ROWS = 1024

security = HTTPBearer()

_conn: duckdb.DuckDBPyConnection | None = None


def _json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    return str(obj)


def _init_conn() -> duckdb.DuckDBPyConnection:
    token = os.environ["MOTHERDUCK_ACCESS_TOKEN"]
    os.environ["motherduck_token"] = token

    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH 'md:entrusted_dw' AS entrusted_dw (READ_ONLY)")

    for alias, fqn in ALLOWED_TABLES.items():
        conn.execute(f"CREATE VIEW {alias} AS SELECT * FROM {fqn}")

    return conn


def _get_conn() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is not None:
        try:
            _conn.execute("SELECT 1")
            return _conn
        except Exception:
            _conn = None
    _conn = _init_conn()
    return _conn


def _validate_sql(sql: str) -> tuple[bool, str]:
    import sqlglot
    from sqlglot import exp

    try:
        statements = sqlglot.parse(sql, dialect="duckdb")
    except Exception as e:
        return False, f"SQL parse error: {e}"

    if not statements:
        return False, "Empty SQL"

    for stmt in statements:
        if stmt is None:
            return False, "Unparseable SQL statement"

        root = type(stmt)
        if root not in (exp.Select, exp.Union, exp.Intersect, exp.Except):
            return False, "Only SELECT queries are allowed"

        cte_aliases: set[str] = set()
        for cte in stmt.find_all(exp.CTE):
            if cte.alias:
                cte_aliases.add(cte.alias.lower())

        allowed_names = set(ALLOWED_TABLES.keys())
        allowed_fqns = {v.lower() for v in ALLOWED_TABLES.values()}

        for table in stmt.find_all(exp.Table):
            name = table.name.lower()

            if name in cte_aliases:
                continue

            parts = []
            if table.catalog:
                parts.append(table.catalog.lower())
            if table.db:
                parts.append(table.db.lower())
            parts.append(name)
            ref = ".".join(parts)

            is_allowed = (
                name in allowed_names
                or ref in allowed_fqns
                or (
                    len(parts) == 2
                    and any(fqn.endswith(ref) for fqn in allowed_fqns)
                )
            )
            if not is_allowed:
                return False, (
                    f"Access denied: table '{table.name}' is not permitted. "
                    f"Allowed tables: {', '.join(sorted(ALLOWED_TABLES.keys()))}"
                )

    return True, ""


def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    expected = os.environ.get("API_BEARER_TOKEN", "")
    if not expected:
        raise HTTPException(status_code=500, detail="API_BEARER_TOKEN not configured")
    if credentials.credentials != expected:
        raise HTTPException(status_code=401, detail="Invalid bearer token")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _get_conn()
    log.info("MotherDuck connection established")
    yield
    global _conn
    if _conn:
        _conn.close()
        _conn = None


app = FastAPI(
    title="Entrusted Data Warehouse API",
    description="Read-only REST API for the Entrusted data warehouse (leads, opportunities, services).",
    version="1.0.0",
    lifespan=lifespan,
)


class QueryRequest(BaseModel):
    sql: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/tables", dependencies=[Depends(verify_token)])
def list_tables():
    return [{"name": k, "fully_qualified": v} for k, v in ALLOWED_TABLES.items()]


@app.get("/tables/{table_name}", dependencies=[Depends(verify_token)])
def describe_table(table_name: str):
    key = table_name.lower()
    if key not in ALLOWED_TABLES:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown table '{table_name}'. Allowed: {list(ALLOWED_TABLES.keys())}",
        )

    conn = _get_conn()
    try:
        rows = conn.execute(f"DESCRIBE {ALLOWED_TABLES[key]}").fetchall()
        return [
            {"column_name": r[0], "column_type": r[1], "null": r[2], "key": r[3], "default": r[4], "extra": r[5]}
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query", dependencies=[Depends(verify_token)])
def run_query(body: QueryRequest):
    ok, err = _validate_sql(body.sql)
    if not ok:
        raise HTTPException(status_code=400, detail=err)

    conn = _get_conn()
    try:
        result = conn.execute(body.sql)
        columns = [d[0] for d in result.description]
        rows = result.fetchmany(MAX_ROWS)
        data = [dict(zip(columns, row)) for row in rows]
        return json.loads(json.dumps(
            {"columns": columns, "row_count": len(data), "data": data},
            default=_json_default,
        ))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
