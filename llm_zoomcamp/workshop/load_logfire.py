"""Load Logfire trace/span records into DuckDB with dlt.

Credentials: put `LOGFIRE_READ_TOKEN` in `.env`, or set
`sources.load_logfire.records.read_token` in `.dlt/secrets.toml`.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import dlt
from dotenv import load_dotenv
from logfire.query_client import AsyncLogfireQueryClient

load_dotenv(Path(__file__).resolve().parent / ".env")


def _read_token() -> str:
    token = os.environ.get("LOGFIRE_READ_TOKEN", "").strip().strip("'\"")
    if token:
        return token
    try:
        token = dlt.secrets["sources.load_logfire.records.read_token"]
    except Exception:
        token = None
    if not token:
        raise RuntimeError(
            "Missing Logfire read token. Set LOGFIRE_READ_TOKEN in .env "
            "or sources.load_logfire.records.read_token in .dlt/secrets.toml."
        )
    return str(token)


@dlt.resource(name="records", write_disposition="replace")
async def records(batch_size: int = 1000):
    """Fetch Logfire `records` (spans/logs) and yield dicts for nested normalization."""
    async with AsyncLogfireQueryClient(read_token=_read_token()) as client:
        # `records` excludes pending spans; `records_all` includes agent/tool spans.
        result = await client.query_json_rows(
            sql="SELECT * FROM records_all",
            min_timestamp=datetime(1970, 1, 1, tzinfo=timezone.utc),
            limit=batch_size,
        )
        # Yield dict rows so dlt expands nested JSON into child tables.
        yield result["rows"]


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="logfire_agent",
        destination="duckdb",
        dataset_name="agent_traces",
    )
    load_info = pipeline.run(records())
    print(load_info)
    print(f"DuckDB file: {pipeline.pipeline_name}.duckdb")
    print(f"Dataset: {pipeline.dataset_name}")


if __name__ == "__main__":
    run()
