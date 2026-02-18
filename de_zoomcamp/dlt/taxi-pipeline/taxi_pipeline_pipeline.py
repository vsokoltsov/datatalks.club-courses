"""dlt pipeline to ingest NYC taxi data from the Data Engineering Zoomcamp REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_pipeline_rest_api_source():
    """Define dlt resources for NYC taxi data (paginated JSON via ?page=1, ?page=2, ...)."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
        },
        "resources": [
            {
                "name": "taxi_trips",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "params": {},
                    "data_selector": "[*]",
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


# Explicit path + read_only=False so pipeline and dashboard use the same connection
# config; avoids DuckDB "different configuration than existing connections" error.
pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination=dlt.destinations.duckdb("taxi_pipeline.duckdb", read_only=False),
    dataset_name="taxi_data",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    # One-shot run (extract → normalize → load)
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201

    # Debug: run in stages to see "Extract done" then load progress (swap with run() above)
    # pipeline.extract(taxi_pipeline_rest_api_source())
    # print("Extract done. Normalizing and loading...")
    # pipeline.normalize(workers=1); load_info = pipeline.load()
    # print(load_info)

    # After run: pipeline.last_trace has last_extract_info, last_normalize_info.row_counts, last_load_info
