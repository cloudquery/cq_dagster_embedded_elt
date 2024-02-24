import os
from dagster._utils import file_relative_path

DUCKDB_CONNECTION_STRING = os.getenv("DUCKDB_CONNECTION_STRING", file_relative_path(__file__, "../data/example.db"))

CQ_MIGRATE_ONLY = os.getenv("CQ_MIGRATE_ONLY", "false")

CQ_HN_SPEC = f"""
kind: source
spec:
  name: "hackernews"
  path: "cloudquery/hackernews"
  registry: "cloudquery"
  version: "v3.0.22"
  tables: ["*"]
  destinations:
    - "duckdb"
  spec:
    item_concurrency: 100
    start_time: "2024-02-02T00:00:00Z"
---
kind: destination
spec:
  name: duckdb
  path: cloudquery/duckdb
  registry: cloudquery
  version: "v5.1.0"
  spec:
    connection_string: {DUCKDB_CONNECTION_STRING}
"""

