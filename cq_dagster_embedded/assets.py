import tempfile
from dagster import asset, MetadataValue, AssetExecutionContext, MaterializeResult, FilesystemIOManager # import the `dagster` library
from dagster import asset
from dagster_shell import execute_shell_command
from dagster_duckdb import DuckDBResource
from .constants import CQ_MIGRATE_ONLY, CQ_HN_SPEC


@asset
def ingest_hn(context: AssetExecutionContext) -> None:
  ret = None
  with tempfile.NamedTemporaryFile(mode='w+t') as temp_file:
    temp_file.write(CQ_HN_SPEC)
    temp_file.flush()
    if CQ_MIGRATE_ONLY:
        ret = execute_shell_command(f"cloudquery migrate --log-console {temp_file.name}", output_logging="STREAM", log=context.log)
    else:
        ret = execute_shell_command(f"cloudquery sync --log-console {temp_file.name}", output_logging="STREAM", log=context.log)
    if ret[1] != 0:
      raise Exception(f"cloudquery command failed with exit code {ret[1]}")


@asset(deps=[ingest_hn])
def top_stories(duckdb: DuckDBResource) -> MaterializeResult:
  with duckdb.get_connection() as conn:
    conn.execute("CREATE TABLE if not exists hn_stories AS SELECT * FROM hackernews_items")
