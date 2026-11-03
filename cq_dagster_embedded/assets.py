import subprocess
import tempfile
from dagster import asset, MetadataValue, AssetExecutionContext, MaterializeResult, FilesystemIOManager # import the `dagster` library
from dagster import asset
from dagster_duckdb import DuckDBResource
from .constants import CQ_MIGRATE_ONLY, CQ_HN_SPEC


def run_cloudquery(context: AssetExecutionContext, *args: str) -> int:
  process = subprocess.Popen(["cloudquery", *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
  for line in process.stdout:
    context.log.info(line.rstrip())
  return process.wait()


@asset
def ingest_hn(context: AssetExecutionContext) -> None:
  with tempfile.NamedTemporaryFile(mode='w+t') as temp_file:
    temp_file.write(CQ_HN_SPEC)
    temp_file.flush()
    command = "migrate" if CQ_MIGRATE_ONLY else "sync"
    exit_code = run_cloudquery(context, command, "--log-console", temp_file.name)
    if exit_code != 0:
      raise Exception(f"cloudquery command failed with exit code {exit_code}")


@asset(deps=[ingest_hn])
def top_stories(duckdb: DuckDBResource) -> MaterializeResult:
  with duckdb.get_connection() as conn:
    conn.execute("CREATE TABLE if not exists hn_stories AS SELECT * FROM hackernews_items")
