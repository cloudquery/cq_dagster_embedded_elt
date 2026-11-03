import pytest
from dagster import build_asset_context

import cq_dagster_embedded
from cq_dagster_embedded.assets import ingest_hn


def fake_cloudquery(tmp_path, monkeypatch, exit_code):
  script = tmp_path / "cloudquery"
  script.write_text(f"#!/bin/sh\necho \"cloudquery $@\"\nexit {exit_code}\n")
  script.chmod(0o755)
  monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")


def test_definitions_load():
  assert cq_dagster_embedded.defs.resolve_job_def("hackernews_job")


def test_ingest_hn_succeeds(tmp_path, monkeypatch):
  fake_cloudquery(tmp_path, monkeypatch, 0)
  ingest_hn(build_asset_context())


def test_ingest_hn_fails_on_nonzero_exit(tmp_path, monkeypatch):
  fake_cloudquery(tmp_path, monkeypatch, 3)
  with pytest.raises(Exception, match="exit code 3"):
    ingest_hn(build_asset_context())
