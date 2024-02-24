import os
from dagster import load_assets_from_modules, materialize

from cq_dagster_embedded import assets, resources

def test_elt_pipeline():
  loadded_assets = load_assets_from_modules([assets])
  materialize(loadded_assets, resources=resources)