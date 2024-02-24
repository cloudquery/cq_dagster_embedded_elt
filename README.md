# CloudQuery Dagster Embedded Example

This example contains an example of how to run CloudQuery as ingestion step
inside a dagster asset utilizing solely dagster orchestrator and resource managemetn without relying on other cloud providers and orchestrators.

This example pipeline will be fully runnable both locally and in the cloud using the same configuration, code and queries! Utilizing Dagster, DuckDB (MotherDuck) and CloudQuery local & cloud capabilities.

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Setup

To run this example locally

```
git clone https://github.com/cloudquery/cq_dagster_embedded
cd cq_dagster_embedded
pip install -e ".[dev]"

# Load it in the web UI
dagster-webserver
```

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `cq_dagster_embedded/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `cq_dagster_embedded_tests` directory and you can run tests using `pytest`:

```bash
pytest cq_dagster_embedded_tests
```