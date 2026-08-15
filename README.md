# Pre-commit and CI for PEP8 checks

This repository includes a pre-commit configuration and a GitHub Actions workflow to enforce PEP8 and related best practices using `ruff`, `black`, `isort`, and `mypy`.

Quick start (local):

```bash
python -m pip install -r requirements-dev.txt
pre-commit install
pre-commit run --all-files
```

CI: The workflow is at `.github/workflows/ci.yml` and runs on push and pull_request.

Databricks / local development
--------------------------------

This repository runs on a Databricks cluster in production. To run
linters and simple local tests without a cluster, use the provided
`dev_stubs` helpers which inject lightweight stand-ins for `pyspark`
and the notebook `display()` function.

Use locally by adding at the top of your test runner or a local entrypoint:

```python
import dev_stubs
dev_stubs.activate()
# now imports like `from pyspark.sql import functions as F` and calls to
# `display()` will work for linting and quick local checks
```

The stubs are intentionally minimal — they allow imports and simple
operations for static checks and unit tests, but they do not emulate a
real Spark runtime. For integration tests use a real Databricks cluster
or a local PySpark installation.

