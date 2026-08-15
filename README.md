# Pre-commit and CI for PEP8 checks

This repository includes a pre-commit configuration and a GitHub Actions workflow to enforce PEP8 and related best practices using `ruff`, `black`, `isort`, and `mypy`.

Quick start (local):

```bash
python -m pip install -r requirements-dev.txt
pre-commit install
pre-commit run --all-files
```

CI: The workflow is at `.github/workflows/ci.yml` and runs on push and pull_request.
