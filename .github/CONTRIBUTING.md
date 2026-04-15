# Contributing

Thanks for your interest in contributing! Here's how to get started.

## Prerequisites

- A Databricks workspace with Unity Catalog enabled
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) v0.22+
- [uv](https://docs.astral.sh/uv/) for Python dependency management
- Python 3.12

## Setting Up Locally

```bash
git clone https://github.com/atedimmock/dbx-dp-food-hygiene-ratings.git
cd dbx-dp-food-hygiene-ratings
uv sync
```

Copy `.env.template` to `.env` and fill in your workspace URL:

```bash
cp .env.template .env
# Edit .env and set DATABRICKS_HOST
```

Validate the bundle before deploying:

```bash
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
databricks bundle validate
```

## Making Changes

1. Fork the repository and create a branch from `main`.
2. Make your changes.
3. Ensure `databricks bundle validate` passes with `DATABRICKS_HOST` set or a configured CLI profile.
4. Open a pull request with a clear description of the change and why it's needed.

## Code Style

This project uses Python. Please follow these conventions:

- Format with [ruff](https://docs.astral.sh/ruff/) (`uv run ruff format .`)
- Lint with ruff (`uv run ruff check .`)
- Keep new public functions and classes type-annotated

## Commit Messages

Use short, imperative-mood subject lines (e.g. `Add silver layer SCD Type 2 support`). Reference any relevant issue numbers in the body.

## Reporting Bugs

Open a [GitHub Issue](https://github.com/atedimmock/dbx-dp-food-hygiene-ratings/issues) with steps to reproduce, expected behaviour, and actual behaviour. Please check for existing issues first.

## Security Issues

Please **do not** open a public issue for security vulnerabilities. See [SECURITY.md](../SECURITY.md) for the responsible disclosure process.
