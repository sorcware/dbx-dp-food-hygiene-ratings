# dbx-dp-food-hygiene-ratings

Databricks Asset Bundle that ingests and transforms UK Food Hygiene Rating Scheme (FHRS) data from the [Food Standards Agency API](https://api.ratings.food.gov.uk) into a medallion lakehouse architecture.

## Architecture

```
FSA API  →  ingest_fhrs (Job)  →  raw tables  →  Pipeline (bronze → silver → gold)
```

### Project Structure

```
├── databricks.yml              # Bundle config — job, pipeline, artifact
├── pyproject.toml              # Package definition (datasources wheel)
├── dist/                       # Built wheel output (gitignored)
└── src/
    ├── datasources/            # Packaged as a wheel and installed on the cluster
    │   └── fhrs/
    │       ├── _datasource.py  # FhrsDataSource base class
    │       ├── _http.py        # Shared HTTP session + partition dataclasses
    │       ├── establishments.py
    │       ├── authorities.py
    │       ├── countries.py
    │       ├── regions.py
    │       ├── business_types.py
    │       ├── ratings.py
    │       ├── rating_operators.py
    │       ├── sort_options.py
    │       └── scheme_types.py
    ├── jobs/
    │   └── ingest_fhrs.py      # spark_python_task entry point
    └── pipelines/
        ├── bronze.py
        ├── silver.py
        └── gold.py
```

### Job: `ingest_fhrs`

A two-task Databricks job:

1. **ingest** (`src/jobs/ingest_fhrs.py`) — Registers and runs custom PySpark data sources to fetch data from the FSA API and append it to raw Unity Catalog tables under `workspace.fhrs.*`. The data sources are packaged as a wheel (`src/datasources/`) and installed into the task environment at deploy time. Each reference endpoint stores the full API JSON response per run; establishment data is fetched per-authority as XML.

2. **transform** — Runs the Lakeflow Spark Declarative Pipeline after ingest completes.

### Datasources Package

The `src/datasources/` directory is built into a Python wheel and uploaded to the workspace by the bundle on deploy. It is installed as a library dependency of the ingest task, making `from datasources.fhrs import ALL_DATA_SOURCES` available in `ingest_fhrs.py` without any path manipulation.

Each FSA endpoint has its own file under `src/datasources/fhrs/` as a subclass of `FhrsDataSource`, which implements the PySpark `DataSource` API. The `ALL_DATA_SOURCES` list in `src/datasources/fhrs/__init__.py` aggregates all sources for registration.

### Pipeline: `food_hygiene_pipeline`

A serverless Lakeflow Declarative Pipeline writing to `workspace.fhrs` with three layers:

| Layer | Source | Tables |
|-------|--------|--------|
| **Bronze** (`src/pipelines/bronze.py`) | Raw tables | `bronze_establishments`, `bronze_authorities`, `bronze_countries`, `bronze_regions`, `bronze_business_types`, `bronze_ratings`, `bronze_rating_operators`, `bronze_sort_options`, `bronze_scheme_types` |
| **Silver** (`src/pipelines/silver.py`) | Bronze tables | `silver_*` — SCD Type 2 via `AUTO CDC`, keyed per entity |
| **Gold** (`src/pipelines/gold.py`) | Silver tables | `gold_*` — Materialized views of current (non-expired) records |

**Bronze** parses raw JSON (reference data) and XML (establishments) into typed columns. Reference tables use `from_json` + `explode` to unpack the API array response; establishment tables use `from_xml` + `explode` on the XML bulk download.

**Silver** applies SCD Type 2 change tracking using `AUTO CDC`, sequenced by `ingest_timestamp`, excluding the `ingest_timestamp` column from the target table.

**Gold** filters silver to current records (`__END_AT IS NULL`) as materialized views.

## Prerequisites

- Databricks workspace with serverless pipelines enabled
- Unity Catalog with a `workspace` catalog and `fhrs` schema
- Databricks CLI installed and authenticated (`databricks configure`)
- [uv](https://docs.astral.sh/uv/) package manager

## Development Setup

1. Install dependencies:
   ```bash
   uv sync
   ```

2. Validate the bundle:
   ```bash
   databricks bundle validate
   ```

## Deployment

The bundle builds the datasources wheel automatically on deploy using `uv build`.

```bash
# Deploy to dev (default target) — builds wheel, uploads, deploys job + pipeline
databricks bundle deploy

# Run the full ingest + pipeline job
databricks bundle run ingest_fhrs
```

## Data Sources

All data is sourced from the [Food Standards Agency Open Data API](https://api.ratings.food.gov.uk):

| Endpoint | Description |
|----------|-------------|
| `/Authorities` | Local authority metadata and XML file URLs |
| `/Countries` | Country reference data |
| `/Regions` | Region reference data |
| `/BusinessTypes` | Business type reference data |
| `/Ratings` | Rating value reference data |
| `/RatingOperators` | Rating operator reference data |
| `/SortOptions` | Sort option reference data |
| `/SchemeTypes` | Scheme type reference data |
| Per-authority XML files | Establishment inspection records |
