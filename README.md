# dbx-dp-food-hygiene-ratings

Databricks Asset Bundle that ingests and transforms UK Food Hygiene Rating Scheme (FHRS) data from the [Food Standards Agency API](https://api.ratings.food.gov.uk) into a medallion lakehouse architecture.

## Architecture

```
FSA API  →  ingest_fhrs (Job)  →  raw tables  →  Pipeline (bronze → silver → gold)
```

### Job: `ingest_fhrs`

A two-task Databricks job:

1. **ingest** (`src/jobs/ingest_fhrs.py`) — Uses custom PySpark data sources to fetch data from the FSA API and append it to raw Unity Catalog tables under `workspace.fhrs.*`. Each reference endpoint (countries, regions, authorities, etc.) stores the full API JSON response per run. Establishment data is fetched per-authority as XML.

2. **transform** — Runs the Lakeflow Spark Declarative Pipeline after ingest completes.

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
- Databricks CLI with the bundle extension

## Development Setup

```bash
uv sync
databricks bundle validate
```

## Deployment

```bash
# Deploy to dev (default target)
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
