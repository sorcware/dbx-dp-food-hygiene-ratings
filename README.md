# dbx-dp-food-hygiene-ratings

Databricks Asset Bundle that ingests and transforms UK Food Hygiene Rating Scheme (FHRS) data from the [Food Standards Agency API](https://api.ratings.food.gov.uk) into a medallion lakehouse architecture.

## Architecture

```
FSA API  →  ingest_fhrs (Job)  →  raw catalog  →  Pipeline (bronze → silver → gold)
```

Data flows across four separate Unity Catalog catalogs:

| Catalog | Schema | Contents |
|---------|--------|----------|
| `raw` | `fhrs` | Raw API responses written by the ingest job |
| `bronze` | `fhrs` | Parsed, typed tables produced by the pipeline |
| `silver` | `fhrs` | SCD Type 2 history tables |
| `gold` | `fhrs` | Current (non-expired) materialized views |

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

1. **ingest** (`src/jobs/ingest_fhrs.py`) — Registers and runs custom PySpark data sources to fetch data from the FSA API and append it to raw Unity Catalog tables. The target catalog and schema are passed as task parameters from the bundle variables. The data sources are packaged as a wheel (`src/datasources/`) and installed into the task environment at deploy time. Each reference endpoint stores the full API JSON response per run; establishment data is fetched per-authority as XML.

2. **transform** — Runs the Lakeflow Spark Declarative Pipeline after ingest completes.

### Datasources Package

The `src/datasources/` directory is built into a Python wheel and uploaded to the workspace by the bundle on deploy. It is installed as a library dependency of the ingest task, making `from datasources.fhrs import ALL_DATA_SOURCES` available in `ingest_fhrs.py` without any path manipulation.

Each FSA endpoint has its own file under `src/datasources/fhrs/` as a subclass of `FhrsDataSource`, which implements the PySpark `DataSource` API. The `ALL_DATA_SOURCES` list in `src/datasources/fhrs/__init__.py` aggregates all sources for registration.

### Pipeline: `food_hygiene_pipeline`

A serverless Lakeflow Declarative Pipeline with three layers. The catalog and schema for each layer are passed via the bundle `configuration` block and read at runtime via `spark.conf.get()`.

| Layer | Reads from | Writes to | Tables |
|-------|-----------|-----------|--------|
| **Bronze** (`src/pipelines/bronze.py`) | `raw.fhrs.*` | `bronze.fhrs.*` | `bronze_establishments`, `bronze_authorities`, `bronze_countries`, `bronze_regions`, `bronze_business_types`, `bronze_ratings`, `bronze_rating_operators`, `bronze_sort_options`, `bronze_scheme_types` |
| **Silver** (`src/pipelines/silver.py`) | `bronze.fhrs.*` | `silver.fhrs.*` | `silver_*` — SCD Type 2 via `AUTO CDC`, keyed per entity |
| **Gold** (`src/pipelines/gold.py`) | `silver.fhrs.*` | `gold.fhrs.*` | `gold_*` — Materialized views of current (non-expired) records |

**Bronze** parses raw JSON (reference data) and XML (establishments) into typed columns. Reference tables use `from_json` + `explode` to unpack the API array response; establishment tables use `from_xml` + `explode` on the XML bulk download.

**Silver** applies SCD Type 2 change tracking using `AUTO CDC`, sequenced by `ingest_timestamp`, excluding the `ingest_timestamp` column from the target table.

**Gold** filters silver to current records (`__END_AT IS NULL`) as materialized views.

## Prerequisites

- Databricks workspace with serverless pipelines enabled
- Unity Catalog with four catalogs: `raw`, `bronze`, `silver`, `gold` (catalog creation requires metastore admin)
- Databricks CLI installed and authenticated (`databricks configure`)
- Python 3.12
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

The bundle builds the datasources wheel automatically on deploy using `uv build`. Catalog and schema names are configurable via bundle variables.

The bundle creates the schemas automatically on deploy. The four catalogs (`raw`, `bronze`, `silver`, `gold`) must already exist.

Set `DATABRICKS_HOST` in your environment or use a configured Databricks CLI profile before running bundle commands.

In development mode, Databricks automatically prefixes schema names with `dev_<username>_` (e.g. `dev_atedimmock_fhrs`). The bundle resolves this correctly by referencing deployed schema resource names rather than raw variable values.

The default catalog and schema values are defined in `databricks.yml`. You can deploy with those defaults as-is, or override individual bundle variables at the CLI with `--var` when needed.

```bash
# Deploy using the defaults from databricks.yml
databricks bundle deploy -p <profile>

# Or deploy with one-off variable overrides
databricks bundle deploy -p <profile> \
  --var="raw_catalog=my_raw" \
  --var="raw_schema=fhrs" \
  --var="bronze_catalog=my_bronze" \
  --var="bronze_schema=fhrs" \
  --var="silver_catalog=my_silver" \
  --var="silver_schema=fhrs" \
  --var="gold_catalog=my_gold" \
  --var="gold_schema=fhrs"

# Run the full ingest + pipeline job
databricks bundle run ingest_fhrs
```

### Bundle Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `raw_catalog` | `raw` | Catalog for raw ingest tables |
| `raw_schema` | `fhrs` | Schema for raw ingest tables |
| `bronze_catalog` | `bronze` | Catalog for bronze tables |
| `bronze_schema` | `fhrs` | Schema for bronze tables |
| `silver_catalog` | `silver` | Catalog for silver tables |
| `silver_schema` | `fhrs` | Schema for silver tables |
| `gold_catalog` | `gold` | Catalog for gold tables |
| `gold_schema` | `fhrs` | Schema for gold tables |

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
