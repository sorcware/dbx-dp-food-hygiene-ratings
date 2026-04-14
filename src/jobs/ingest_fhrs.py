import sys

from pyspark.sql import functions as F
from databricks.sdk.runtime import spark

from datasources.fhrs import ALL_DATA_SOURCES

_CATALOG = sys.argv[1] if len(sys.argv) > 1 else "workspace"
_SCHEMA = sys.argv[2] if len(sys.argv) > 2 else "fhrs"

# ScoreDescriptors are excluded — they are per-establishment lookups, not
# a batch reference dataset.

_SOURCES = [
    ("fhrs_establishments",   f"{_CATALOG}.{_SCHEMA}.establishments"),
    ("fhrs_countries",        f"{_CATALOG}.{_SCHEMA}.countries"),
    ("fhrs_regions",          f"{_CATALOG}.{_SCHEMA}.regions"),
    ("fhrs_authorities",      f"{_CATALOG}.{_SCHEMA}.authorities"),
    ("fhrs_business_types",   f"{_CATALOG}.{_SCHEMA}.business_types"),
    ("fhrs_ratings",          f"{_CATALOG}.{_SCHEMA}.ratings"),
    ("fhrs_rating_operators",  f"{_CATALOG}.{_SCHEMA}.rating_operators"),
    ("fhrs_sort_options",     f"{_CATALOG}.{_SCHEMA}.sort_options"),
    ("fhrs_scheme_types",     f"{_CATALOG}.{_SCHEMA}.scheme_types"),
]


for _ds in ALL_DATA_SOURCES:
    spark.dataSource.register(_ds)

for _format, _table in _SOURCES:
    df = spark.read.format(_format).load()
    if _format == "fhrs_establishments":
        df = df.select(
            F.current_timestamp().alias("ingest_timestamp"),
            F.col("authority_id"),
            F.col("authority_name"),
            F.col("file_url"),
            F.col("last_published").cast("timestamp").alias("last_published"),
            F.col("raw"),
        )
    else:
        df = df.select(
            F.col("raw"),
            F.current_timestamp().alias("ingest_timestamp"),
        )
    (
        df
        .write
        .mode("append")
        .saveAsTable(_table)
    )
