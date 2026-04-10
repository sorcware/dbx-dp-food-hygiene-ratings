from pyspark.sql import functions as F
from databricks.sdk.runtime import spark

from datasources.fhrs import ALL_DATA_SOURCES

# ScoreDescriptors are excluded — they are per-establishment lookups, not
# a batch reference dataset.

_SOURCES = [
    ("fhrs_establishments",   "workspace.fhrs.establishments"),
    ("fhrs_countries",        "workspace.fhrs.countries"),
    ("fhrs_regions",          "workspace.fhrs.regions"),
    ("fhrs_authorities",      "workspace.fhrs.authorities"),
    ("fhrs_business_types",   "workspace.fhrs.business_types"),
    ("fhrs_ratings",          "workspace.fhrs.ratings"),
    ("fhrs_rating_operators", "workspace.fhrs.rating_operators"),
    ("fhrs_sort_options",     "workspace.fhrs.sort_options"),
    ("fhrs_scheme_types",     "workspace.fhrs.scheme_types"),
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
