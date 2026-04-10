from pyspark import pipelines as dp
from pyspark.sql import functions as F

# (bronze_table, key_col, silver_table)
_SILVER_SOURCES = [
    ("bronze_establishments",   "FHRSID",           "silver_establishments"),
    ("bronze_countries",        "id",               "silver_countries"),
    ("bronze_regions",          "id",               "silver_regions"),
    ("bronze_authorities",      "LocalAuthorityId", "silver_authorities"),
    ("bronze_business_types",   "BusinessTypeId",   "silver_business_types"),
    ("bronze_ratings",          "ratingId",         "silver_ratings"),
    ("bronze_rating_operators", "ratingOperatorId", "silver_rating_operators"),
    ("bronze_sort_options",     "sortOptionId",     "silver_sort_options"),
    ("bronze_scheme_types",     "schemeTypeid",     "silver_scheme_types"),
]


def _make_silver_scd2(bronze_table, key_col, silver_table):
    dp.create_streaming_table(
        name=silver_table,
        table_properties={"quality": "silver"},
    )

    dp.create_auto_cdc_flow(
        target=silver_table,
        source=bronze_table,
        keys=[key_col],
        sequence_by=F.col("ingest_timestamp"),
        stored_as_scd_type=2,
        except_column_list=["ingest_timestamp"],
    )


for _bronze, _key_col, _silver in _SILVER_SOURCES:
    _make_silver_scd2(_bronze, _key_col, _silver)
