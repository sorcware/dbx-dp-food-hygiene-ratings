from pyspark import pipelines as dp
from pyspark.sql import functions as F

# (bronze_table, json_key, key_col, silver_table)
# json_key=None means key_col already exists as a native column
_SILVER_SOURCES = [
    ("bronze_establishments",   None, "FHRSID",             "silver_establishments"),
    ("bronze_countries",        None, "id",                 "silver_countries"),
    ("bronze_regions",          None, "id",                 "silver_regions"),
    ("bronze_authorities",      None, "LocalAuthorityId",   "silver_authorities"),
    ("bronze_business_types",   None, "BusinessTypeId",     "silver_business_types"),
    ("bronze_ratings",          None, "ratingId",           "silver_ratings"),
    ("bronze_rating_operators", None, "ratingOperatorId",   "silver_rating_operators"),
    ("bronze_sort_options",     None, "sortOptionId",       "silver_sort_options"),
    ("bronze_scheme_types",     None, "schemeTypeid",       "silver_scheme_types"),
]


def _make_silver_scd2(bronze_table, json_key, key_col, silver_table):
    if json_key is not None:
        tmp_name = f"_tmp_{silver_table}"

        def _prepare():
            return (
                spark.readStream.table(bronze_table)
                .withColumn(key_col, F.get_json_object(F.col("raw"), f"$.{json_key}"))
            )

        dp.temporary_view(name=tmp_name)(_prepare)
        source = tmp_name
    else:
        source = bronze_table

    dp.create_streaming_table(
        name=silver_table,
        table_properties={"quality": "silver"},
    )

    dp.create_auto_cdc_flow(
        target=silver_table,
        source=source,
        keys=[key_col],
        sequence_by=F.col("ingest_timestamp"),
        stored_as_scd_type=2,
        except_column_list=["ingest_timestamp"],
    )


for _bronze, _json_key, _key_col, _silver in _SILVER_SOURCES:
    _make_silver_scd2(_bronze, _json_key, _key_col, _silver)
