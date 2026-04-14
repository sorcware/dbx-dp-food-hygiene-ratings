from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, IntegerType, LongType, StringType,
    StructField, StructType, ArrayType
)
from databricks.sdk.runtime import spark


_RAW_CATALOG = spark.conf.get("raw_catalog")
_RAW_SCHEMA = spark.conf.get("raw_schema")
_CATALOG = spark.conf.get("bronze_catalog")
_SCHEMA = spark.conf.get("bronze_schema")

_TABLE_PROPERTIES = {"quality": "bronze"}

# ---------------------------------------------------------------------------
# Establishment schemas (XML)
# ---------------------------------------------------------------------------

_establishment_schema = StructType([
    StructField("FHRSID", LongType()),
    StructField("BusinessName", StringType()),
    StructField("BusinessType", StringType()),
    StructField("AddressLine1", StringType()),
    StructField("AddressLine2", StringType()),
    StructField("AddressLine3", StringType()),
    StructField("AddressLine4", StringType()),
    StructField("PostCode", StringType()),
    StructField("RatingValue", StringType()),
    StructField("RatingDate", DateType()),
    StructField("LocalAuthorityName", StringType()),
    StructField("SchemeType", StringType()),
])

_file_schema = StructType([
    StructField("EstablishmentCollection", StructType([
        StructField("EstablishmentDetail", ArrayType(_establishment_schema)),
    ])),
])

# ---------------------------------------------------------------------------
# Reference data schemas (JSON)
# ---------------------------------------------------------------------------

_country_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("nameKey", StringType()),
    StructField("code", StringType()),
])

_region_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("nameKey", StringType()),
    StructField("code", StringType()),
])

_authority_schema = StructType([
    StructField("LocalAuthorityId", IntegerType()),
    StructField("LocalAuthorityIdCode", StringType()),
    StructField("Name", StringType()),
    StructField("FriendlyName", StringType()),
    StructField("Url", StringType()),
    StructField("SchemeUrl", StringType()),
    StructField("Email", StringType()),
    StructField("RegionName", StringType()),
    StructField("FileName", StringType()),
    StructField("FileNameWelsh", StringType()),
    StructField("EstablishmentCount", IntegerType()),
    StructField("CreationDate", StringType()),
    StructField("LastPublishedDate", StringType()),
    StructField("SchemeType", IntegerType()),
])

_business_type_schema = StructType([
    StructField("BusinessTypeId", IntegerType()),
    StructField("BusinessTypeName", StringType()),
])

_rating_schema = StructType([
    StructField("ratingId", IntegerType()),
    StructField("ratingName", StringType()),
    StructField("ratingKey", StringType()),
    StructField("ratingKeyName", StringType()),
    StructField("schemeTypeId", IntegerType()),
])

_rating_operator_schema = StructType([
    StructField("ratingOperatorId", IntegerType()),
    StructField("ratingOperatorName", StringType()),
    StructField("ratingOperatorKey", StringType()),
])

_sort_option_schema = StructType([
    StructField("sortOptionId", IntegerType()),
    StructField("sortOptionName", StringType()),
    StructField("sortOptionKey", StringType()),
])

_scheme_type_schema = StructType([
    StructField("schemeTypeid", IntegerType()),
    StructField("schemeTypeName", StringType()),
    StructField("schemeTypeKey", StringType()),
])


# ---------------------------------------------------------------------------
# Establishments (XML bulk download)
# ---------------------------------------------------------------------------

@dp.table(
    name=f"{_CATALOG}.{_SCHEMA}.bronze_establishments",
    comment="Parsed food hygiene establishments, full history",
    table_properties=_TABLE_PROPERTIES,
)
def bronze_establishments():
    return (
        spark.readStream.table(f"{_RAW_CATALOG}.{_RAW_SCHEMA}.establishments")
            .select(
                F.explode(
                    F.from_xml("raw", _file_schema)
                        .getField("EstablishmentCollection")
                        .getField("EstablishmentDetail")
                ).alias("e"),
                F.col("ingest_timestamp"),
            )
            .select(
                F.col("e.FHRSID").alias("FHRSID"),
                F.col("e.BusinessName").alias("BusinessName"),
                F.col("e.BusinessType").alias("BusinessType"),
                F.col("e.AddressLine1").alias("AddressLine1"),
                F.col("e.AddressLine2").alias("AddressLine2"),
                F.col("e.AddressLine3").alias("AddressLine3"),
                F.col("e.AddressLine4").alias("AddressLine4"),
                F.col("e.PostCode").alias("PostCode"),
                F.col("e.RatingValue").alias("RatingValue"),
                F.col("e.RatingDate").alias("RatingDate"),
                F.col("e.LocalAuthorityName").alias("LocalAuthorityName"),
                F.col("e.SchemeType").alias("SchemeType"),
                F.col("ingest_timestamp"),
            )
    )


# ---------------------------------------------------------------------------
# Reference data (JSON API responses)
# ---------------------------------------------------------------------------

# (source_table, array_key, bronze_table_name, item_schema)
# Each raw row is the full API response; array_key is the top-level key holding the array.
_REFERENCE_SOURCES = [
    (f"{_RAW_CATALOG}.{_RAW_SCHEMA}.countries",        "countries",       "bronze_countries",        _country_schema),
    (f"{_RAW_CATALOG}.{_RAW_SCHEMA}.regions",          "regions",         "bronze_regions",          _region_schema),
    (f"{_RAW_CATALOG}.{_RAW_SCHEMA}.authorities",      "authorities",     "bronze_authorities",      _authority_schema),
    (f"{_RAW_CATALOG}.{_RAW_SCHEMA}.business_types",   "businessTypes",   "bronze_business_types",   _business_type_schema),
    (f"{_RAW_CATALOG}.{_RAW_SCHEMA}.ratings",          "ratings",         "bronze_ratings",          _rating_schema),
    (f"{_RAW_CATALOG}.{_RAW_SCHEMA}.rating_operators", "ratingOperator",  "bronze_rating_operators", _rating_operator_schema),
    (f"{_RAW_CATALOG}.{_RAW_SCHEMA}.sort_options",     "sortOptions",     "bronze_sort_options",     _sort_option_schema),
    (f"{_RAW_CATALOG}.{_RAW_SCHEMA}.scheme_types",     "schemeTypes",     "bronze_scheme_types",     _scheme_type_schema),
]


def _make_reference_table(source_table, array_key, table_name, item_schema):
    def _reader():
        return (
            spark.readStream.table(source_table)
                .select(
                    F.explode(
                        F.from_json(
                            F.get_json_object(F.col("raw"), f"$.{array_key}"),
                            ArrayType(item_schema),
                        )
                    ).alias("item"),
                    F.col("ingest_timestamp"),
                )
                .select("item.*", "ingest_timestamp")
        )

    _reader.__name__ = table_name
    dp.table(name=f"{_CATALOG}.{_SCHEMA}.{table_name}", table_properties=_TABLE_PROPERTIES)(_reader)


for _source, _key, _name, _schema in _REFERENCE_SOURCES:
    _make_reference_table(_source, _key, _name, _schema)
