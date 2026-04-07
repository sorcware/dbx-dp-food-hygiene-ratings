from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.types import (
    StructType, StructField, ArrayType, StringType, LongType, DateType
)

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

dp.create_streaming_table(
    name="food_hygiene_bronze",
    comment="Parsed food hygiene establishments, full history",
    table_properties={
        "quality": "bronze",
    }
)

@dp.append_flow(target="food_hygiene_bronze")
def load_food_hygiene_bronze():
    return (
        spark.readStream.table("workspace.fhrs.multiplex_raw")
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