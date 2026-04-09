from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name="food_hygiene_establishments",
    comment="SCD type 2 history of each establishment derived from bronze append history",
    table_properties={"quality": "silver"}
)
@dp.expect("valid_business_name", "BusinessName IS NOT NULL")
@dp.expect("valid_rating", "RatingValue IS NOT NULL")
def food_hygiene_establishments():
    _data_cols = [
        "BusinessName", "BusinessType",
        "AddressLine1", "AddressLine2", "AddressLine3", "AddressLine4",
        "PostCode", "RatingValue", "RatingDate",
        "LocalAuthorityName", "SchemeType",
    ]
    w_dedup = Window.partitionBy("FHRSID", "_row_hash").orderBy(F.col("ingest_timestamp"))
    w_scd = Window.partitionBy("FHRSID").orderBy(F.col("valid_from"))
    return (
        spark.read.table("food_hygiene_bronze")
            .withColumn("_row_hash", F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in _data_cols])))
            .withColumn("_rn", F.row_number().over(w_dedup))
            .filter(F.col("_rn") == 1)
            .withColumn("valid_from", F.col("ingest_timestamp"))
            .withColumn("valid_to", F.lead("valid_from").over(w_scd))
            .withColumn("is_current", F.col("valid_to").isNull())
            .drop("ingest_timestamp", "_row_hash", "_rn")
    )