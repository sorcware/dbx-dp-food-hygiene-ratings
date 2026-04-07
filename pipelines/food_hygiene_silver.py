from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name="food_hygiene_establishments",
    comment="Latest version of each establishment, deduplicated by FHRSID",
    table_properties={"quality": "silver"}
)
@dp.expect("valid_business_name", "BusinessName IS NOT NULL")
@dp.expect("valid_rating", "RatingValue IS NOT NULL")
def food_hygiene_establishments():
    w = Window.partitionBy("FHRSID").orderBy(F.col("ingest_timestamp").desc())
    return (
        spark.read.table("food_hygiene_bronze")
            .select(
                F.get_json_object("raw", "$.FHRSID").cast("long").alias("FHRSID"),
                F.get_json_object("raw", "$.BusinessName").alias("BusinessName"),
                F.get_json_object("raw", "$.BusinessType").alias("BusinessType"),
                F.get_json_object("raw", "$.AddressLine1").alias("AddressLine1"),
                F.get_json_object("raw", "$.AddressLine2").alias("AddressLine2"),
                F.get_json_object("raw", "$.PostCode").alias("PostCode"),
                F.get_json_object("raw", "$.RatingValue").alias("RatingValue"),
                F.get_json_object("raw", "$.RatingDate").cast("date").alias("RatingDate"),
                F.get_json_object("raw", "$.LocalAuthorityName").alias("LocalAuthorityName"),
                F.get_json_object("raw", "$.SchemeType").alias("SchemeType"),
                F.col("ingest_timestamp"),
            )
            .withColumn("_row_num", F.row_number().over(w))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num", "ingest_timestamp")
    )