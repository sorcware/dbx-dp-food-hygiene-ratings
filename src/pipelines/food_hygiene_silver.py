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
    w = Window.partitionBy("FHRSID").orderBy(F.col("ingest_timestamp"))
    return (
        spark.read.table("food_hygiene_bronze")
            .withColumn("valid_from", F.col("ingest_timestamp"))
            .withColumn("valid_to", F.lead("ingest_timestamp").over(w))
            .withColumn("is_current", F.col("valid_to").isNull())
            .drop("ingest_timestamp")
    )