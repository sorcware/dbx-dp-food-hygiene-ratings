from pyspark.sql import functions as F
from pyspark import pipelines as dp

# Create the streaming table first
dp.create_streaming_table(
    name="food_hygiene_bronze",
    comment="Streaming bronze layer from raw",
    table_properties={"quality": "bronze"}
)

# Append flow to load data from batch source into streaming table
@dp.append_flow(target="food_hygiene_bronze", once=True)
def load_food_hygiene_bronze():
    return (spark.read.table("food_hygiene_raw")
        .withColumn("ingest_timestamp", F.current_timestamp()))