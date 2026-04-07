from pyspark.sql import functions as F
from pyspark import pipelines as dp

dp.create_streaming_table(
    name="food_hygiene_bronze",
    comment="Append-only history of raw API responses, never reprocessed",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)

@dp.append_flow(target="food_hygiene_bronze")
def load_food_hygiene_bronze():
    return (
        dp.read_stream("food_hygiene_raw")
            .select(
                F.col("raw"),
                F.current_timestamp().alias("ingest_timestamp"),
            )
    )