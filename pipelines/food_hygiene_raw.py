from pyspark import pipelines as dp

@dp.table(
    name="food_hygiene_raw",
    comment="Append only raw API responses, never reprocessed",
    table_properties={
        "quality": "raw",
        "pipelines.reset.allowed": "false"
    }
)
def food_hygiene_raw():
    return (
        spark.read
            .format("food_hygiene")
            .option("pageSize", "100")
            .load()
    )