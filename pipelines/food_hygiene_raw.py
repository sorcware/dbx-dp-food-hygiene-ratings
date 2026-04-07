from pyspark import pipelines as dp

@dp.table(
    name="food_hygiene_raw",
    comment="Current snapshot of all establishments from the API, refreshed each run",
    table_properties={"quality": "raw"}
)
def food_hygiene_raw():
    return (
        spark.read
            .format("food_hygiene")
            .option("pageSize", "100")
            .load()
    )