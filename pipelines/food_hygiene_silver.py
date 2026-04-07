from pyspark import pipelines as dp
from pyspark.sql.functions import get_json_object

@dp.table(
    name="food_hygiene_establishments",
    comment="Parsed establishments streaming from bronze",
    table_properties={"quality": "silver"}
)
@dp.expect("valid_business_name", "BusinessName IS NOT NULL")
@dp.expect("valid_rating", "RatingValue IS NOT NULL")
def food_hygiene_establishments():
    return (
        dp.read_stream("food_hygiene_bronze")
            .select(
                get_json_object("raw", "$.FHRSID").alias("FHRSID"),
                get_json_object("raw", "$.BusinessName").alias("BusinessName"),
                get_json_object("raw", "$.BusinessType").alias("BusinessType"),
                get_json_object("raw", "$.AddressLine1").alias("AddressLine1"),
                get_json_object("raw", "$.AddressLine2").alias("AddressLine2"),
                get_json_object("raw", "$.PostCode").alias("PostCode"),
                get_json_object("raw", "$.RatingValue").alias("RatingValue"),
                get_json_object("raw", "$.RatingDate").alias("RatingDate"),
                get_json_object("raw", "$.LocalAuthorityName").alias("LocalAuthorityName"),
                get_json_object("raw", "$.SchemeType").alias("SchemeType"),
            )
    )