from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType
import json
import requests
from databricks.sdk.runtime import spark
from pyspark.sql.datasource import InputPartition
from dataclasses import dataclass

class FoodHygieneDataSource(DataSource):

    @classmethod
    def name(cls):
        return "food_hygiene"

    def schema(self):
        return StructType([
            StructField("raw", StringType())
        ])

    def reader(self, schema):
        return FoodHygieneReader(self.options)


class FoodHygieneReader(DataSourceReader):

    def __init__(self, options):
        self.page_size = int(options.get("pageSize", "100"))

    def read(self, partition):

        base_url = "https://api.ratings.food.gov.uk/Establishments"
        headers = {
            "Accept": "application/json",
            "x-api-version": "2"
        }

        # partition.value is the authorityId
        page = 1
        while True:
            response = requests.get(
                base_url,
                headers=headers,
                params={
                    "localAuthorityId": partition.value,
                    "pageNumber": page,
                    "pageSize": self.page_size
                }
            )
            
            # Check if request was successful
            if response.status_code != 200:
                print(f"API request failed with status {response.status_code} for authority {partition.value}, page {page}")
                break
            
            # Check if response has content
            if not response.text:
                print(f"Empty response for authority {partition.value}, page {page}")
                break
                
            data = response.json()
            establishments = data.get("establishments", [])

            if not establishments:
                break

            for e in establishments:
                yield (json.dumps(e),)

            page += 1

    def partitions(self):

        response = requests.get(
            "https://api.ratings.food.gov.uk/Authorities",
            headers={
                "Accept": "application/json",
                "x-api-version": "2"
            }
        )
        authorities = response.json().get("authorities", [])
        return [AuthorityPartition(a["LocalAuthorityId"]) for a in authorities]


@dataclass
class AuthorityPartition(InputPartition):
    value: int

spark.dataSource.register(FoodHygieneDataSource)