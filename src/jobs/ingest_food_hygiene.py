from pyspark.sql import functions as F
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType, LongType
from databricks.sdk.runtime import spark
from dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class FoodHygieneDataSource(DataSource):

    @classmethod
    def name(cls):
        return "food_hygiene"

    def schema(self):
        return StructType([
            StructField("authority_id", LongType()),
            StructField("authority_name", StringType()),
            StructField("file_url", StringType()),
            StructField("last_published", StringType()),
            StructField("raw", StringType()),
        ])

    def reader(self, schema):
        return FoodHygieneReader()


class FoodHygieneReader(DataSourceReader):

    def __init__(self):
        pass

    def read(self, partition):
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(partition.file_url, timeout=120, allow_redirects=True)
        response.raise_for_status()

        yield (
            partition.authority_id,
            partition.authority_name,
            partition.file_url,
            partition.last_published,
            response.text,
        )

    def partitions(self):
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(
            "https://api.ratings.food.gov.uk/Authorities",
            headers={"Accept": "application/json", "x-api-version": "2"},
            timeout=30,
        )
        response.raise_for_status()
        authorities = response.json().get("authorities", [])
        return [
            AuthorityPartition(
                authority_id=a["LocalAuthorityId"],
                authority_name=a["Name"],
                file_url=a["FileName"],
                last_published=a["LastPublishedDate"],
            )
            for a in authorities
            if a.get("FileName")
        ]


@dataclass
class AuthorityPartition(InputPartition):
    authority_id: int
    authority_name: str
    file_url: str
    last_published: str


spark.dataSource.register(FoodHygieneDataSource)

raw_df = spark.read.format("food_hygiene").load()

(
    raw_df.select(
        F.current_timestamp().alias("ingest_timestamp"),
        F.col("authority_id"),
        F.col("authority_name"),
        F.col("file_url"),
        F.col("last_published").cast("timestamp").alias("last_published"),
        F.col("raw"),
    )
    .write
    .mode("append")
    .saveAsTable("workspace.fhrs.multiplex_raw")
)
