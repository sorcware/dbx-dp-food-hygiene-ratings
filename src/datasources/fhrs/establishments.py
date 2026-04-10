from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import LongType, StringType, StructField, StructType

from ._http import AuthorityPartition, make_session

_HEADERS = {"Accept": "application/json", "x-api-version": "2"}


class EstablishmentsReader(DataSourceReader):

    def __init__(self, schema):
        self.schema = schema

    def partitions(self):
        session = make_session()
        response = session.get(
            "https://api.ratings.food.gov.uk/Authorities",
            headers=_HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        authorities = response.json().get("authorities", [])
        return [
            AuthorityPartition(
                authority_id=a.get("LocalAuthorityId"),
                authority_name=a.get("Name", ""),
                file_url=a["FileName"],
                last_published=a.get("LastPublishedDate") or "",
            )
            for a in authorities
            if a.get("FileName")
        ]

    def read(self, partition):
        session = make_session()
        response = session.get(partition.file_url, timeout=120)
        response.raise_for_status()
        yield (
            partition.authority_id,
            partition.authority_name,
            partition.file_url,
            partition.last_published,
            response.text,
        )


class EstablishmentsDataSource(DataSource):

    @classmethod
    def name(cls):
        return "fhrs_establishments"

    def __init__(self, options):
        self.options = options

    def schema(self):
        return StructType([
            StructField("authority_id", LongType()),
            StructField("authority_name", StringType()),
            StructField("file_url", StringType()),
            StructField("last_published", StringType()),
            StructField("raw", StringType()),
        ])

    def reader(self, schema):
        return EstablishmentsReader(schema)
