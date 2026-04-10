from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StringType, StructField, StructType

from ._http import SinglePartition, make_session

_HEADERS = {"Accept": "application/json", "x-api-version": "2"}


class FhrsReader(DataSourceReader):

    def __init__(self, url: str):
        self.url = url

    def partitions(self):
        return [SinglePartition()]

    def read(self, partition):
        session = make_session()
        response = session.get(self.url, headers=_HEADERS, timeout=30)
        response.raise_for_status()
        yield (response.text,)


class FhrsDataSource(DataSource):
    source_name: str
    url: str

    @classmethod
    def name(cls):
        return cls.source_name

    def __init__(self, options):
        self.options = options

    def schema(self):
        return StructType([StructField("raw", StringType())])

    def reader(self, schema):
        return FhrsReader(self.url)
