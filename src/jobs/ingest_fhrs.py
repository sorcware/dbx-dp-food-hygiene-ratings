from pyspark.sql import functions as F
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import LongType, StringType, StructField, StructType
from databricks.sdk.runtime import spark
from dataclasses import dataclass


# ScoreDescriptors are excluded — they are per-establishment lookups, not
# a batch reference dataset.


@dataclass
class SinglePartition(InputPartition):
    pass


@dataclass
class AuthorityPartition(InputPartition):
    authority_id: int
    authority_name: str
    file_url: str
    last_published: str


# ---------------------------------------------------------------------------
# Establishments (XML bulk download)
# ---------------------------------------------------------------------------

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


class EstablishmentsReader(DataSourceReader):

    def __init__(self, schema):
        self.schema = schema

    def partitions(self):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
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
                authority_id=a.get("LocalAuthorityId"),
                authority_name=a.get("Name", ""),
                file_url=a["FileName"],
                last_published=a.get("LastPublishedDate") or "",
            )
            for a in authorities
            if a.get("FileName")
        ]

    def read(self, partition):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(partition.file_url, timeout=120)
        response.raise_for_status()

        yield (
            partition.authority_id,
            partition.authority_name,
            partition.file_url,
            partition.last_published,
            response.text,
        )


# ---------------------------------------------------------------------------
# Countries
# ---------------------------------------------------------------------------

class CountriesDataSource(DataSource):

    @classmethod
    def name(cls):
        return "fhrs_countries"

    def __init__(self, options):
        self.options = options

    def schema(self):
        return StructType([
            StructField("raw", StringType()),
        ])

    def reader(self, schema):
        return CountriesReader(schema)


class CountriesReader(DataSourceReader):

    def __init__(self, schema):
        self.schema = schema

    def partitions(self):
        return [SinglePartition()]

    def read(self, partition):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(
            "https://api.ratings.food.gov.uk/Countries",
            headers={"Accept": "application/json", "x-api-version": "2"},
            timeout=30,
        )
        response.raise_for_status()

        yield (response.text,)


# ---------------------------------------------------------------------------
# Regions
# ---------------------------------------------------------------------------

class RegionsDataSource(DataSource):

    @classmethod
    def name(cls):
        return "fhrs_regions"

    def __init__(self, options):
        self.options = options

    def schema(self):
        return StructType([
            StructField("raw", StringType()),
        ])

    def reader(self, schema):
        return RegionsReader(schema)


class RegionsReader(DataSourceReader):

    def __init__(self, schema):
        self.schema = schema

    def partitions(self):
        return [SinglePartition()]

    def read(self, partition):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(
            "https://api.ratings.food.gov.uk/Regions",
            headers={"Accept": "application/json", "x-api-version": "2"},
            timeout=30,
        )
        response.raise_for_status()

        yield (response.text,)


# ---------------------------------------------------------------------------
# Authorities
# ---------------------------------------------------------------------------

class AuthoritiesDataSource(DataSource):

    @classmethod
    def name(cls):
        return "fhrs_authorities"

    def __init__(self, options):
        self.options = options

    def schema(self):
        return StructType([
            StructField("raw", StringType()),
        ])

    def reader(self, schema):
        return AuthoritiesReader(schema)


class AuthoritiesReader(DataSourceReader):

    def __init__(self, schema):
        self.schema = schema

    def partitions(self):
        return [SinglePartition()]

    def read(self, partition):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(
            "https://api.ratings.food.gov.uk/Authorities",
            headers={"Accept": "application/json", "x-api-version": "2"},
            timeout=30,
        )
        response.raise_for_status()

        yield (response.text,)


# ---------------------------------------------------------------------------
# BusinessTypes
# ---------------------------------------------------------------------------

class BusinessTypesDataSource(DataSource):

    @classmethod
    def name(cls):
        return "fhrs_business_types"

    def __init__(self, options):
        self.options = options

    def schema(self):
        return StructType([
            StructField("raw", StringType()),
        ])

    def reader(self, schema):
        return BusinessTypesReader(schema)


class BusinessTypesReader(DataSourceReader):

    def __init__(self, schema):
        self.schema = schema

    def partitions(self):
        return [SinglePartition()]

    def read(self, partition):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(
            "https://api.ratings.food.gov.uk/BusinessTypes",
            headers={"Accept": "application/json", "x-api-version": "2"},
            timeout=30,
        )
        response.raise_for_status()

        yield (response.text,)


# ---------------------------------------------------------------------------
# Ratings
# ---------------------------------------------------------------------------

class RatingsDataSource(DataSource):

    @classmethod
    def name(cls):
        return "fhrs_ratings"

    def __init__(self, options):
        self.options = options

    def schema(self):
        return StructType([
            StructField("raw", StringType()),
        ])

    def reader(self, schema):
        return RatingsReader(schema)


class RatingsReader(DataSourceReader):

    def __init__(self, schema):
        self.schema = schema

    def partitions(self):
        return [SinglePartition()]

    def read(self, partition):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(
            "https://api.ratings.food.gov.uk/Ratings",
            headers={"Accept": "application/json", "x-api-version": "2"},
            timeout=30,
        )
        response.raise_for_status()

        yield (response.text,)


# ---------------------------------------------------------------------------
# RatingOperators
# ---------------------------------------------------------------------------

class RatingOperatorsDataSource(DataSource):

    @classmethod
    def name(cls):
        return "fhrs_rating_operators"

    def __init__(self, options):
        self.options = options

    def schema(self):
        return StructType([
            StructField("raw", StringType()),
        ])

    def reader(self, schema):
        return RatingOperatorsReader(schema)


class RatingOperatorsReader(DataSourceReader):

    def __init__(self, schema):
        self.schema = schema

    def partitions(self):
        return [SinglePartition()]

    def read(self, partition):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(
            "https://api.ratings.food.gov.uk/RatingOperators",
            headers={"Accept": "application/json", "x-api-version": "2"},
            timeout=30,
        )
        response.raise_for_status()

        yield (response.text,)


# ---------------------------------------------------------------------------
# SortOptions
# ---------------------------------------------------------------------------

class SortOptionsDataSource(DataSource):

    @classmethod
    def name(cls):
        return "fhrs_sort_options"

    def __init__(self, options):
        self.options = options

    def schema(self):
        return StructType([
            StructField("raw", StringType()),
        ])

    def reader(self, schema):
        return SortOptionsReader(schema)


class SortOptionsReader(DataSourceReader):

    def __init__(self, schema):
        self.schema = schema

    def partitions(self):
        return [SinglePartition()]

    def read(self, partition):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(
            "https://api.ratings.food.gov.uk/SortOptions",
            headers={"Accept": "application/json", "x-api-version": "2"},
            timeout=30,
        )
        response.raise_for_status()

        yield (response.text,)


# ---------------------------------------------------------------------------
# SchemeTypes
# ---------------------------------------------------------------------------

class SchemeTypesDataSource(DataSource):

    @classmethod
    def name(cls):
        return "fhrs_scheme_types"

    def __init__(self, options):
        self.options = options

    def schema(self):
        return StructType([
            StructField("raw", StringType()),
        ])

    def reader(self, schema):
        return SchemeTypesReader(schema)


class SchemeTypesReader(DataSourceReader):

    def __init__(self, schema):
        self.schema = schema

    def partitions(self):
        return [SinglePartition()]

    def read(self, partition):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(
            "https://api.ratings.food.gov.uk/SchemeTypes",
            headers={"Accept": "application/json", "x-api-version": "2"},
            timeout=30,
        )
        response.raise_for_status()

        yield (response.text,)


# ---------------------------------------------------------------------------
# Register all data sources
# ---------------------------------------------------------------------------

for _ds in [
    EstablishmentsDataSource,
    CountriesDataSource,
    RegionsDataSource,
    AuthoritiesDataSource,
    BusinessTypesDataSource,
    RatingsDataSource,
    RatingOperatorsDataSource,
    SortOptionsDataSource,
    SchemeTypesDataSource,
]:
    spark.dataSource.register(_ds)


# ---------------------------------------------------------------------------
# Ingest — append each reference dataset with ingest_timestamp
# ---------------------------------------------------------------------------

_SOURCES = [
    ("fhrs_establishments",   "workspace.fhrs.establishments"),
    ("fhrs_countries",        "workspace.fhrs.countries"),
    ("fhrs_regions",          "workspace.fhrs.regions"),
    ("fhrs_authorities",      "workspace.fhrs.authorities"),
    ("fhrs_business_types",   "workspace.fhrs.business_types"),
    ("fhrs_ratings",          "workspace.fhrs.ratings"),
    ("fhrs_rating_operators", "workspace.fhrs.rating_operators"),
    ("fhrs_sort_options",     "workspace.fhrs.sort_options"),
    ("fhrs_scheme_types",     "workspace.fhrs.scheme_types"),
]

for _format, _table in _SOURCES:
    df = spark.read.format(_format).load()
    if _format == "fhrs_establishments":
        df = df.select(
            F.current_timestamp().alias("ingest_timestamp"),
            F.col("authority_id"),
            F.col("authority_name"),
            F.col("file_url"),
            F.col("last_published").cast("timestamp").alias("last_published"),
            F.col("raw"),
        )
    else:
        df = df.select(
            F.col("raw"),
            F.current_timestamp().alias("ingest_timestamp"),
        )
    (
        df
        .write
        .mode("append")
        .saveAsTable(_table)
    )
