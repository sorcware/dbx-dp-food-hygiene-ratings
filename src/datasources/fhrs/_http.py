from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql.datasource import InputPartition


@dataclass
class SinglePartition(InputPartition):
    pass


@dataclass
class AuthorityPartition(InputPartition):
    authority_id: int
    authority_name: str
    file_url: str
    last_published: str


def make_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session
