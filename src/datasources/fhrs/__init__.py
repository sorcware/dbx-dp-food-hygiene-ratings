from .authorities import AuthoritiesDataSource
from .business_types import BusinessTypesDataSource
from .countries import CountriesDataSource
from .establishments import EstablishmentsDataSource
from .rating_operators import RatingOperatorsDataSource
from .ratings import RatingsDataSource
from .regions import RegionsDataSource
from .scheme_types import SchemeTypesDataSource
from .sort_options import SortOptionsDataSource

ALL_DATA_SOURCES = [
    EstablishmentsDataSource,
    AuthoritiesDataSource,
    BusinessTypesDataSource,
    CountriesDataSource,
    RatingOperatorsDataSource,
    RatingsDataSource,
    RegionsDataSource,
    SchemeTypesDataSource,
    SortOptionsDataSource,
]
