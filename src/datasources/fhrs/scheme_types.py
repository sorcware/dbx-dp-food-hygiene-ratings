from ._datasource import FhrsDataSource


class SchemeTypesDataSource(FhrsDataSource):
    source_name = "fhrs_scheme_types"
    url = "https://api.ratings.food.gov.uk/SchemeTypes"
