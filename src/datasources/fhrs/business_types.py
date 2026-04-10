from ._datasource import FhrsDataSource


class BusinessTypesDataSource(FhrsDataSource):
    source_name = "fhrs_business_types"
    url = "https://api.ratings.food.gov.uk/BusinessTypes"
