from ._datasource import FhrsDataSource


class AuthoritiesDataSource(FhrsDataSource):
    source_name = "fhrs_authorities"
    url = "https://api.ratings.food.gov.uk/Authorities"
