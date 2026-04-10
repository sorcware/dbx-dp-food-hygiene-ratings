from ._datasource import FhrsDataSource


class CountriesDataSource(FhrsDataSource):
    source_name = "fhrs_countries"
    url = "https://api.ratings.food.gov.uk/Countries"
