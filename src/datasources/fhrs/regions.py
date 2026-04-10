from ._datasource import FhrsDataSource


class RegionsDataSource(FhrsDataSource):
    source_name = "fhrs_regions"
    url = "https://api.ratings.food.gov.uk/Regions"
