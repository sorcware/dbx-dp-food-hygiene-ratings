from ._datasource import FhrsDataSource


class SortOptionsDataSource(FhrsDataSource):
    source_name = "fhrs_sort_options"
    url = "https://api.ratings.food.gov.uk/SortOptions"
