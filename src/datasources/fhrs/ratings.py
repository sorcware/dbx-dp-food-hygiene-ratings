from ._datasource import FhrsDataSource


class RatingsDataSource(FhrsDataSource):
    source_name = "fhrs_ratings"
    url = "https://api.ratings.food.gov.uk/Ratings"
