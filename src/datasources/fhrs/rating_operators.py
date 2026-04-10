from ._datasource import FhrsDataSource


class RatingOperatorsDataSource(FhrsDataSource):
    source_name = "fhrs_rating_operators"
    url = "https://api.ratings.food.gov.uk/RatingOperators"
