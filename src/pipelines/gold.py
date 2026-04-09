from pyspark import pipelines as dp
from pyspark.sql import functions as F

_GOLD_SOURCES = [
    ("silver_establishments",   "gold_establishments"),
    ("silver_countries",        "gold_countries"),
    ("silver_regions",          "gold_regions"),
    ("silver_authorities",      "gold_authorities"),
    ("silver_business_types",   "gold_business_types"),
    ("silver_ratings",          "gold_ratings"),
    ("silver_rating_operators", "gold_rating_operators"),
    ("silver_sort_options",     "gold_sort_options"),
    ("silver_scheme_types",     "gold_scheme_types"),
]


def _make_gold_current(silver_table, gold_table):
    def _current():
        return (
            spark.read.table(silver_table)
            .filter(F.col("__END_AT").isNull())
        )

    dp.materialized_view(
        name=gold_table,
        table_properties={"quality": "gold"},
    )(_current)


for _silver, _gold in _GOLD_SOURCES:
    _make_gold_current(_silver, _gold)
