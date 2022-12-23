"""EarliestPurchaseDate feature"""
import pyspark.sql.functions as f
from pyspark.sql import Column

from .min_feature import Min


class EarliestPurchaseDate(Min):
    def column_expression(self) -> Column:
        return f.to_date(f.col("Timestamp"))

    @property
    def description_subject(self) -> str:
        return "Earliest purchase date"

    def default_value(self) -> Column:
        return f.lit(None)

    @property
    def commentary(self) -> str:
        return (
            "Basically, when was a purchase first made. This could "
            + "be used to determine a whole myriad of useful information"
            + " such as when a customer first shopped, or"
            + "when a customer first bought a particular product. To make "
            + "use of this you would likely need to define "
            + "a large feature period, there isn't much use"
            + "in knowing when someone's earliest purchase was "
            + "in the previous two weeks."
        )
