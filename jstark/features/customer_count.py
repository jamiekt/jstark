import pyspark.sql.functions as f
from pyspark.sql import Column

from .distinctcount_feature import DistinctCount


class CustomerCount(DistinctCount):
    def column_expression(self) -> Column:
        return f.col("Customer")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Customers"

    @property
    def commentary(self) -> str:
        return (
            "The number of customers. Typically the dataframe supplied "
            + "to this feature will have many transactions for many baskets each bought"
            + " a different customer, this feature allows you to determine how many "
            + "disinct customerss bought those baskets."
        )
