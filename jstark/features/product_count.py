"""ProductCount feature"""
import pyspark.sql.functions as f
from pyspark.sql import Column

from .distinctcount_feature import DistinctCount


class ProductCount(DistinctCount):
    def column_expression(self) -> Column:
        return f.col("Product")

    @property
    def description_subject(self) -> str:
        return "Distinct count of Products"

    @property
    def commentary(self) -> str:
        return (
            "The number of products. Typically the dataframe supplied "
            + "to this feature will have many transactions for many baskets with "
            + "many products bought, this feature allows you to determine how many "
            + "disinct products were bought in those baskets."
        )
