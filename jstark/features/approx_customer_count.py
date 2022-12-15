import pyspark.sql.functions as f
from pyspark.sql import Column

from .approxdistinctcount_feature import ApproxDistinctCount


class ApproxCustomerCount(ApproxDistinctCount):
    def column_expression(self) -> Column:
        return f.col("Customer")

    @property
    def description_subject(self) -> str:
        return "Approximate distinct count of Customers"

    @property
    def commentary(self) -> str:
        return (
            "The approximate number of customers. Similar to "
            + f"CustomerCount_{self.feature_period.mnemonic} "
            + "except that it uses an approximation algorithm which "
            + "will not be as accurate as "
            + f"CustomerCount_{self.feature_period.mnemonic} but will be a lot "
            + 'quicker to compute and in many cases will be "close enough".'
        )
