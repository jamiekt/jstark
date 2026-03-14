"""OrderPeriods feature"""

import functools
import operator

from jstark.features.feature import DerivedFeature

from pyspark.sql import Column
import pyspark.sql.functions as f

from .order_count import OrderCount
from jstark.feature_period import FeaturePeriod


class OrderPeriods(DerivedFeature):
    """OrderPeriods feature"""

    def column_expression(self) -> Column:
        exprs = []
        for period in range(self.feature_period.end, self.feature_period.start + 1):
            exprs.append(
                f.when(
                    OrderCount(
                        as_at=self.as_at,
                        feature_period=FeaturePeriod(
                            self.feature_period.period_unit_of_measure, period, period
                        ),
                        first_day_of_week=self._first_day_of_week,
                    ).column
                    > 0,
                    1,
                ).otherwise(0)
            )
        return functools.reduce(operator.add, exprs)

    def default_value(self) -> Column:
        return f.lit(None)

    @property
    def description_subject(self) -> str:
        return (
            f"Number of {self.feature_period.period_unit_of_measure.name.lower()}s"
            + " in which at least one order was placed"
        )

    @property
    def commentary(self) -> str:
        return (
            f"The number of {self.feature_period.period_unit_of_measure.name.lower()}s "
            + "in which at least one basket was purchased. The value will be in the "
            + f"range 0 to {self.feature_period.start - self.feature_period.end + 1} "
            + f"because {self.feature_period.start - self.feature_period.end + 1} is "
            + f"the number of {self.feature_period.period_unit_of_measure.name.lower()}"
            + f"s between {self.start_date.strftime('%Y-%m-%d')} and"
            + f" {self.end_date.strftime('%Y-%m-%d')}. When grouped by Customer and"
            + " Product this feature is a useful indicator of the frequency of"
            + " which a Customer purchases a Product."
        )

    @property
    def feature_name(self) -> str:
        return (
            f"Order{self.feature_period.period_unit_of_measure.name.title()}s"
            + f"_{self.feature_period.mnemonic}"
        )
