"Average baskets per given periodunitofmeasure feature"

from .feature import DerivedFeature

from pyspark.sql import Column
import pyspark.sql.functions as f

from jstark.feature_period import FeaturePeriod
from .basket_count import BasketCount


class AverageBasket(DerivedFeature):
    "Average baskets per given periodunitofmeasure feature"

    def column_expression(self) -> Column:
        return (
            BasketCount(
                as_at=self.as_at,
                feature_period=FeaturePeriod(
                    self.feature_period.period_unit_of_measure,
                    self.feature_period.start,
                    self.feature_period.end,
                ),
            ).column
            / self.feature_period.number_of_periods
        )

    def default_value(self) -> Column:
        return f.lit(None)

    @property
    def description_subject(self) -> str:
        return (
            "Average number of baskets per "
            + f"{self.feature_period.period_unit_of_measure.name.lower()}"
        )

    @property
    def feature_name(self) -> str:
        return (
            "AverageBasketsPer"
            + f"{self.feature_period.period_unit_of_measure.name.title()}"
            + f"_{self.feature_period.mnemonic}"
        )
