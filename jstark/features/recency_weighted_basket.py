"""RecencyWeightedBasket feature"""

from datetime import date

from .feature import DerivedFeature

from pyspark.sql import Column
import pyspark.sql.functions as f

from jstark.feature_period import FeaturePeriod
from .basket_count import BasketCount
from .approx_basket_count import ApproxBasketCount


class RecencyWeightedApproxBasket(DerivedFeature):
    """RecencyWeightedApproxBasket"""

    def __init__(
        self, as_at: date, feature_period: FeaturePeriod, smoothing_factor: float
    ) -> None:
        super().__init__(as_at, feature_period)
        self.__smoothing_factor = smoothing_factor

    @property
    def smoothing_factor(self) -> float:
        return self.__smoothing_factor

    def column_expression(self) -> Column:
        expr = f.lit(0.0)
        for period in range(self.feature_period.end, self.feature_period.start + 1):
            expr = expr + ApproxBasketCount(
                as_at=self.as_at,
                feature_period=FeaturePeriod(
                    self.feature_period.period_unit_of_measure, period, period
                ),
            ).column * pow(self.smoothing_factor, period)
        return expr

    def default_value(self) -> Column:
        return f.lit(None)

    @property
    def description_subject(self) -> str:
        return (
            "Exponentially weighted moving average, with smoothing factor of"
            + f" {self.smoothing_factor}, of the approximate number of baskets"
            + f" per {self.feature_period.period_unit_of_measure.name.lower()}"
        )

    @property
    def feature_name(self) -> str:
        return (
            "RecencyWeightedApproxBasket"
            + f"{self.feature_period.period_unit_of_measure.name.title()}s"
            + f"{int(self.smoothing_factor*100)}"
            + f"_{self.feature_period.mnemonic}"
        )

    @property
    def commentary(self) -> str:
        return (
            "Exponential smoothing "
            + "(https://en.wikipedia.org/wiki/Exponential_smoothing)"
            + " is an alternative to a simple moving average which"
            + " gives greater weighting to more recent observations, thus is an"
            + " exponentially weighted moving average. It uses a smoothing factor"
            + f" between 0 & 1 which for this feature is {self.smoothing_factor}."
            + " Here the approximate number of baskets per"
            + f" {self.feature_period.period_unit_of_measure.name.lower()} is smoothed."
            + " This feature is considered to be a highly effective predictor of future"
            + " purchases, if a customer has bought a product recently then there's a"
            + " relatively high probability they will buy it again."
            + f" This is less accurate than {self.feature_name.replace('Approx', '')}"
            + " though is less computationally expensive to calculate because it "
            + " does not calculate a distinct count for each"
            + f" {self.feature_period.period_unit_of_measure.name.lower()}."
        )


class RecencyWeightedBasket(RecencyWeightedApproxBasket):
    """RecencyWeightedBasket feature"""

    def __init__(
        self, as_at: date, feature_period: FeaturePeriod, smoothing_factor: float
    ) -> None:
        super().__init__(as_at, feature_period, smoothing_factor)

    def column_expression(self) -> Column:
        expr = f.lit(0.0)
        for period in range(self.feature_period.end, self.feature_period.start + 1):
            expr = expr + BasketCount(
                as_at=self.as_at,
                feature_period=FeaturePeriod(
                    self.feature_period.period_unit_of_measure, period, period
                ),
            ).column * pow(super().smoothing_factor, period)
        return expr

    @property
    def description_subject(self) -> str:
        """simply RecencyWeightedApproxBasketXX_periodmenmonic's description with the
        word approximate removed"""
        return super().description_subject.replace("approximate ", "")

    @property
    def commentary(self) -> str:
        return (
            "Exponential smoothing "
            + "(https://en.wikipedia.org/wiki/Exponential_smoothing)"
            + " is an alternative to a simple moving average which"
            + " gives greater weighting to more recent observations, thus is an"
            + " exponentially weighted moving average. It uses a smoothing factor"
            + f" between 0 & 1 which for this feature is {self.smoothing_factor}."
            + " Here the number of baskets per"
            + f" {self.feature_period.period_unit_of_measure.name.lower()} is smoothed."
            + " This feature is considered to be a highly effective predictor of future"
            + " purchases, if a customer has bought a product recently then there's a"
            + " relatively high probability they will buy it again."
            + " This is computationally expensive to calculate because it "
            + " requires a distinct count of baskets for each"
            + f" {self.feature_period.period_unit_of_measure.name.lower()}. Every"
            + " distinct count operation is expensive so the less that are performed,"
            + " the better (YMMV based on a number of factors, "
            + "mainly the volume of data"
            + " being processed). For this reason you should consider choosing a small"
            + f" number of {self.feature_period.period_unit_of_measure.name.lower()}s"
            + " for the feature period. This feature"
            + f" ({self.feature_name}) is for"
            + f" {self.feature_period.number_of_periods}"
            + f" {self.feature_period.period_unit_of_measure.name.lower()}"
            + f"{'s' if self.feature_period.number_of_periods > 1 else ''}. You might"
            + f" consider using {self.feature_name.replace('Basket', 'ApproxBasket')}"
            + " instead which is less accurate but computationally cheaper."
        )

    @property
    def feature_name(self) -> str:
        """simply RecencyWeightedApproxBasketXX_periodmenmonic's with the
        word approximate removed"""
        return super().feature_name.replace("Approx", "")


class RecencyWeightedBasket90(RecencyWeightedBasket):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period, 0.9)


class RecencyWeightedBasket95(RecencyWeightedBasket):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period, 0.95)


class RecencyWeightedBasket99(RecencyWeightedBasket):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period, 0.99)


class RecencyWeightedApproxBasket90(RecencyWeightedApproxBasket):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period, 0.9)


class RecencyWeightedApproxBasket95(RecencyWeightedApproxBasket):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period, 0.95)


class RecencyWeightedApproxBasket99(RecencyWeightedApproxBasket):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        super().__init__(as_at, feature_period, 0.99)
