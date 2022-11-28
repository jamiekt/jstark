from abc import ABC, abstractmethod
from datetime import date
from typing import Callable

from pyspark.sql import Column
import pyspark.sql.functions as f

from jstark.feature_period import FeaturePeriod


class Feature(ABC):
    def __init__(self, as_at: date, feature_period: FeaturePeriod) -> None:
        self.__feature_period = feature_period
        self.__as_at = as_at

    def sum_aggregator(self, column: Column) -> Column:
        return f.sum(column)

    def count_aggregator(self, column: Column) -> Column:
        return f.count(column)

    @abstractmethod
    def aggregator(self) -> Callable[[Column], Column]:
        pass

    @property
    def feature_period(self) -> FeaturePeriod:
        return self.__feature_period

    @property
    def as_at(self) -> date:
        return self.__as_at

    @abstractmethod
    def columnExpression(self) -> Column:
        pass

    @property
    def feature_name(self) -> str:
        return f"{type(self).__name__}_{self.feature_period.code}"

    @property
    def column(self) -> Column:
        return self.aggregator()(self.columnExpression()).alias(self.feature_name)
