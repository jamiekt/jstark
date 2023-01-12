"""Base class for all feature generators
"""
from abc import ABCMeta
import re
from datetime import date
from typing import List, Union, Dict

from pyspark.sql import Column, SparkSession

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.exceptions import FeaturePeriodMnemonicIsInvalid


class FeatureGenerator(metaclass=ABCMeta):
    """Base class for all feature generators"""

    def __init__(
        self,
        as_at: date,
        feature_periods: Union[List[FeaturePeriod], List[str]] = [
            FeaturePeriod(PeriodUnitOfMeasure.WEEK, 52, 0),
        ],
    ) -> None:
        # sourcery skip: use-named-expression
        # walrus operator not supported until python3.8, we are still supporting 3.7
        self.as_at = as_at
        period_unit_of_measure_values = "".join([e.value for e in PeriodUnitOfMeasure])
        regex = (
            # https://regex101.com/r/Xvf3ey/1
            r"^(\d*)(["
            + period_unit_of_measure_values
            + r"])(\d*)$"
        )
        _feature_periods = []
        for fp in feature_periods:
            if isinstance(fp, FeaturePeriod):
                _feature_periods.append(fp)
            else:
                matches = re.match(regex, fp)
                if not matches:
                    raise FeaturePeriodMnemonicIsInvalid
                _feature_periods.append(
                    FeaturePeriod(
                        PeriodUnitOfMeasure(matches[2]),
                        int(matches[1]),
                        int(matches[3]),
                    )
                )
        self.feature_periods = _feature_periods

    # would prefer list[Type[Feature]] as type hint but
    # this only works on py3.10 and above
    FEATURE_CLASSES: list

    @property
    def as_at(self) -> date:
        return self.__as_at

    @as_at.setter
    def as_at(self, value: date) -> None:
        self.__as_at = value

    @property
    def feature_periods(self) -> List[FeaturePeriod]:
        return self.__feature_periods

    @feature_periods.setter
    def feature_periods(self, value: List[FeaturePeriod]) -> None:
        self.__feature_periods = value

    @property
    def features(self) -> List[Column]:
        return [
            feature.column
            for feature in [
                f[0](as_at=self.as_at, feature_period=f[1])
                for f in (
                    (cls, fp)
                    for cls in self.FEATURE_CLASSES
                    for fp in self.feature_periods
                )
            ]
        ]

    @property
    def references(self) -> Dict[str, List[str]]:
        # this function requires a SparkSession in order to do its thing.
        # In normal operation a SparkSession will probably already exist
        # but in unit tests that might not be the case, so getOrCreate one
        SparkSession.builder.getOrCreate()
        return {
            expr.name(): self.parse_references(expr.references().toList().toString())
            # pylint: disable=protected-access
            for expr in [c._jc.expr() for c in self.features]  # type: ignore
        }

    @staticmethod
    def parse_references(references: str) -> List[str]:
        return sorted(
            "".join(
                references.replace("'", "")
                .replace("List(", "")
                .replace(")", "")
                .replace(")", "")
                .split()
            ).split(",")
        )
