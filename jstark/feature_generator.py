"""Base class for all feature generators"""

from abc import ABCMeta
import re
from datetime import date

from pyspark.sql import Column, SparkSession

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.exceptions import FeaturePeriodMnemonicIsInvalid
from jstark.features.feature import Feature


class FeatureGenerator(metaclass=ABCMeta):
    """Base class for all feature generators"""

    def __init__(
        self,
        as_at: date,
        feature_periods: list[FeaturePeriod] | list[str] | None = None,
        feature_stems: set[str] | list[str] | None = None,
    ) -> None:
        if feature_periods is None:
            feature_periods = [FeaturePeriod(PeriodUnitOfMeasure.WEEK, 52, 0)]
        if feature_stems is None:
            feature_stems = set[str]()
        if isinstance(feature_stems, list):
            feature_stems = set[str](feature_stems)
        self.as_at = as_at
        period_unit_of_measure_values = "".join([e.value for e in PeriodUnitOfMeasure])
        regex = (
            # https://regex101.com/r/Xvf3ey/1
            r"^(\d*)([" + period_unit_of_measure_values + r"])(\d*)$"
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
        self.feature_stems = feature_stems

    FEATURE_CLASSES: set[type["Feature"]] = set[type["Feature"]]()

    @property
    def as_at(self) -> date:
        return self.__as_at

    @as_at.setter
    def as_at(self, value: date) -> None:
        self.__as_at = value

    @property
    def feature_periods(self) -> list[FeaturePeriod]:
        return self.__feature_periods

    @feature_periods.setter
    def feature_periods(self, value: list[FeaturePeriod]) -> None:
        self.__feature_periods = value

    @property
    def features(self) -> list[Column]:
        # Find feature stems that do not correspond to any class in FEATURE_CLASSES.
        # If any are not found, raise an Exception.
        missing_stems = self.feature_stems - {
            cls.__name__ for cls in self.FEATURE_CLASSES
        }
        if missing_stems:
            # Only raise on the first (sorted for determinism)
            raise Exception(f"Feature(s) {sorted(missing_stems)} not found")
        desired_features = (
            [fc for fc in self.FEATURE_CLASSES if fc.__name__ in self.feature_stems]
            if self.feature_stems
            else self.FEATURE_CLASSES
        )
        return [
            feature.column
            for feature in [
                f[0](as_at=self.as_at, feature_period=f[1])
                for f in (
                    (cls, fp) for cls in desired_features for fp in self.feature_periods
                )
            ]
        ]

    @property
    def references(self) -> dict[str, list[str]]:
        # this function requires a SparkSession in order to do its thing.
        # In normal operation a SparkSession will probably already exist
        # but in unit tests that might not be the case, so getOrCreate one
        SparkSession.builder.getOrCreate()
        return {
            # pylint: disable=protected-access
            node.name().head(): self.parse_references(node.toString())
            for node in [c._jc.node() for c in self.features]  # type: ignore
        }

    @staticmethod
    def parse_references(references: str) -> list[str]:
        return sorted(
            set(re.findall(r"UnresolvedAttribute\(List\(([^)]+)\)", references))
        )
