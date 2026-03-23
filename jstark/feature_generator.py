"""Base class for all feature generators"""

from abc import ABCMeta
import re
from datetime import date
from typing import Self

from pyspark.sql import Column, SparkSession

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.exceptions import FeaturePeriodMnemonicIsInvalid
from jstark.features.feature import Feature

FeaturePeriodsType = (
    list[FeaturePeriod]
    | list[str]
    | list[FeaturePeriod | str]
    | set[FeaturePeriod]
    | set[str]
    | None
)


class FeatureGenerator(metaclass=ABCMeta):
    """Base class for all feature generators"""

    def __init__(
        self,
        as_at: date | None = None,
        feature_periods: FeaturePeriodsType = None,
        feature_stems: set[str] | list[str] | None = None,
        first_day_of_week: str | None = None,
        use_absolute_periods: bool = False,
    ) -> None:
        if as_at is None:
            as_at = date.today()
        if feature_stems is None:
            feature_stems = set[str]()
        if isinstance(feature_stems, list):
            feature_stems = set[str](feature_stems)
        self.as_at = as_at
        self.feature_periods = feature_periods
        self.feature_stems = feature_stems
        self.first_day_of_week = first_day_of_week
        self.use_absolute_periods = use_absolute_periods

    FEATURE_CLASSES: set[type["Feature"]] = set[type["Feature"]]()

    @property
    def as_at(self) -> date:
        return self._as_at

    @as_at.setter
    def as_at(self, value: date) -> None:
        self._as_at = value

    @property
    def feature_periods(self) -> list[FeaturePeriod]:
        return self.__feature_periods

    @feature_periods.setter
    def feature_periods(self, value: FeaturePeriodsType) -> None:
        if value is None:
            value = {FeaturePeriod(PeriodUnitOfMeasure.WEEK, 52, 0)}
        period_unit_of_measure_values = "".join([e.value for e in PeriodUnitOfMeasure])
        regex = (
            # https://regex101.com/r/Xvf3ey/1
            r"^(\d*)([" + period_unit_of_measure_values + r"])(\d*)$"
        )
        _feature_periods = []
        for fp in value:
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
        self.__feature_periods = _feature_periods

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
                f[0](
                    as_at=self.as_at,
                    feature_period=f[1],
                    first_day_of_week=self.first_day_of_week,
                    use_absolute_periods=self.use_absolute_periods,
                )
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

    @property
    def flattened_references(self) -> set[str]:
        return {item for sublist in self.references.values() for item in sublist}

    @staticmethod
    def parse_references(references: str) -> list[str]:
        return sorted(
            set(re.findall(r"UnresolvedAttribute\(List\(([^)]+)\)", references))
        )

    def with_feature_periods(self, feature_periods: FeaturePeriodsType) -> Self:
        self.feature_periods = feature_periods
        return self

    def with_feature_period(self, feature_period: FeaturePeriod | str) -> Self:
        self.feature_periods = [*self.feature_periods, feature_period]
        return self

    def with_feature_stems(self, feature_stems: list[str]) -> Self:
        self.feature_stems = set[str](feature_stems)
        return self

    def with_feature_stem(self, feature_stem: str) -> Self:
        self.feature_stems.add(feature_stem)
        return self

    def with_first_day_of_week(self, first_day_of_week: str) -> Self:
        self.first_day_of_week = first_day_of_week
        return self

    def with_as_at(self, as_at: date) -> Self:
        self.as_at = as_at
        return self

    def with_use_absolute_periods(self, use_absolute_periods: bool) -> Self:
        self.use_absolute_periods = use_absolute_periods
        return self

    def without_feature_period(self, feature_period: FeaturePeriod | str) -> Self:
        if isinstance(feature_period, str):
            # Reuse setter to parse the mnemonic, then extract the result
            original = self.feature_periods
            self.feature_periods = [feature_period]
            feature_period = self.feature_periods[0]
            self.feature_periods = original
        self.feature_periods = [
            fp for fp in self.feature_periods if fp != feature_period
        ]
        return self

    def without_feature_stem(self, feature_stem: str) -> Self:
        self.feature_stems.discard(feature_stem)
        return self

    def __repr__(self) -> str:
        periods = sorted([fp.mnemonic for fp in self.feature_periods])
        stems = sorted(self.feature_stems) if self.feature_stems is not None else None
        return (
            f"{self.__class__.__name__}"
            f"(as_at={self.as_at}"
            f", feature_periods={periods}"
            f", feature_stems={stems}"
            f", first_day_of_week={self.first_day_of_week})"
        )
