import pytest

from jstark.exceptions import AsAtIsNotADate
from jstark.features.gross_spend_feature import GrossSpend
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure


def test_errors_if_as_at_is_not_a_date():
    with pytest.raises(AsAtIsNotADate):
        GrossSpend(
            as_at="2000-01-01",  # type: ignore
            feature_period=FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 1),
        )
