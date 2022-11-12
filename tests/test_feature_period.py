import pytest

from jstark.feature import FeaturePeriod, PeriodUnitOfMeasure
from jstark.exceptions import FeaturePeriodEndGreaterThanStartError

def test_feature_period_description():
    assert (
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 3, 2).description
        == "Between 3 and 2 days ago"
    )

def test_feature_period_end_greater_than_start_raises_exception():
    with pytest.raises(FeaturePeriodEndGreaterThanStartError) as excInfo:
        FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 3)
    assert str(excInfo.value) == "End of the feature period (3) cannot be before the start of the feature period (2)"