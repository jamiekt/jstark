from datetime import date
from jstark import feature_generator
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure


def test_with_feature_periods():
    fg = feature_generator.FeatureGenerator().with_feature_periods(["3m1", "6m4"])
    assert fg.feature_periods == [
        FeaturePeriod(PeriodUnitOfMeasure.MONTH, 3, 1),
        FeaturePeriod(PeriodUnitOfMeasure.MONTH, 6, 4),
    ]


def test_with_feature_period():
    fg1 = feature_generator.FeatureGenerator().with_feature_period("10w10")
    fg2 = feature_generator.FeatureGenerator(feature_periods=["10w10"])
    expected = FeaturePeriod(PeriodUnitOfMeasure.WEEK, 10, 10)
    assert expected in fg1.feature_periods
    assert expected in fg2.feature_periods


def test_without_feature_period():
    fg = feature_generator.FeatureGenerator().with_feature_periods(["3m1", "6m4"])
    fg = fg.without_feature_period(FeaturePeriod(PeriodUnitOfMeasure.MONTH, 3, 1))
    assert FeaturePeriod(PeriodUnitOfMeasure.MONTH, 3, 1) not in fg.feature_periods


def test_without_feature_period_mnemonic():
    fg = feature_generator.FeatureGenerator().with_feature_periods(["3m1", "6m4"])
    fg = fg.without_feature_period("3m1")
    assert FeaturePeriod(PeriodUnitOfMeasure.MONTH, 3, 1) not in fg.feature_periods


def test_with_as_at():
    fg = feature_generator.FeatureGenerator()
    assert fg.as_at == date.today()
    fg = fg.with_as_at(date(2026, 3, 21))
    assert fg.as_at == date(2026, 3, 21)


def test_with_first_day_of_week():
    fg = feature_generator.FeatureGenerator()
    assert fg.first_day_of_week is None
    fg = fg.with_first_day_of_week("Monday")
    assert fg.first_day_of_week == "Monday"


def test_with_use_absolute_periods():
    fg = feature_generator.FeatureGenerator()
    assert fg.use_absolute_periods is False
    fg = fg.with_use_absolute_periods(True)
    assert fg.use_absolute_periods is True


def test_with_feature_stems():
    fg = feature_generator.FeatureGenerator()
    assert fg.feature_stems == set[str]()
    fg = fg.with_feature_stems(
        ["AverageGrossSpendPerBasket", "AverageNetSpendPerBasket"]
    )
    assert fg.feature_stems == {
        "AverageGrossSpendPerBasket",
        "AverageNetSpendPerBasket",
    }


def test_with_feature_stem():
    fg = feature_generator.FeatureGenerator()
    assert fg.feature_stems == set[str]()
    fg = fg.with_feature_stem("AverageGrossSpendPerBasket")
    assert fg.feature_stems == {"AverageGrossSpendPerBasket"}


def test_without_feature_stem():
    fg = feature_generator.FeatureGenerator().with_feature_stems(
        ["AverageGrossSpendPerBasket", "AverageNetSpendPerBasket"]
    )
    fg = fg.without_feature_stem("AverageGrossSpendPerBasket")
    assert fg.feature_stems == {"AverageNetSpendPerBasket"}
