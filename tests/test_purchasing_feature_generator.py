from datetime import date, datetime, timedelta
import pytest
from pyspark.sql import DataFrame, Row
import pyspark.sql.functions as f

from jstark.purchasing_feature_generator import PurchasingFeatureGenerator
from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.exceptions import FeaturePeriodMnemonicIsInvalid


def test_feature_period_mnemonic():
    pfg1 = PurchasingFeatureGenerator(
        date.today(), [FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0)]
    )
    pfg2 = PurchasingFeatureGenerator(date.today(), ["0d0"])
    assert (
        pfg1.feature_periods[0].period_unit_of_measure.value
        == pfg2.feature_periods[0].period_unit_of_measure.value
    )
    assert pfg1.feature_periods[0].start == pfg2.feature_periods[0].start
    assert pfg1.feature_periods[0].end == pfg2.feature_periods[0].end


def test_feature_period_invalid_mnemonic():
    with pytest.raises(FeaturePeriodMnemonicIsInvalid) as exc_info:
        PurchasingFeatureGenerator(date.today(), ["qwerty"])
    assert str(exc_info.value) == (
        "FeaturePeriod mnemonic must be an integer followed by a letter "
        + "from ['d', 'w', 'm', 'q', 'y'] followed by an integer"
    )


def test_feature_period_invalid_mnemonic_empty_string():
    with pytest.raises(FeaturePeriodMnemonicIsInvalid) as exc_info:
        PurchasingFeatureGenerator(date.today(), [""])
    assert str(exc_info.value) == (
        "FeaturePeriod mnemonic must be an integer followed by a letter "
        + "from ['d', 'w', 'm', 'q', 'y'] followed by an integer"
    )


def test_feature_period_invalid_mnemonic_unit_of_measure():
    with pytest.raises(FeaturePeriodMnemonicIsInvalid) as exc_info:
        PurchasingFeatureGenerator(date.today(), ["0z0"])
    assert str(exc_info.value) == (
        "FeaturePeriod mnemonic must be an integer followed by a letter "
        + "from ['d', 'w', 'm', 'q', 'y'] followed by an integer"
    )


def test_parse_references_grossspend():
    """
    PurchasingFeatureGenerator.parse_references() is only an internal helper function
    but still a good idea to have a couple of little tests here to verify and
    document its behaviour
    """
    assert PurchasingFeatureGenerator.parse_references(
        "List('Timestamp, 'GrossSpend)"
    ) == ["GrossSpend", "Timestamp"]


def test_parse_references_count():
    """Basic test that static method parse_references works"""
    assert PurchasingFeatureGenerator.parse_references("List('Timestamp)") == [
        "Timestamp"
    ]


def test_references_count(purchasing_feature_generator: PurchasingFeatureGenerator):
    """Verify that Count_0d0 requires field Timestamp"""
    assert purchasing_feature_generator.references["Count_0d0"] == ["Timestamp"]


def test_references_grossspend(
    purchasing_feature_generator: PurchasingFeatureGenerator,
):
    """Verify that Count_0d0 requires fields Timestamp,GrossSpend"""
    assert purchasing_feature_generator.references["GrossSpend_0d0"] == [
        "GrossSpend",
        "Timestamp",
    ]


def test_count_today(luke_and_leia_purchases_first: Row):
    """Test Count_0d0"""
    assert float(luke_and_leia_purchases_first["Count_0d0"]) == 2


def test_gross_spend_today(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    """Test GrossSpend_0d0"""

    def _validate_expected_value(df, expected):
        result = df.first()
        assert result is not None
        assert float(result["GrossSpend_0d0"]) == expected
        return result

    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0),
            ],
        ).features
    )
    _validate_expected_value(df, 5.5)
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date() - timedelta(days=1),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 0, 0),
            ],
        ).features
    )
    _validate_expected_value(df, 4.0)


def test_gross_spend_today_and_yesterday(
    as_at_timestamp: datetime, luke_and_leia_purchases: DataFrame
):
    """Test GrossSpend_1d0"""

    def _validate_expected_value(df, expected):
        result = df.first()
        assert result is not None
        assert float(result["GrossSpend_1d0"]) == expected
        return result

    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date(),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 1, 0),
            ],
        ).features
    )
    _validate_expected_value(df, 9.5)
    df = luke_and_leia_purchases.groupBy().agg(
        *PurchasingFeatureGenerator(
            as_at=as_at_timestamp.date() - timedelta(days=1),
            feature_periods=[
                FeaturePeriod(PeriodUnitOfMeasure.DAY, 1, 0),
            ],
        ).features
    )
    _validate_expected_value(df, 4.0)


def test_gross_spend_today_yesterday_and_day_before_yesterday(
    luke_and_leia_purchases_first: Row,
):
    """This test is being used to verify the logic that filters the
    input dataframe according to each the respective feature period
    """
    assert float(luke_and_leia_purchases_first["GrossSpend_2d0"]) == 9.5


def test_gross_spend_day_before_yesterday(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_2d2"""
    assert float(luke_and_leia_purchases_first["GrossSpend_2d2"]) == 0


def test_gross_spend_this_week(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_0w0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_0w0"]) == 9.5


def test_gross_spend_last_week(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1w1"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1w1"]) == 3.25


def test_gross_spend_this_week_and_last_week(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1w0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1w0"]) == 12.75


def test_gross_spend_this_month(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_0m0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_0m0"]) == 12.75


def test_gross_spend_last_month(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1m1"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1m1"]) == 6.0


def test_gross_spend_this_month_and_last_month(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1m0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1m0"]) == 18.75


def test_gross_spend_this_quarter(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_0q0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_0q0"]) == 18.75


def test_gross_spend_last_quarter(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1q1"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1q1"]) == 7


def test_gross_spend_this_quarter_and_last_quarter(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1q0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1q0"]) == 25.75


def test_gross_spend_this_year(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_0y0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_0y0"]) == 25.75


def test_gross_spend_last_year(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1y1"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1y1"]) == 8


def test_gross_spend_this_year_and_last_year(luke_and_leia_purchases_first: Row):
    """Test GrossSpend_1y0"""
    assert float(luke_and_leia_purchases_first["GrossSpend_1y0"]) == 33.75


def test_grossspend_metadata_description(
    dataframe_of_purchases: DataFrame,
    purchasing_feature_generator: PurchasingFeatureGenerator,
):
    """Test GrossSpend_2d0 metadata"""
    df = dataframe_of_purchases.agg(*purchasing_feature_generator.features)
    assert [c.metadata["description"] for c in df.schema if c.name == "GrossSpend_2d0"][
        0
    ] == "Sum of GrossSpend between 2022-11-28 and 2022-11-30"


def test_recencydays_and_mostrecentpurchasedate(
    dataframe_of_purchases: DataFrame,
    purchasing_feature_generator: PurchasingFeatureGenerator,
):
    """Test RecencyDays_2y0 & MostRecentPurchaseDate_2y0"""
    dataframe_of_purchases = dataframe_of_purchases.where(
        f.col("Timestamp") < f.lit(date(2022, 1, 1))
    )
    df = dataframe_of_purchases.agg(*purchasing_feature_generator.features)
    first = df.select("RecencyDays_2y0", "MostRecentPurchaseDate_2y0").first()
    assert first is not None
    assert first["RecencyDays_2y0"] == 365
    assert first["MostRecentPurchaseDate_2y0"] == date(2021, 11, 30)


def test_basket_count_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test BasketCount_0y0"""
    assert float(luke_and_leia_purchases_first["BasketCount_0y0"]) == 5


def test_store_count_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test StoreCount_0y0"""
    assert float(luke_and_leia_purchases_first["StoreCount_0y0"]) == 3


def test_product_count_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test ProductCount_0y0"""
    assert float(luke_and_leia_purchases_first["ProductCount_0y0"]) == 6


def test_customer_count_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test CustomerCount_0y0"""
    assert float(luke_and_leia_purchases_first["CustomerCount_0y0"]) == 2


def test_channel_count_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test ChannelCount_0y0"""
    assert float(luke_and_leia_purchases_first["ChannelCount_0y0"]) == 2


def test_discount_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test Discount_0y0"""
    assert float(luke_and_leia_purchases_first["Discount_0y0"]) == 1.05


def test_net_spend_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test NetSpend_0y0"""
    assert float(luke_and_leia_purchases_first["NetSpend_0y0"]) == 24.25


def test_max_net_spend_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MaxNetSpend_0y0"""
    assert float(luke_and_leia_purchases_first["MaxNetSpend_0y0"]) == 6.75


def test_min_net_spend_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MinNetSpend_0y0"""
    assert float(luke_and_leia_purchases_first["MinNetSpend_0y0"]) == 2.25


def test_max_gross_spend_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MaxGrossSpend_0y0"""
    assert float(luke_and_leia_purchases_first["MaxGrossSpend_0y0"]) == 7.0


def test_min_gross_spend_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MinGrossSpend_0y0"""
    assert float(luke_and_leia_purchases_first["MinGrossSpend_0y0"]) == 2.5


def test_avg_gross_spend_per_basket_luke_and_leia_0y0(
    luke_and_leia_purchases_first: Row,
):
    """Test AverageGrossSpendPerBasket_0y0"""
    assert (
        float(luke_and_leia_purchases_first["AverageGrossSpendPerBasket_0y0"]) == 5.15
    )


def test_quantity_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test Quantity_0y0"""
    assert float(luke_and_leia_purchases_first["Quantity_0y0"]) == 15


def test_avg_quantity_per_basket_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test AvgQuantityPerBasket_0y0"""
    assert float(luke_and_leia_purchases_first["AvgQuantityPerBasket_0y0"]) == 3.0


def test_min_net_price_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MinNetPrice_0y0"""
    assert float(luke_and_leia_purchases_first["MinNetPrice_0y0"]) == 0.5


def test_max_net_price_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MaxNetPrice_0y0"""
    assert float(luke_and_leia_purchases_first["MaxNetPrice_0y0"]) == 3.75


def test_min_gross_price_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MinGrossPrice_0y0"""
    assert float(luke_and_leia_purchases_first["MinGrossPrice_0y0"]) == 0.5416666666667


def test_max_gross_price_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test MaxGrossPrice_0y0"""
    assert float(luke_and_leia_purchases_first["MaxGrossPrice_0y0"]) == 4.0


def test_earliest_purchase_date_luke_and_leia_0y0(luke_and_leia_purchases_first: Row):
    """Test EarliestPurchaseDate_2y0"""
    assert luke_and_leia_purchases_first["EarliestPurchaseDate_2y0"] == date(
        2021, 11, 30
    )


def test_basketweeks(
    as_at_timestamp: datetime, dataframe_of_faker_purchases: DataFrame
):
    """Test BasketWeeks

    BasketWeeks is the number of weeks in which at least one basket was purchased

    The result here is 5 because the input dataset includes data from 2021 whereas the
    specified as_at date is 2022-11-30, so there are only 5 weeks in the 52 weeks
    leading up to 2022-11-30 in which any baskets were purchased
    """
    pfg = PurchasingFeatureGenerator(as_at=as_at_timestamp, feature_periods=["52w0"])
    output_df = (
        dataframe_of_faker_purchases.groupBy()
        .agg(*pfg.features)
        .select("BasketWeeks_52w0")
    )
    first = output_df.first()
    assert first is not None
    assert first["BasketWeeks_52w0"] == 5


def test_basketweeks_by_product_and_customer(
    as_at_timestamp: datetime, dataframe_of_faker_purchases: DataFrame
):
    """Test BasketWeeks by product and customer

    Filtering on a specific Customer and Product whose activity
    we happen to know about.
    as_at set at the date immediately after the period for which sample transactions
    are being supplied.
    """
    pfg = PurchasingFeatureGenerator(as_at=date(2022, 1, 1), feature_periods=["52w0"])
    output_df = (
        dataframe_of_faker_purchases.where("Customer = 'John Williams'")
        .where("Product = 'Ice Cream'")
        .groupBy(["Product", "Customer"])
        .agg(*pfg.features)
        .select("BasketWeeks_52w0")
    )
    first = output_df.first()
    assert first is not None
    assert first["BasketWeeks_52w0"] == 6


def test_basketweeks_commentary(
    as_at_timestamp: datetime, dataframe_of_faker_purchases: DataFrame
):
    """Test BasketWeeks commentary"""
    pfg = PurchasingFeatureGenerator(as_at=as_at_timestamp, feature_periods=["52w1"])
    output_df = (
        dataframe_of_faker_purchases.groupBy()
        .agg(*pfg.features)
        .select("BasketWeeks_52w1")
    )
    assert [(c.metadata["commentary"]) for c in output_df.schema][0] == (
        "The number of weeks in which at least one basket was purchased. "
        + "The value will be in the range 0 to 52"
        + " because 52 is the number of weeks between 2021-11-28 and 2022-11-26."
        + " When grouped by Customer and Product"
        + " this feature is a useful indicator of the frequency of which a"
        + " Customer purchases a Product."
    )
