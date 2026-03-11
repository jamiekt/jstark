from datetime import datetime, date
import pytest
from pyspark.sql import DataFrame

from jstark.mealkit.mealkit_features import MealkitFeatures


def test_orderweeks(
    as_at_timestamp: datetime, dataframe_of_faker_mealkit_orders: DataFrame
):
    """Test OrderWeeks

    OrderWeeks is the number of weeks in which at least one order was placed

    """
    mf = MealkitFeatures(as_at=as_at_timestamp, feature_periods=["52w0"])
    output_df = (
        dataframe_of_faker_mealkit_orders.groupBy()
        .agg(*mf.features)
        .select("OrderCount_52w0")
    )
    first = output_df.first()
    assert first is not None
    assert first["OrderCount_52w0"] == 872


def test_as_at_timestamp(dataframe_of_faker_mealkit_orders: DataFrame):
    """
    We had a situaiton where as_at was being passed as a datetime (which we support)
    however an error was still occurring:
    > TypeError: can't compare datetime.datetime to datetime.date
    Following TDD this was written as a failing test which was then fixed,
    the fix is in the same commit as this test.
    """
    as_at_timestamp = datetime(2022, 11, 30, 10, 12, 13)
    mf = MealkitFeatures(
        as_at=as_at_timestamp, feature_periods=["1q1", "2q2", "3q3", "4q4"]
    )
    dataframe_of_faker_mealkit_orders.groupBy().agg(*mf.features)


def test_desired_features(dataframe_of_faker_mealkit_orders: DataFrame):
    mf = MealkitFeatures(
        as_at=date(2022, 11, 30),
        feature_periods=["1q1", "2q2", "3q3", "4q4"],
        feature_stems=["OrderCount"],
    )
    output_df = dataframe_of_faker_mealkit_orders.groupBy().agg(*mf.features)
    assert output_df.columns == [
        "OrderCount_1q1",
        "OrderCount_2q2",
        "OrderCount_3q3",
        "OrderCount_4q4",
    ]


def test_non_existent_desired_features(dataframe_of_faker_mealkit_orders: DataFrame):
    mf = MealkitFeatures(
        as_at=date(2022, 11, 30),
        feature_periods=["1q1", "2q2", "3q3", "4q4"],
        feature_stems=["NonExistentFeature", "NonExistentFeature2"],
    )
    with pytest.raises(Exception) as exc_info:
        dataframe_of_faker_mealkit_orders.groupBy().agg(*mf.features)
    assert (
        str(exc_info.value)
        == "Feature(s) ['NonExistentFeature', 'NonExistentFeature2'] not found"
    )
