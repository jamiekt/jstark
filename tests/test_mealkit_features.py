from datetime import datetime
from pyspark.sql import DataFrame

from jstark.mealkit.mealkit_features import MealkitFeatures


def test_orderweeks(
    as_at_timestamp: datetime, dataframe_of_faker_mealkit_orders: DataFrame
):
    """Test OrderWeeks

    OrderWeeks is the number of weeks in which at least one order was placed

    """
    pfg = MealkitFeatures(as_at=as_at_timestamp, feature_periods=["52w0"])
    output_df = (
        dataframe_of_faker_mealkit_orders.groupBy()
        .agg(*pfg.features)
        .select("OrderCount_52w0")
    )
    first = output_df.first()
    assert first is not None
    assert first["OrderCount_52w0"] == 872


# def test_basketweeks_commentary(
#     as_at_timestamp: datetime, dataframe_of_faker_purchases: DataFrame
# ):
#     """Test BasketWeeks commentary"""
#     pfg = MealkitFeatures(as_at=as_at_timestamp, feature_periods=["52w1"])
#     output_df = (
#         dataframe_of_faker_purchases.groupBy()
#         .agg(*pfg.features)
#         .select("BasketWeeks_52w1")
#     )
#     assert [(c.metadata["commentary"]) for c in output_df.schema][0] == (
#         "The number of weeks in which at least one basket was purchased. "
#         + "The value will be in the range 0 to 52"
#         + " because 52 is the number of weeks between 2021-11-28 and 2022-11-26."
#         + " When grouped by Customer and Product"
#         + " this feature is a useful indicator of the frequency of which a"
#         + " Customer purchases a Product."
#     )
