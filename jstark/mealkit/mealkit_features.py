from datetime import date

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.features.count import Count
from jstark.features.recency_days import RecencyDays
from jstark.features.customer_count import CustomerCount
from jstark.features.approx_customer_count import ApproxCustomerCount
from jstark.features.quantity import Quantity
from jstark.features.product_count import ProductCount
from jstark.features.most_recent_purchase_date import MostRecentPurchaseDate
from jstark.features.earliest_purchase_date import EarliestPurchaseDate
from jstark.mealkit.order_periods import OrderPeriods
from jstark.mealkit.order_count import OrderCount
from jstark.mealkit.approx_order_count import ApproxOrderCount
from jstark.features.approx_product_count import ApproxProductCount
from jstark.mealkit.recipe_count import RecipeCount
from jstark.mealkit.approx_recipe_count import ApproxRecipeCount
from jstark.mealkit.average_order import AvgOrder
from jstark.features.discount import Discount
from jstark.features.feature import Feature
from jstark.feature_generator import FeatureGenerator
from jstark.mealkit.average_quantity_per_order import AvgQuantityPerOrder
from jstark.mealkit.cycles_since_last_order import CyclesSinceLastOrder
from jstark.mealkit.average_purchase_cycle import AvgPurchaseCycle


class MealkitFeatures(FeatureGenerator):
    def __init__(
        self,
        as_at: date,
        feature_periods: list[FeaturePeriod] | list[str] = [
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 0),
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 4, 3),
        ],
        feature_stems: set[str] | list[str] = set[str](),
    ) -> None:
        super().__init__(as_at, feature_periods, feature_stems)

    FEATURE_CLASSES: set[type[Feature]] = {
        Count,
        # NetSpend,
        # GrossSpend,
        RecencyDays,
        CustomerCount,
        ApproxCustomerCount,
        Quantity,
        MostRecentPurchaseDate,
        EarliestPurchaseDate,
        OrderPeriods,
        OrderCount,
        ApproxOrderCount,
        ProductCount,
        ApproxProductCount,
        RecipeCount,
        ApproxRecipeCount,
        AvgOrder,
        Discount,
        AvgQuantityPerOrder,
        CyclesSinceLastOrder,
        AvgPurchaseCycle,
    }
