from datetime import date

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.features.count import Count
from jstark.features.net_spend import NetSpend
from jstark.features.gross_spend import GrossSpend
from jstark.features.recency_days import RecencyDays
from jstark.grocery.basket_count import BasketCount
from jstark.grocery.store_count import StoreCount
from jstark.features.product_count import ProductCount
from jstark.features.approx_product_count import ApproxProductCount
from jstark.features.customer_count import CustomerCount
from jstark.grocery.channel_count import ChannelCount
from jstark.features.approx_customer_count import ApproxCustomerCount
from jstark.grocery.approx_basket_count import ApproxBasketCount
from jstark.features.discount import Discount
from jstark.features.min_gross_spend import MinGrossSpend
from jstark.features.max_gross_spend import MaxGrossSpend
from jstark.features.min_net_spend import MinNetSpend
from jstark.features.max_net_spend import MaxNetSpend
from jstark.grocery.average_gross_spend_per_basket import AvgGrossSpendPerBasket
from jstark.features.quantity import Quantity
from jstark.grocery.average_quantity_per_basket import AvgQuantityPerBasket
from jstark.features.most_recent_purchase_date import MostRecentPurchaseDate
from jstark.features.min_net_price import MinNetPrice
from jstark.features.max_net_price import MaxNetPrice
from jstark.features.min_gross_price import MinGrossPrice
from jstark.features.max_gross_price import MaxGrossPrice
from jstark.features.earliest_purchase_date import EarliestPurchaseDate
from jstark.grocery.average_discount_per_basket import AvgDiscountPerBasket
from jstark.grocery.average_purchase_cycle import AvgPurchaseCycle
from jstark.grocery.cycles_since_last_purchase import CyclesSinceLastPurchase
from jstark.grocery.basket_periods import BasketPeriods
from jstark.grocery.recency_weighted_basket import (
    RecencyWeightedBasket99,
    RecencyWeightedBasket90,
    RecencyWeightedBasket95,
    RecencyWeightedApproxBasket90,
    RecencyWeightedApproxBasket95,
    RecencyWeightedApproxBasket99,
)
from jstark.grocery.average_basket import AvgBasket
from jstark.features.feature import Feature
from jstark.feature_generator import FeatureGenerator


class GroceryFeatures(FeatureGenerator):
    def __init__(
        self,
        as_at: date,
        feature_periods: list[FeaturePeriod] | list[str] = [
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 0),
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 4, 3),
        ],
    ) -> None:
        super().__init__(as_at, feature_periods)

    FEATURE_CLASSES: list[type[Feature]] = [
        Count,
        NetSpend,
        GrossSpend,
        RecencyDays,
        BasketCount,
        StoreCount,
        ProductCount,
        CustomerCount,
        ChannelCount,
        ApproxBasketCount,
        ApproxProductCount,
        ApproxCustomerCount,
        Discount,
        MinGrossSpend,
        MaxGrossSpend,
        MinNetSpend,
        MaxNetSpend,
        AvgGrossSpendPerBasket,
        Quantity,
        AvgQuantityPerBasket,
        MostRecentPurchaseDate,
        MinNetPrice,
        MaxNetPrice,
        MinGrossPrice,
        MaxGrossPrice,
        EarliestPurchaseDate,
        AvgDiscountPerBasket,
        AvgPurchaseCycle,
        CyclesSinceLastPurchase,
        BasketPeriods,
        RecencyWeightedBasket95,
        RecencyWeightedBasket90,
        RecencyWeightedBasket99,
        RecencyWeightedApproxBasket95,
        RecencyWeightedApproxBasket90,
        RecencyWeightedApproxBasket99,
        AvgBasket,
    ]
