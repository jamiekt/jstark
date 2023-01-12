from datetime import date
from typing import List, Union

from jstark.feature_period import FeaturePeriod, PeriodUnitOfMeasure
from jstark.features import (
    Count,
    NetSpend,
    GrossSpend,
    RecencyDays,
    BasketCount,
    StoreCount,
    ProductCount,
    CustomerCount,
    ChannelCount,
    ApproxCustomerCount,
    ApproxBasketCount,
    Discount,
    MinGrossSpend,
    MaxGrossSpend,
    MinNetSpend,
    MaxNetSpend,
    AverageGrossSpendPerBasket,
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
    RecencyWeightedBasket99,
    RecencyWeightedBasket90,
    RecencyWeightedBasket95,
    AverageBasket,
)
from jstark.feature_generator import FeatureGenerator


class PurchasingFeatureGenerator(FeatureGenerator):
    def __init__(
        self,
        as_at: date,
        feature_periods: Union[List[FeaturePeriod], List[str]] = [
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 2, 0),
            FeaturePeriod(PeriodUnitOfMeasure.DAY, 4, 3),
        ],
    ) -> None:
        super().__init__(as_at, feature_periods)

    # would prefer list[Type[Feature]] as type hint but
    # this only works on py3.10 and above
    FEATURE_CLASSES: list = [
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
        ApproxCustomerCount,
        Discount,
        MinGrossSpend,
        MaxGrossSpend,
        MinNetSpend,
        MaxNetSpend,
        AverageGrossSpendPerBasket,
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
        AverageBasket,
    ]
