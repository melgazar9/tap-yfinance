"""YFinance tap class."""

from __future__ import annotations

import singer_sdk.typing as th
from singer_sdk import Tap

from tap_yfinance.streams import (
    ActionsStream,
    AllTickersStream,
    AnalystPriceTargetsStream,
    BalanceSheetStream,
    BondsTickersStream,
    CalendarStream,
    CashFlowStream,
    CryptoPrices1dStream,
    CryptoPrices1hStream,
    CryptoPrices1mStream,
    CryptoPrices2mStream,
    CryptoPrices5mStream,
    CryptoPricesWide1mStream,
    CryptoTickersStream,
    CryptoTickersTop250Stream,
    DividendsStream,
    EarningsDatesStream,
    EarningsEstimateStream,
    EarningsHistoryStream,
    EpsRevisionsStream,
    EpsTrendStream,
    ETFTickersStream,
    FastInfoStream,
    FinancialsStream,
    ForexPrices1dStream,
    ForexPrices1hStream,
    ForexPrices1mStream,
    ForexPrices2mStream,
    ForexPrices5mStream,
    ForexPricesWide1mStream,
    ForexTickersStream,
    FuturesPrices1dStream,
    FuturesPrices1hStream,
    FuturesPrices1mStream,
    FuturesPrices2mStream,
    FuturesPrices5mStream,
    FuturesPricesWide1mStream,
    FuturesTickersStream,
    GrowthEstimatesStream,
    HistoryMetadataStream,
    IncomeStmtStream,
    IndicesTickersStream,
    InfoStream,
    InsiderPurchasesStream,
    InsiderRosterHoldersStream,
    InsiderTransactionsStream,
    InstitutionalHoldersStream,
    IsInStream,
    MajorHoldersStream,
    MutualFundHoldersStream,
    MutualFundTickersStream,
    NewsStream,
    OptionChainStream,
    OptionsStream,
    OptionsTickersStream,
    PriceStream1d,
    PriceStream1h,
    PriceStream1m,
    PriceStream2m,
    PriceStream5m,
    PrivateCompaniesStream,
    PTSTickersStream,
    QuarterlyBalanceSheetStream,
    QuarterlyCashFlowStream,
    QuarterlyFinancialsStream,
    QuarterlyIncomeStmtStream,
    RecommendationsStream,
    RecommendationsSummaryStream,
    RevenueEstimateStream,
    SecFilingsStream,
    SECTickersStream,
    SectorTickersStream,
    SharesFullStream,
    SplitsStream,
    StockPrices1dStream,
    StockPrices1hStream,
    StockPrices1mStream,
    StockPrices2mStream,
    StockPrices5mStream,
    StockPricesWide1mStream,
    StockTickersStream,
    SustainabilityStream,
    TtmCashFlowStream,
    TtmFinancialsStream,
    TtmIncomeStmtStream,
    UpgradesDowngradesStream,
)

STREAMS = [
    AllTickersStream,
    PriceStream1m,
    PriceStream2m,
    PriceStream5m,
    PriceStream1h,
    PriceStream1d,
    StockTickersStream,
    PTSTickersStream,
    FuturesTickersStream,
    BondsTickersStream,
    ForexTickersStream,
    OptionsTickersStream,
    IndicesTickersStream,
    CryptoTickersStream,
    CryptoTickersTop250Stream,
    PrivateCompaniesStream,
    ETFTickersStream,
    MutualFundTickersStream,
    StockPrices1mStream,
    StockPrices2mStream,
    StockPrices5mStream,
    StockPrices1hStream,
    StockPrices1dStream,
    FuturesPrices1mStream,
    FuturesPrices2mStream,
    FuturesPrices5mStream,
    FuturesPrices1hStream,
    FuturesPrices1dStream,
    ForexPrices1mStream,
    ForexPrices2mStream,
    ForexPrices5mStream,
    ForexPrices1hStream,
    ForexPrices1dStream,
    CryptoPrices1mStream,
    CryptoPrices2mStream,
    CryptoPrices5mStream,
    CryptoPrices1hStream,
    CryptoPrices1dStream,
    StockPricesWide1mStream,
    FuturesPricesWide1mStream,
    ForexPricesWide1mStream,
    CryptoPricesWide1mStream,
    ActionsStream,
    AnalystPriceTargetsStream,
    BalanceSheetStream,
    CalendarStream,
    CashFlowStream,
    DividendsStream,
    EarningsDatesStream,
    EarningsEstimateStream,
    EarningsHistoryStream,
    EpsRevisionsStream,
    EpsTrendStream,
    FastInfoStream,
    FinancialsStream,
    GrowthEstimatesStream,
    HistoryMetadataStream,
    InfoStream,
    IncomeStmtStream,
    InsiderPurchasesStream,
    InsiderRosterHoldersStream,
    InsiderTransactionsStream,
    InstitutionalHoldersStream,
    IsInStream,
    MajorHoldersStream,
    MutualFundHoldersStream,
    MutualFundTickersStream,
    NewsStream,
    RecommendationsStream,
    RecommendationsSummaryStream,
    RevenueEstimateStream,
    SecFilingsStream,
    SectorTickersStream,
    SECTickersStream,
    SharesFullStream,
    SplitsStream,
    SustainabilityStream,
    TtmCashFlowStream,
    TtmFinancialsStream,
    TtmIncomeStmtStream,
    OptionChainStream,
    OptionsStream,
    QuarterlyBalanceSheetStream,
    QuarterlyCashFlowStream,
    QuarterlyFinancialsStream,
    QuarterlyIncomeStmtStream,
    UpgradesDowngradesStream,
]


class TapYFinance(Tap):
    name = "tap-yfinance"
    config_jsonschema = th.PropertiesList(
        th.Property("start_date", th.StringType, required=False),
        th.Property("default_interval", th.StringType, required=False),
        th.Property("yf_cache_params", th.ObjectType(), required=False),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ticker_cache = {}
        self._first_stream_processed = False

    def discover_streams(self):
        return [stream(tap=self) for stream in STREAMS]


if __name__ == "__main__":
    TapYFinance.cli()
