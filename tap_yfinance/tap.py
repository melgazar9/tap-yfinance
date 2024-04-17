"""YFinance tap class."""

from __future__ import annotations

import singer_sdk.typing as th
from singer_sdk import Tap

from singer_sdk._singerlib.catalog import (
    CatalogEntry,
    MetadataMapping,
    Schema,
    Metadata,
)


from tap_yfinance.streams import (
    StockTickersStream,
    FuturesTickersStream,
    ForexTickersStream,
    CryptoTickersStream,
    CryptoTickersTop250Stream,
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
    BalanceSheetStream,
    CalendarStream,
    CashFlowStream,
    DividendsStream,
    EarningsDatesStream,
    FastInfoStream,
    FinancialsStream,
    HistoryMetadataStream,
    IncomeStmtStream,
    InsiderPurchasesStream,
    InsiderRosterHoldersStream,
    InsiderTransactionsStream,
    InstitutionalHoldersStream,
    MajorHoldersStream,
    MutualFundHoldersStream,
    NewsStream,
    RecommendationsStream,
    SharesFullStream,
    SplitsStream,
    OptionChainStream,
    OptionsStream,
    QuarterlyBalanceSheetStream,
    QuarterlyCashFlowStream,
    QuarterlyFinancialsStream,
    QuarterlyIncomeStmtStream,
    UpgradesDowngradesStream
)


STREAMS = [
    StockTickersStream,
    FuturesTickersStream,
    ForexTickersStream,
    CryptoTickersStream,
    CryptoTickersTop250Stream,
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
    BalanceSheetStream,
    CalendarStream,
    CashFlowStream,
    DividendsStream,
    EarningsDatesStream,
    FastInfoStream,
    FinancialsStream,
    HistoryMetadataStream,
    IncomeStmtStream,
    InsiderPurchasesStream,
    InsiderRosterHoldersStream,
    InsiderTransactionsStream,
    InstitutionalHoldersStream,
    MajorHoldersStream,
    MutualFundHoldersStream,
    NewsStream,
    RecommendationsStream,
    SharesFullStream,
    SplitsStream,
    OptionChainStream,
    OptionsStream,
    QuarterlyBalanceSheetStream,
    QuarterlyCashFlowStream,
    QuarterlyFinancialsStream,
    QuarterlyIncomeStmtStream,
    UpgradesDowngradesStream
]


class TapYFinance(Tap):
    """YFinance tap class."""

    name = "tap-yfinance"

    def discover_streams(self) -> list:
        """Return a list of discovered streams."""

        return [stream(tap=self) for stream in STREAMS]


if __name__ == "__main__":
    TapYFinance.cli()