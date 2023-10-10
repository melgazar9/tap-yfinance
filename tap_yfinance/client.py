"""Custom client handling, including YFinanceStream base class."""

from __future__ import annotations
from singer_sdk import Tap, typing as th
from typing import Iterable, Optional
from singer_sdk.streams import Stream
from tap_yfinance.price_utils import *
from singer_sdk.streams.core import REPLICATION_INCREMENTAL


class YFinanceStream(Stream):
    """Stream class for YFinance streams."""
    name = "tap-yfinance"
    replication_key = "replication_key"

    _schema = th.PropertiesList(  # Define the _schema attribute here
        th.Property("replication_key", th.StringType, required=True),
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("timestamp_tz_aware", th.StringType, required=True),
        th.Property("timezone", th.StringType, required=True),
        th.Property("yahoo_ticker", th.StringType, required=True),
        th.Property("open", th.NumberType, required=True),
        th.Property("high", th.NumberType, required=True),
        th.Property("low", th.NumberType, required=True),
        th.Property("close", th.NumberType, required=True),
        th.Property("volume", th.NumberType, required=True),
        th.Property("dividends", th.NumberType, required=True),
        th.Property("stock_splits", th.NumberType, required=True)
    ).to_dict()

    # TODO: Need to know why I can't override with __init__ method, even when calling super().__init__()
    # def __init__(self, tap: Tap):
    #     super().__init__(tap=tap)


    # @property
    # def is_sorted(self) -> bool:
    #     """Return a boolean indicating whether the replication key is alphanumerically sortable."""
    #     return self.replication_method == REPLICATION_INCREMENTAL



    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            NotImplementedError: If the implementation is TODO
        """

        bookmark = self.tap_state['bookmarks']

        ticker_downloader = TickerDownloader()

        for asset_class in self.config['asset_class'].keys():
            price_tap = YFinancePriceTap(asset_class=asset_class)

            asset_params = self.config['asset_class'][asset_class]

            for table_name in asset_params.keys():
                if asset_class == 'stocks' and asset_params[table_name]['tickers'] == '*':
                    df_tickers = ticker_downloader.download_pts_stock_tickers()
                    tickers = df_tickers['yahoo_ticker'].tolist()
                elif asset_class == 'forex' and asset_params[table_name]['tickers'] == '*':
                    df_tickers = ticker_downloader.download_forex_pairs()
                    tickers = df_tickers['yahoo_ticker'].tolist()
                elif asset_class == 'crypto' and asset_params[table_name]['tickers'] == '*':
                    df_tickers = ticker_downloader.download_top_250_crypto_tickers()
                    tickers = df_tickers['yahoo_ticker'].tolist()
                else:
                    tickers = asset_params[table_name]['tickers']
                    assert tickers != '*', 'tickers = * but did not use TickerDownloader() class!'

                # TODO: Write the df_tickers dataframe to its own table

                yf_params = asset_params[table_name]['yf_params'].copy()
                # data_category = "prices"

                for ticker in tickers:
                    if self.replication_method == REPLICATION_INCREMENTAL and bookmark:
                        self.logger.info(f"using existing bookmark: {bookmark}")
                        if table_name in bookmark.keys() and ticker in bookmark[table_name].keys():
                            start_date = bookmark[table_name][ticker]['last_timestamp']

                    else:
                        start_date = self.config.get("default_start_date", '1950-01-01')
                        self.logger.debug(f"no bookmark - using start date: {start_date}")

                    start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                    # TODO: Write each table_name to its own table

                    yf_params['start'] = max(start_date, '1950-01-01')

                    df = price_tap.download_single_symbol_price_history(ticker=ticker, yf_history_params=yf_params)

                    for record in df.to_dict(orient='records'):
                        yield record

                    # TODO: Integrate financials vs prices as data_category
                    # self.write_bookmark()