"""Stream type classes for tap-yfinance."""

from __future__ import annotations
from singer_sdk import Tap, typing as th
from typing import Iterable, Optional, Any
from singer_sdk.streams import Stream
from tap_yfinance.price_utils import *
from singer_sdk.streams.core import REPLICATION_INCREMENTAL

class YFinanceStream(Stream):
    """Stream class for yahoo finance price streams."""

    replication_key = "replication_key"

    _schema = th.PropertiesList(  # Define the _schema attribute here
        th.Property("replication_key", th.StringType, required=True),
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("timestamp_tz_aware", th.StringType, required=True),
        th.Property("timezone", th.StringType, required=True),
        th.Property("yahoo_ticker", th.StringType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("dividends", th.NumberType),
        th.Property("stock_splits", th.NumberType),
        th.Property("repaired", th.StringType)
    ).to_dict()

    def __init__(self, tap: Tap, name: str, asset_class: str):
        super().__init__(tap, name=name)
        self.asset_class = asset_class

    @property
    def partitions(self):
        state_data = {}
        for ticker in self.config['asset_class'][self.asset_class][self.name]['tickers']:
            state_data[ticker] = {
                "replication_key": "replication_key",
                "replication_key_value": f"{ticker}|{datetime.now()}"
            }

        # for asset_class, asset_params in self.config['asset_class'].items():
        #     for table_name, table_params in asset_params.items():
        #         state_data[table_name] = {}
        #         for ticker in table_params['tickers']:
        #             state_data[table_name][ticker] = {
        #                 "replication_key": "replication_key",
        #                 "replication_key_value": f"{ticker}|{datetime.now()}"
        #             }

        return [state_data]


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

        ticker_downloader = TickerDownloader()
        price_tap = YFinancePriceTap(asset_class=self.asset_class)
        asset_params = self.config['asset_class'][self.asset_class]

        print(f'\n\n\n{self.asset_class}\n\n\n')
        print(f'\n\n\n{self.name}\n\n\n')

        if self.asset_class == 'stocks' and asset_params[self.name]['tickers'] == '*':
            df_tickers = ticker_downloader.download_pts_stock_tickers()
            tickers = df_tickers['yahoo_ticker'].tolist()
        elif self.asset_class == 'forex' and asset_params[self.name]['tickers'] == '*':
            df_tickers = ticker_downloader.download_forex_pairs()
            tickers = df_tickers['yahoo_ticker'].tolist()
        elif self.asset_class == 'crypto' and asset_params[self.name]['tickers'] == '*':
            df_tickers = ticker_downloader.download_top_250_crypto_tickers()
            tickers = df_tickers['yahoo_ticker'].tolist()
        else:
            tickers = asset_params[self.name]['tickers']
            assert tickers != '*', "tickers = '*' but did not use TickerDownloader() class!"

        yf_params = asset_params[self.name]['yf_params'].copy()

        for ticker in tickers:
            print(f'\n\n\n{ticker}\n\n\n')
            if self.replication_method == REPLICATION_INCREMENTAL:
                state = self.get_context_state(context)
                print(f'\n\n\nSTATE: {state}\n\n\n')
                if 'context' in state.keys() and ticker in state['context'].keys():
                    replication_key = state['context'][ticker]['replication_key_value']
                    start_date = \
                        datetime.strptime(replication_key.split('|')[1], '%Y-%m-%d %H:%M:%S.%f')\
                                .strftime('%Y-%m-%d')
                else:
                    start_date = self.config.get("default_start_date", '1950-01-01')
                    start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                    self.logger.warning(f"Key 'context' is not in state.keys() - Using start date: {start_date}")
            else:
                start_date = self.config.get("default_start_date", '1950-01-01')
                start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                self.logger.warning(f"replication_method is not set to {REPLICATION_INCREMENTAL}! - Using start date: {start_date}")

            yf_params['start'] = max(start_date, '1950-01-01')

            df = price_tap.download_single_symbol_price_history(ticker=ticker, yf_history_params=yf_params)

            for record in df.to_dict(orient='records'):
                if self.config['add_record_metadata']:
                    replication_key = state['context'][ticker]['replication_key_value']
                    batch_timestamp = state['context'][ticker]['replication_key_value'].split('|')[1]
                    record['replication_key'] = replication_key
                    record['batch_timestamp'] = batch_timestamp
                    print(f"\n\n\n****** {record} ****** \n\n\n")
                yield record