"""Stream type classes for tap-yfinance."""

from __future__ import annotations
from singer_sdk import Tap
from typing import Iterable, Optional, Any
from singer_sdk.streams import Stream
from tap_yfinance.price_utils import *
from tap_yfinance.schema import *
from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.helpers._state import increment_state

class YFinancePriceStream(Stream):
    """Stream class for yahoo finance price streams."""

    replication_key = "timestamp"

    def __init__(self, tap: Tap, catalog_entry: dict) -> None:
        """Initialize the database stream.
        Args:
            tap: The parent tap object.
            catalog_entry: Catalog entry dict.
        """

        self.asset_class = None
        self.catalog_entry = catalog_entry
        self._table_name: str = self.catalog_entry["table_name"]

        super().__init__(
            tap=tap,
            schema=self.catalog_entry["schema"],
            name=self.catalog_entry["table_name"]
        )


    # @property
    # def partitions(self):
    #     # state_data = {}
    #     # for ticker in self.config['asset_class'][self.asset_class][self.name]['tickers']:
    #     #     state_data[ticker] = {
    #     #         "replication_key": "replication_key",
    #     #         "replication_key_value": f"{ticker}|{datetime.now()}"
    #     #     }
    #
    #     # for asset_class, asset_params in self.config['asset_class'].items():
    #     #     for table_name, table_params in asset_params.items():
    #     #         state_data[table_name] = {}
    #     #         for ticker in table_params['tickers']:
    #     #             state_data[table_name][ticker] = {
    #     #                 "replication_key": "replication_key",
    #     #                 "replication_key_value": f"{ticker}|{datetime.now()}"
    #     #             }
    #
    #     tables = self.config['asset_class'][self.asset_class].keys()
    #     state_data = [{'ticker': t} for t in self.config['asset_class'][self.asset_class][list(tables)[0]]['tickers']]
    #     return state_data

    # def _increment_stream_state(self, latest_record: dict[str, Any], *, context: dict | None = None) -> None:
    #     """Update state of stream or partition with data from the provided record.
    #     Args:
    #         latest_record: Dict of the latest record
    #         context: Stream partition or context dictionary.
    #     """
    #
    #     state = self.get_context_state(context)
    #
    #     if latest_record['yahoo_ticker'] == 'AAPL':
    #         self.logger.info('hi')
    #     increment_state(
    #         state,
    #         replication_key=self.replication_key,
    #         latest_record=latest_record,
    #         is_sorted=self.is_sorted,
    #         check_sorted=self.check_sorted
    #     )
    #
    #     return self

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.
        """

        state = None
        ticker_downloader = TickerDownloader()
        price_tap = YFinancePriceTap(asset_class=self.asset_class)

        stream_params: dict = self.config['asset_class'][self.asset_class][self.name]
        tickers: list = stream_params['tickers'].copy()
        yf_params: dict = stream_params['yf_params'].copy()

        if self.asset_class == 'stocks' and tickers == '*':
            df_tickers = ticker_downloader.download_pts_stock_tickers()
            tickers = df_tickers['yahoo_ticker'].tolist()
        elif self.asset_class == 'forex' and tickers == '*':
            df_tickers = ticker_downloader.download_forex_pairs()
            tickers = df_tickers['yahoo_ticker'].tolist()
        elif self.asset_class == 'crypto' and tickers == '*':
            df_tickers = ticker_downloader.download_top_250_crypto_tickers()
            tickers = df_tickers['yahoo_ticker'].tolist()
        else:
            assert tickers != '*', "tickers = '*' but did not use TickerDownloader() class!"

        for ticker in tickers:
            if self.replication_method == REPLICATION_INCREMENTAL:
                state = self.get_starting_replication_key_value(context)
                if isinstance(state, dict) and 'context' in state.keys() and ticker in state['context'].keys():
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
                if self.config.get('add_record_metadata', False) and record:
                    if isinstance(state, dict) and 'progress_markers' in state.keys():
                        batch_timestamp = state['progress_markers']['replication_key_value'].split('|')[1]
                    else:
                        batch_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')

                    replication_key = ticker + '|' + batch_timestamp
                    record['batch_timestamp'] = batch_timestamp
                    record['replication_key'] = replication_key
                self.logger.info(f'\n\n\n*** {record} ***\n\n\n')
                yield record
