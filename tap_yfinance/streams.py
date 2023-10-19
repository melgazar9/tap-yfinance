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
    is_timestamp_replication_key = True

    def __init__(self, tap: Tap, catalog_entry: dict) -> None:
        """Initialize the database stream.

        Args:
            tap: The parent tap object.
            catalog_entry: Catalog entry dict.
        """

        self.catalog_entry = catalog_entry

        if catalog_entry['table_name'].startswith('stock'):
            self.asset_class = 'stocks'
        elif catalog_entry['table_name'].startswith('forex'):
            self.asset_class = 'forex'
        elif catalog_entry['table_name'].startswith('crypto'):
            self.asset_class = 'crypto'
        else:
            raise ValueError('Could not parse asset class.')

        self.table_name = self.catalog_entry['table_name']
        self.schema = get_price_schema(asset_class=self.asset_class)

        super().__init__(
            tap=tap,
            schema=self.catalog_entry["schema"],
            name=self.catalog_entry["table_name"]
        )

        self.stream_params: dict = self.config['asset_class'][self.asset_class][self.name]
        self.tickers: list = self.stream_params['tickers'].copy()
        self.yf_params: dict = self.stream_params['yf_params'].copy()

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def partitions(self) -> list[dict]:
        return [{'ticker': t} for t in self.stream_params['tickers']]

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.

        """

        ticker_downloader = TickerDownloader()
        price_tap = YFinancePriceTap(asset_class=self.asset_class)
        yf_params = self.yf_params.copy()

        if self.asset_class == 'stocks' and self.tickers == '*':
            df_tickers = ticker_downloader.download_pts_stock_tickers()
            tickers = df_tickers['ticker'].tolist()
        elif self.asset_class == 'forex' and self.tickers == '*':
            df_tickers = ticker_downloader.download_forex_pairs()
            tickers = df_tickers['ticker'].tolist()
        elif self.asset_class == 'crypto' and self.tickers == '*':
            df_tickers = ticker_downloader.download_top_250_crypto_tickers()
            tickers = df_tickers['ticker'].tolist()
        else:
            assert self.tickers != '*', "tickers = '*' but did not use TickerDownloader() class!"
            tickers = self.tickers

        for ticker in tickers:
            state = self.get_context_state(context)
            if state and 'replication_key_value' in state.keys():
                self.logger.info(f"\n\n\n{state}\n\n\n")
                start_date = datetime.fromisoformat(state.get('replication_key_value')).strftime('%Y-%m-%d')
            else:
                start_date = self.config.get('default_start_date')

            yf_params['start'] = start_date

            df = price_tap.download_single_symbol_price_history(ticker=ticker, yf_history_params=yf_params)

            for record in df.to_dict(orient='records'):
                batch_timestamp = datetime.utcnow().strftime('%Y-%m-%d')
                replication_key = ticker + '|' + batch_timestamp
                record['replication_key'] = replication_key

                if ticker == 'AAPL' and pd.to_datetime(record['timestamp'], utc=True) > pd.to_datetime('2023-10-01', utc=True):
                    continue
                elif ticker == 'NVDA' and pd.to_datetime(record['timestamp'], utc=True) > pd.to_datetime('2023-10-09', utc=True):
                    continue
                elif ticker == 'BTC-USD' and pd.to_datetime(record['timestamp'], utc=True) > pd.to_datetime('2023-10-07', utc=True):
                    continue
                else:
                    increment_state(
                        state,
                        replication_key=self.replication_key,
                        latest_record=record,
                        is_sorted=True,
                        check_sorted=self.check_sorted
                    )

                yield record
