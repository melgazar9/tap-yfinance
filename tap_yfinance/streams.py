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

        self.catalog_entry = catalog_entry

        # TODO: Fix this -- meltano is deselecting all streams when tap_stream_id, stream, and table are not all the same in
        #  CatalogEntry. E.G. it fails with:
        #  CatalogEntry(
        #               tap_stream_id=asset_class + '|' + table_name,
        #               stream=asset_class + '|' + table_name,
        #               table=table_name, ...)

        try:
            self.asset_class = self.catalog_entry['tap_stream_id'].split('|')[0]
            assert self.asset_class in ('stocks', 'forex', 'crypto')
        except:
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

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

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
            start_date = '1950-01-01'
            yf_params['start'] = max(start_date, '1950-01-01')
            df = price_tap.download_single_symbol_price_history(ticker=ticker, yf_history_params=yf_params)
            for record in df.to_dict(orient='records'):
                batch_timestamp = datetime.utcnow().strftime('%Y-%m-%d')
                replication_key = ticker + '|' + batch_timestamp
                record['replication_key'] = replication_key
                self.logger.info(f'\n\n\n*** {record} ***\n\n\n')
                yield record
