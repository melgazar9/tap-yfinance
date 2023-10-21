"""Stream type classes for tap-yfinance."""

from __future__ import annotations

import pandas as pd
from singer_sdk import Tap
from typing import Iterable, Optional, Any
from singer_sdk.streams import Stream
from tap_yfinance.price_utils import *
from tap_yfinance.schema import *
from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.helpers._state import increment_state


class TickerStream(Stream):
    """Stream class for yahoo tickers."""

    replication_key = "yahoo_ticker"
    is_timestamp_replication_key = False

    def __init__(self, tap: Tap, catalog_entry: dict) -> None:
        """
        Initialize the database stream.

        Args:
            tap: The parent tap object.
            catalog_entry: Catalog entry dict.
        """

        self.catalog_entry = catalog_entry
        self.table_name = self.catalog_entry['table_name']

        super().__init__(
            tap=tap,
            schema=self.catalog_entry["schema"],
            name=self.catalog_entry["table_name"]
        )

        self.financial_category = self.catalog_entry['metadata'][-1]['metadata']['schema-name']

        self.ticker_downloader = TickerDownloader()

        self.ticker_downloader_mappings = {
            'stock_tickers': 'download_valid_stock_tickers',
            'forex_tickers': 'download_forex_pairs',
            'crypto_tickers': 'download_top_250_crypto_tickers'
        }

        self.stream_params: dict = self.config.get('financial_category').get(self.financial_category).get(self.name)
        self.schema_category = self.stream_params.get('schema_category')
        self.ticker_download_method = self.ticker_downloader_mappings[self.catalog_entry["table_name"]]

        self.df_tickers: pd.DataFrame = getattr(self.ticker_downloader, self.ticker_download_method)()

        if self.stream_params.get('tickers') != '*':
            assert isinstance(self.stream_params.get('tickers'), list)
            self.df_tickers = self.df_tickers[self.df_tickers['yahoo_ticker'].isin(self.stream_params.get('tickers'))]

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def partitions(self) -> list[dict]:
        return [{'ticker': t} for t in self.df_tickers['yahoo_ticker'].unique().tolist()]

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """
        Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.
        """
        state = self.get_context_state(context)
        df = self.df_tickers[self.df_tickers['yahoo_ticker'] == context['ticker']]

        for record in df.to_dict(orient='records'):
            increment_state(
                state,
                replication_key=self.replication_key,
                latest_record=record,
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted
            )
            yield record

class PriceStream(Stream):
    """Stream class for yahoo finance price streams."""

    replication_key = "timestamp"
    is_timestamp_replication_key = True

    def __init__(self, tap: Tap, catalog_entry: dict) -> None:
        """
        Initialize the stream.

        Args:
            tap: The parent tap object.
            catalog_entry: Catalog entry dict.
        """

        self.catalog_entry = catalog_entry
        self.table_name = self.catalog_entry['table_name']

        super().__init__(
            tap=tap,
            schema=self.catalog_entry["schema"],
            name=self.catalog_entry["table_name"]
        )

        self.financial_category = self.catalog_entry['metadata'][-1]['metadata']['schema-name']
        self.stream_params: dict = self.config.get('financial_category').get(self.financial_category).get(self.name)
        self.schema_category = self.stream_params.get('schema_category')

        self.yf_params = self.stream_params.get('yf_params')

        self.ticker_downloader = TickerDownloader()

        if self.stream_params['tickers'] == '*':
            if catalog_entry['tap_stream_id'].startswith('stock'):
                self.ticker_download_method = 'download_valid_stock_tickers'
            elif catalog_entry['tap_stream_id'].startswith('forex'):
                self.ticker_download_method = 'download_forex_pairs'
            elif catalog_entry['tap_stream_id'].startswith('crypto'):
                self.ticker_download_method = 'download_top_250_crypto_tickers'
            else:
                raise ValueError('Could not determine ticker download method.')

            self.tickers: list = \
                getattr(self.ticker_downloader, self.ticker_download_method)()['yahoo_ticker'].unique().tolist()

        else:
            self.tickers = self.stream_params['tickers'].copy()

        assert isinstance(self.tickers, list)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def partitions(self) -> list[dict]:
        return [{'ticker': t} for t in self.tickers]

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """
        Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.
        """

        price_tap = YFinancePriceTap(schema_category=self.schema_category)
        yf_params = self.yf_params.copy()
        state = self.get_context_state(context)

        if state and 'progress_markers' in state.keys():
            start_date = datetime.fromisoformat(state.get('progress_markers').get('replication_key_value')).strftime('%Y-%m-%d')
        else:
            start_date = self.config.get('default_start_date')

        yf_params['start'] = start_date

        df = price_tap.download_single_symbol_price_history(ticker=context['ticker'], yf_history_params=yf_params)

        for record in df.to_dict(orient='records'):
            replication_key = context['ticker'] + '|' + record['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')
            record['replication_key'] = replication_key
            increment_state(
                state,
                replication_key=self.replication_key,
                latest_record=record,
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted
            )

            yield record