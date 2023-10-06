"""Stream type classes for tap-yfinance."""

from __future__ import annotations

from pathlib import Path
from singer_sdk.streams import Stream
from typing import Iterable
import logging

from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from skimpy import clean_columns

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

# logger = logging.Logger()

def get_valid_yfinance_start_timestamp(interval, start='1950-01-01 00:00:00'):
    """
    Description
    -----------
    Get a valid yfinance date to lookback

    Valid intervals with maximum lookback period
    1m: 7 days
    2m: 60 days
    5m: 60 days
    15m: 60 days
    30m: 60 days
    60m: 730 days
    90m: 60 days
    1h: 730 days
    1d: 50+ years
    5d: 50+ years
    1wk: 50+ years
    1mo: 50+ years --- Buggy!
    3mo: 50+ years --- Buggy!

    Note: Often times yfinance returns an error even when looking back maximum number of days - 1,
        by default, return a date 2 days closer to the current date than the maximum specified in the yfinance docs
    """

    valid_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '1h', '90m', '1d', '5d', '1wk', '1mo', '3mo']
    assert interval in valid_intervals, f'must pass a valid interval {valid_intervals}'

    if interval == '1m':
        start = max((datetime.today() - timedelta(days=5)), pd.to_datetime(start))
    elif interval in ['2m', '5m', '15m', '30m', '90m']:
        start = (max((datetime.today() - timedelta(days=58)), pd.to_datetime(start)))
    elif interval in ['60m', '1h']:
        start = max((datetime.today() - timedelta(days=728)), pd.to_datetime(start))
    else:
        start = pd.to_datetime(start)
    start = start.strftime('%Y-%m-%d')  # yfinance doesn't like strftime with hours minutes or seconds

    return start

class YFinanceStream(Stream):
    """Define custom stream."""

    name = "yfinance_stream"
    schema = {
        "type": "object",
        "properties": {
            "replication_key": {"type": "string"}
        }
    }

    # the tap breaks when this is uncommented... need to figure out why
    # def __init__(self):
    #     self.failed_ticker_downloads = {}
    #     self.column_order = ['replication_key', 'timestamp', 'timestamp_tz_aware', 'timezone', 'yahoo_ticker', 'open',
    #     'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']

    def download_single_stock_price_history(self, ticker, yf_history_params=None):
        """
        Description
        -----------
        Download a single stock price ticker from the yfinance python library.
        Minor transformations happen:
            - Add column yahoo_ticker to show which ticker has been pulled
            - Set start date to the minimum start date allowed by yfinance for that ticker (passed in yf_history_params)
            - Clean column names
            - Set tz_aware timestamp column to be a string
        """
        yf_history_params = self.yf_params.copy() if yf_history_params is None else yf_history_params.copy()

        assert 'interval' in yf_history_params.keys(), 'must pass interval parameter to yf_history_params'

        if yf_history_params['interval'] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_history_params['interval']] = []

        if 'start' not in yf_history_params.keys():
            yf_history_params['start'] = '1950-01-01 00:00:00'

        yf_history_params['start'] = \
            get_valid_yfinance_start_timestamp(interval=yf_history_params['interval'], start=yf_history_params['start'])

        t = yf.Ticker(ticker)
        try:
            df = \
                t.history(**yf_history_params) \
                    .rename_axis(index='timestamp') \
                    .pipe(lambda x: clean_columns(x))

            self.n_requests += 1

            if df is not None and not df.shape[0]:
                self.failed_ticker_downloads[yf_history_params['interval']].append(ticker)
                return pd.DataFrame(columns=self.column_order)
        except:
            self.failed_ticker_downloads[yf_history_params['interval']].append(ticker)
            return

        df.loc[:, self.yahoo_ticker_colname] = ticker
        df.reset_index(inplace=True)
        df['timestamp_tz_aware'] = df['timestamp'].copy()
        df.loc[:, 'timezone'] = str(df['timestamp_tz_aware'].dt.tz)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        if self.convert_tz_aware_to_string:
            df['timestamp_tz_aware'] = df['timestamp_tz_aware'].astype(str)

        df.loc[:, "replication_key"] = df['timestamp'].astype(str) + '|' + df['yahoo_ticker'].astype(str)

        df = df[self.column_order]
        return df

    def get_records(self, context: dict) -> Iterable[dict]:
        # Define your list of tickers
        tickers = ["AAPL", "MSFT", "GOOGL"]

        yf_params = {'interval': '1m', 'start': '2023-10-01'}
        for ticker_symbol in tickers:
            ticker = yf.Ticker(ticker_symbol)
            df = self.download_single_stock_price_history(ticker=ticker, yf_history_params=yf_params)
            record = df.to_dict()
            yield record