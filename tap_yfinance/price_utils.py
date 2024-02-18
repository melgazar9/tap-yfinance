from datetime import datetime, timedelta
import time
import logging
import pandas as pd
import numpy as np
import yfinance as yf
from pytickersymbols import PyTickerSymbols
from numerapi import SignalsAPI
from pandas_datareader import data as pdr
import re


class PriceTap():
    """
    Parameters
    ----------
    yf_params: dict - passed to yf.Ticker(<ticker>).history(**yf_params) - see docs for yfinance ticker.history() params
    ticker_colname: str of column name to set of output yahoo ticker columns
    """

    def __init__(self,
                 schema_category,
                 yf_params=None,
                 ticker_colname='ticker'):
        self.schema_category = schema_category
        self.yf_params = {} if yf_params is None else yf_params
        self.ticker_colname = ticker_colname

        super().__init__()

        if 'prepost' not in self.yf_params.keys():
            self.yf_params['prepost'] = True
        if 'start' not in self.yf_params.keys():
            self.yf_params['start'] = '1950-01-01'

        self.start_date = self.yf_params['start']

        assert pd.Timestamp(self.start_date) <= datetime.today(), 'Start date cannot be after the current date!'

        assert 'stock_prices' in self.schema_category \
               or 'futures_prices' in self.schema_category \
               or 'forex_prices' in self.schema_category \
               or 'crypto_prices' in self.schema_category, \
            "self.schema_category must be set to either 'stock_prices', 'futures_prices', 'forex_prices', or 'crypto_prices'"

        if self.schema_category == 'stock_prices':
            self.column_order = ['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'open', 'high', 'low',
                                 'close', 'volume', 'dividends', 'stock_splits', 'repaired']

        elif schema_category in ['futures_prices', 'forex_prices', 'crypto_prices']:
            self.column_order = ['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'open', 'high', 'low',
                                 'close', 'volume', 'repaired']

        elif schema_category.endswith('wide'):
            self.column_order = None

        else:
            raise ValueError('Could not determine price column order.')

        self.n_requests = 0

        self.failed_ticker_downloads = {}

    def _request_limit_check(self):
        """
        Description
        -----------
        Check if too many requests were made to yfinance within their allowed number of requests.
        """

        self.request_start_timestamp = datetime.now()
        self.current_runtime_seconds = (datetime.now() - self.request_start_timestamp).seconds

        if self.n_requests > 1900 and self.current_runtime_seconds > 3500:
            logging.info(f'\nToo many requests per hour. Pausing requests for {self.current_runtime_seconds} seconds.\n')
            time.sleep(np.abs(3600 - self.current_runtime_seconds))
        if self.n_requests > 45000 and self.current_runtime_seconds > 85000:
            logging.info(f'\nToo many requests per day. Pausing requests for {self.current_runtime_seconds} seconds.\n')
            time.sleep(np.abs(86400 - self.current_runtime_seconds))
        return self

    def download_price_history(self, ticker, yf_params=None) -> pd.DataFrame():
        """
        Description
        -----------
        Download a single stock price ticker from the yfinance python library.
        Minor transformations happen:
            - Add column ticker to show which ticker has been pulled
            - Set start date to the minimum start date allowed by yfinance for that ticker (passed in yf_params)
            - Clean column names
            - Set tz_aware timestamp column to be a string
        """

        yf_params = self.yf_params.copy() if yf_params is None else yf_params.copy()
        assert 'interval' in yf_params.keys(), 'must pass interval parameter to yf_params'

        if yf_params['interval'] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_params['interval']] = []

        if 'start' not in yf_params.keys():
            yf_params['start'] = '1950-01-01 00:00:00'
            logging.info(f'\n*** YF params start set to 1950-01-01 for ticker {ticker}! ***\n')

        yf_params['start'] = \
            get_valid_yfinance_start_timestamp(interval=yf_params['interval'], start=yf_params['start'])

        t = yf.Ticker(ticker)
        try:
            df = t.history(**yf_params).rename_axis(index='timestamp')
            df.columns = clean_strings(df.columns)

            self.n_requests += 1
            df.loc[:, self.ticker_colname] = ticker
            df = df.reset_index()
            df.loc[:, 'timestamp_tz_aware'] = df['timestamp'].copy()
            df.loc[:, 'timezone'] = str(df['timestamp_tz_aware'].dt.tz)
            df['timestamp_tz_aware'] = df['timestamp_tz_aware'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

            if df is not None and not df.shape[0]:
                self.failed_ticker_downloads[yf_params['interval']].append(ticker)
                return pd.DataFrame(columns=self.column_order)

            df = df.replace([np.inf, -np.inf, np.nan], None)  # None can be handled by json.dumps but inf and NaN can't be
            df = df[self.column_order]
            return df

        except Exception:
            self.failed_ticker_downloads[yf_params['interval']].append(ticker)
            return pd.DataFrame(columns=self.column_order)

    def download_price_history_wide(self, tickers, yf_params):
        yf_params = self.yf_params.copy() if yf_params is None else yf_params.copy()

        assert 'interval' in yf_params.keys(), 'must pass interval parameter to yf_params'

        if yf_params['interval'] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_params['interval']] = []

        if 'start' not in yf_params.keys():
            yf_params['start'] = '1950-01-01 00:00:00'
            logging.info(f'\n*** YF params start set to 1950-01-01 for ticker {ticker}! ***\n')

        yf_params['start'] = \
            get_valid_yfinance_start_timestamp(interval=yf_params['interval'], start=yf_params['start'])

        yf.pdr_override()

        df = pdr.get_data_yahoo(tickers, progress=False, **yf_params).rename_axis(index='timestamp').reset_index()
        self.n_requests += 1

        df.columns = flatten_multindex_columns(df)
        df.loc[:, 'timestamp_tz_aware'] = df['timestamp'].copy()
        df.loc[:, 'timezone'] = str(df['timestamp_tz_aware'].dt.tz)
        df['timestamp_tz_aware'] = df['timestamp_tz_aware'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

        if df is not None and not df.shape[0]:
            self.failed_ticker_downloads[yf_params['interval']].append(ticker)
            return pd.DataFrame(columns=self.column_order)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        return df


class TickerDownloader():
    """
    Description
    -----------
    Class to download PyTickerSymbols, Yahoo, and Numerai ticker symbols into a single dataframe.
    A mapping between all symbols is returned when calling the method download_valid_stock_tickers().
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def download_pts_stock_tickers():
        """
        Description
        -----------
        Download py-ticker-symbols tickers
        """
        pts = PyTickerSymbols()
        all_getters = list(filter(
            lambda x: (
                    x.endswith('_yahoo_tickers') or x.endswith('_google_tickers')
            ),
            dir(pts),
        ))

        all_tickers = {'yahoo_tickers': [], 'google_tickers': []}
        for t in all_getters:
            if t.endswith('google_tickers'):
                all_tickers['google_tickers'].append((getattr(pts, t)()))
            elif t.endswith('yahoo_tickers'):
                all_tickers['yahoo_tickers'].append((getattr(pts, t)()))
        all_tickers['google_tickers'] = flatten_list(all_tickers['google_tickers'])
        all_tickers['yahoo_tickers'] = flatten_list(all_tickers['yahoo_tickers'])
        if len(all_tickers['yahoo_tickers']) == len(all_tickers['google_tickers']):
            all_tickers = pd.DataFrame(all_tickers)
        else:
            all_tickers = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in all_tickers.items()]))

        all_tickers = \
            all_tickers \
                .rename(columns={'yahoo_tickers': 'yahoo_ticker', 'google_tickers': 'google_ticker'}) \
                .sort_values(by=['yahoo_ticker', 'google_ticker']) \
                .drop_duplicates()
        all_tickers = all_tickers.replace([-np.inf, np.inf, np.nan], None)

        return all_tickers

    @staticmethod
    def download_top_250_crypto_tickers(num_currencies=250):
        """
        Description
        -----------
        Download the top 250 cryptocurrencies
        Note: At the time of coding, setting num_currencies higher than 250 results in only 25 crypto tickers returned.
        """

        from requests_html import HTMLSession

        session = HTMLSession()
        resp = session.get(f"https://finance.yahoo.com/crypto?offset=0&count={num_currencies}")
        tables = pd.read_html(resp.html.raw_html)
        session.close()

        df = tables[0].copy()
        df = df.rename(columns={
            'Symbol': 'ticker',
            '% Change': 'pct_change',
            'Volume in Currency (Since 0:00 UTC)': 'volume_in_currency_since_0_00_utc'
        })

        df.columns = clean_strings(df.columns)

        # Add Decentral-Games tickers

        missing_dg_tickers = [i for i in ['ICE13133-USD', 'DG15478-USD', 'XDG-USD'] if i not in df['ticker']]
        if len(missing_dg_tickers):
            df_dg = pd.DataFrame({
                'ticker': missing_dg_tickers,
                'name': missing_dg_tickers,
                'price_intraday': np.nan,
                'change': np.nan,
                'pct_change': np.nan,
                'market_cap': np.nan,
                'volume_in_currency_since_0_00_utc': np.nan,
                'volume_in_currency_24_hr': np.nan,
                'total_volume_all_currencies_24_hr': np.nan,
                'circulating_supply': np.nan,
                '52_week_range': np.nan,
                'day_chart': np.nan
            })

            df = pd.concat([df, df_dg], axis=0).reset_index(drop=True)

        df = df.dropna(how='all', axis=1)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        return df

    @staticmethod
    def download_forex_pairs():
        """
        Description
        -----------
        Download the yfinance forex pair ticker names
        Note: At the time of coding, setting num_currencies higher than 250 results in only 25 crypto tickers returned.
        """

        from requests_html import HTMLSession

        session = HTMLSession()
        resp = session.get(f"https://finance.yahoo.com/currencies/")
        tables = pd.read_html(resp.html.raw_html)
        session.close()

        df = tables[0].copy()

        df = df.rename(columns={'Symbol': 'ticker', '% Change': 'pct_change'})
        df.columns = clean_strings(df.columns)

        df.loc[:, 'bloomberg_ticker'] = df['name'].apply(lambda x: f"{x[4:]}-{x[0:3]}")

        df = df.dropna(how='all', axis=1)
        df = df.replace([np.inf, -np.inf, np.nan], None)

        first_cols = ['ticker', 'name', 'bloomberg_ticker']
        df = df[first_cols + [i for i in df.columns if i not in first_cols]]
        return df

    @staticmethod
    def download_futures_tickers():
        """
        Description
        -----------
        Download the yfinance future contract ticker names
        Note: At the time of coding, setting num_currencies higher than 250 results in only 25 crypto tickers returned.
        """

        from requests_html import HTMLSession

        session = HTMLSession()
        resp = session.get(f"https://finance.yahoo.com/commodities/")
        tables = pd.read_html(resp.html.raw_html)
        session.close()

        df = tables[0].copy()
        df = df.rename(columns={'Symbol': 'ticker', '% Change': 'pct_change', 'Unnamed: 7': 'open_interest'})
        df.columns = clean_strings(df.columns)
        df['volume'] = df['volume'].astype(str)
        df['open_interest'] = df['open_interest'].astype(str)
        df = df.dropna(how='all', axis=1)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        return df

    def download_numerai_signals_ticker_map(
            self,
            napi=SignalsAPI(),
            numerai_ticker_link='https://numerai-signals-public-data.s3-us-west-2.amazonaws.com/signals_ticker_map_w_bbg.csv',
            yahoo_ticker_colname='yahoo'):

        """  Download numerai to yahoo ticker mapping  """

        ticker_map = pd.read_csv(numerai_ticker_link)
        eligible_tickers = pd.Series(napi.ticker_universe(), name='bloomberg_ticker')
        ticker_map = pd.merge(ticker_map, eligible_tickers, on='bloomberg_ticker', how='right')

        logging.info('Number of eligible tickers in map: %s', str(ticker_map.shape[0]))

        ticker_map = ticker_map.replace([np.inf, -np.inf, np.nan], None)
        valid_tickers = [i for i in ticker_map[yahoo_ticker_colname] if i is not None and len(i) > 0]
        logging.info(f'tickers before cleaning: %s', ticker_map.shape)
        ticker_map = ticker_map[ticker_map[yahoo_ticker_colname].isin(valid_tickers)]
        logging.info(f'tickers after cleaning: %s', ticker_map.shape)
        return ticker_map

    @classmethod
    def download_valid_stock_tickers(cls):
        """ Download the valid tickers from py-ticker-symbols """

        # napi = numerapi.SignalsAPI(os.environ.get('NUMERAI_PUBLIC_KEY'), os.environ.get('NUMERAI_PRIVATE_KEY'))

        def handle_duplicate_columns(columns):
            seen_columns = {}
            new_columns = []

            for column in columns:
                if column not in seen_columns:
                    new_columns.append(column)
                    seen_columns[column] = 1
                else:
                    seen_columns[column] += 1
                    new_columns.append(f"{column}_{seen_columns[column]}")
            return new_columns

        df_pts_tickers = cls.download_pts_stock_tickers()

        numerai_yahoo_tickers = \
            cls().download_numerai_signals_ticker_map() \
                .rename(columns={'yahoo': 'yahoo_ticker', 'ticker': 'numerai_ticker'})

        df1 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, on='yahoo_ticker', how='left').set_index('yahoo_ticker')
        df2 = pd.merge(numerai_yahoo_tickers, df_pts_tickers, on='yahoo_ticker', how='left').set_index('yahoo_ticker')
        df3 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, left_on='yahoo_ticker', right_on='numerai_ticker',
                       how='left') \
            .rename(columns={'yahoo_ticker_x': 'yahoo_ticker', 'yahoo_ticker_y': 'yahoo_ticker_old'}) \
            .set_index('yahoo_ticker')

        df4 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, left_on='yahoo_ticker', right_on='bloomberg_ticker',
                       how='left') \
            .rename(columns={'yahoo_ticker_x': 'yahoo_ticker', 'yahoo_ticker_y': 'yahoo_ticker_old'}) \
            .set_index('yahoo_ticker')

        df_tickers_wide = pd.concat([df1, df2, df3, df4], axis=1)
        df_tickers_wide.columns = handle_duplicate_columns(df_tickers_wide.columns)
        df_tickers_wide.columns = clean_strings(df_tickers_wide.columns)

        for col in df_tickers_wide.columns:
            suffix = col[-1]
            if suffix.isdigit():
                root_col = col.strip('_' + suffix)
                df_tickers_wide.loc[:, root_col] = df_tickers_wide[root_col].fillna(df_tickers_wide[col])

        df_tickers = \
            df_tickers_wide.reset_index() \
                [['yahoo_ticker', 'google_ticker', 'bloomberg_ticker', 'numerai_ticker', 'yahoo_ticker_old']] \
                .sort_values(
                by=['yahoo_ticker', 'google_ticker', 'bloomberg_ticker', 'numerai_ticker', 'yahoo_ticker_old']) \
                .drop_duplicates()

        df_tickers.loc[:, 'yahoo_valid_pts'] = False
        df_tickers.loc[:, 'yahoo_valid_numerai'] = False

        df_tickers.loc[
            df_tickers['yahoo_ticker'].isin(df_pts_tickers['yahoo_ticker'].tolist()), 'yahoo_valid_pts'] = True

        df_tickers.loc[
            df_tickers['yahoo_ticker'].isin(numerai_yahoo_tickers['numerai_ticker'].tolist()), 'yahoo_valid_numerai'
        ] = True

        df_tickers = df_tickers.replace([np.inf, -np.inf, np.nan], None)
        df_tickers = df_tickers.rename(columns={'yahoo_ticker': 'ticker'})  # necessary to allow schema partitioning
        return df_tickers

def get_valid_yfinance_start_timestamp(interval, start='1950-01-01 00:00:00'):
    """
    Description
    -----------
    Get a valid yfinance date to lookback.
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
        updated_start = max((datetime.today() - timedelta(days=5)).date(), pd.to_datetime(start).date())
    elif interval in ['2m', '5m', '15m', '30m', '90m']:
        updated_start = (max((datetime.today() - timedelta(days=58)).date(), pd.to_datetime(start).date()))
    elif interval in ['60m', '1h']:
        updated_start = max((datetime.today() - timedelta(days=728)).date(), pd.to_datetime(start).date())
    else:
        updated_start = pd.to_datetime(start)
    updated_start = updated_start.strftime('%Y-%m-%d')  # yfinance doesn't like strftime with hours, minutes, or seconds

    return updated_start

def flatten_list(lst):
    return [v for item in lst for v in (item if isinstance(item, list) else [item])]

def clean_strings(lst):
    cleaned_list = [re.sub(r'[^a-zA-Z0-9_]', '_', s) for s in lst]  # remove special characters
    cleaned_list = [re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower() for s in cleaned_list]  # camel case -> snake case
    cleaned_list = [re.sub(r'_+', '_', s).strip('_').lower() for s in cleaned_list]  # clean leading and trailing underscores
    return cleaned_list

def flatten_multindex_columns(df):
    new_cols = list(clean_strings(pd.Index([str(e[0]).lower() + '_' + str(e[1]).lower() for e in df.columns.tolist()])))
    return new_cols