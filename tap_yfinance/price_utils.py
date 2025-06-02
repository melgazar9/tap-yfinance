import inspect
import logging
import re
import threading
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr
from pytickersymbols import PyTickerSymbols
from requests_html import HTMLSession

pd.set_option("future.no_silent_downcasting", True)


class PriceTap:
    """
    Parameters
    ----------
    yf_params: dict - passed to yf.Ticker(<ticker>).history(**yf_params) - see docs for yfinance ticker.history() params
    ticker_colname: str of column name to set of output yahoo ticker columns
    """

    def __init__(
        self, schema, config, name, ticker=None, yf_params=None, ticker_colname="ticker"
    ):
        self.schema = schema
        self.ticker = ticker
        self.name = name
        self.config = config
        self.yf_params = {} if yf_params is None else yf_params
        self.ticker_colname = ticker_colname

        self.column_order = list(self.schema.get("properties").keys())

        super().__init__()

        if (
            isinstance(self.ticker, str)
            and self.config is not None
            and "yf_cache_params" in self.config.get(self.name)
        ):
            rate_request_limit = (
                self.config.get(self.name)
                .get("yf_cache_params")
                .get("rate_request_limit")
            )
            rate_seconds_limit = (
                self.config.get(self.name)
                .get("yf_cache_params")
                .get("rate_seconds_limit")
            )

            from pyrate_limiter import Duration, Limiter, RequestRate
            from requests import Session
            from requests_cache import CacheMixin, SQLiteCache
            from requests_ratelimiter import LimiterMixin, MemoryQueueBucket

            class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
                pass

            self.session = CachedLimiterSession(
                limiter=Limiter(
                    RequestRate(
                        rate_request_limit, Duration.SECOND * rate_seconds_limit
                    )
                ),
                bucket_class=MemoryQueueBucket,
                backend=SQLiteCache("~/yfinance.cache"),
            )

            self.yf_ticker_obj = yf.Ticker(self.ticker, session=self.session)

        elif self.ticker is not None:
            self.yf_ticker_obj = yf.Ticker(self.ticker)
        else:
            self.yf_ticker_obj = None

        if "prepost" not in self.yf_params.keys():
            self.yf_params["prepost"] = True
        if "start" not in self.yf_params.keys():
            self.yf_params["start"] = "1950-01-01"

        self.start_date = self.yf_params["start"]

        assert (
            pd.Timestamp(self.start_date) <= datetime.today()
        ), "Start date cannot be after the current date!"

        self.n_requests = 0
        self.failed_ticker_downloads = {}

    def _request_limit_check(self):
        """
        Description
        -----------
        Check if too many requests were made to yfinance within their allowed number of requests.
        """

        self.request_start_timestamp = datetime.now()
        self.current_runtime_seconds = (
            datetime.now() - self.request_start_timestamp
        ).seconds

        if self.n_requests > 1900 and self.current_runtime_seconds > 3500:
            logging.info(
                f"\nToo many requests per hour. Pausing requests for {self.current_runtime_seconds} seconds.\n"
            )
            time.sleep(np.abs(3600 - self.current_runtime_seconds))
        if self.n_requests > 45000 and self.current_runtime_seconds > 85000:
            logging.info(
                f"\nToo many requests per day. Pausing requests for {self.current_runtime_seconds} seconds.\n"
            )
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
        method = get_method_name()
        yf_params = self.yf_params.copy() if yf_params is None else yf_params.copy()
        logging.info(
            f"*** Running {method} for ticker {ticker} and stream {self.name} ***"
        )
        assert (
            "interval" in yf_params.keys()
        ), "must pass interval parameter to yf_params"

        if yf_params["interval"] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_params["interval"]] = []

        if "start" not in yf_params.keys() or yf_params["start"] is None:
            yf_params["start"] = "1950-01-01 00:00:00"
            logging.info(
                f"\n*** YF params start set to 1950-01-01 for ticker {ticker}! ***\n"
            )

        yf_params["start"] = get_valid_yfinance_start_timestamp(
            interval=yf_params["interval"], start=yf_params["start"]
        )

        try:
            df = self.yf_ticker_obj.history(**yf_params).rename_axis(index="timestamp")
            df.columns = clean_strings(df.columns)

            self.n_requests += 1
            df.loc[:, self.ticker_colname] = ticker
            df = df.reset_index()
            df.loc[:, "timestamp_tz_aware"] = df["timestamp"].copy()
            df.loc[:, "timezone"] = str(df["timestamp_tz_aware"].dt.tz)
            df["timestamp_tz_aware"] = df["timestamp_tz_aware"].dt.strftime(
                "%Y-%m-%d %H:%M:%S%z"
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

            if df is not None and not df.shape[0]:
                self.failed_ticker_downloads[yf_params["interval"]].append(ticker)
                return pd.DataFrame(columns=self.column_order)

            df = replace_all_specified_missing(df, exclude_columns=["ticker"])
            df = fix_empty_values(df)
            df["replication_key"] = (
                df["ticker"] + "|" + df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            )

            prefixes_no_div_sp = ["forex_prices", "futures_prices", "crypto_prices"]
            cols_to_drop_from_no_div_sp = ["dividends", "stock_splits"]
            cols_to_drop = (
                cols_to_drop_from_no_div_sp
                if any(self.name.startswith(prefix) for prefix in prefixes_no_div_sp)
                else []
            )

            if cols_to_drop:
                df = df.drop(columns=cols_to_drop, axis=1, errors="ignore")

            df.columns = clean_strings(df.columns)
            check_missing_columns(
                df, self.column_order, method, ignore_cols={"capital_gains"}
            )
            df = df[[i for i in self.column_order if i in df.columns]]
            return df

        except Exception as e:
            logging.error(f"Error for ticker {ticker} failed with error: {e}")
            self.failed_ticker_downloads[yf_params["interval"]].append(ticker)
            return pd.DataFrame(columns=self.column_order)

    def download_price_history_wide(self, tickers, yf_params):
        method = get_method_name()
        logging.info(f"Running {method} for ticker {tickers}")
        yf_params = self.yf_params.copy() if yf_params is None else yf_params.copy()

        assert (
            "interval" in yf_params.keys()
        ), "must pass interval parameter to yf_params"

        if yf_params["interval"] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_params["interval"]] = []

        if "start" not in yf_params.keys():
            yf_params["start"] = "1950-01-01 00:00:00"
            logging.info(
                f"\n*** YF params start set to 1950-01-01 for tickers {tickers}! ***\n"
            )

        yf_params["start"] = get_valid_yfinance_start_timestamp(
            interval=yf_params["interval"], start=yf_params["start"]
        )

        yf.pdr_override()

        df = (
            pdr.get_data_yahoo(tickers, progress=False, **yf_params)
            .rename_axis(index="timestamp")
            .reset_index()
        )
        self.n_requests += 1

        df.columns = flatten_multindex_columns(df)
        df.loc[:, "timestamp_tz_aware"] = df["timestamp"].copy()
        df.loc[:, "timezone"] = str(df["timestamp_tz_aware"].dt.tz)
        df["timestamp_tz_aware"] = df["timestamp_tz_aware"].dt.strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        if df is not None and not df.shape[0]:
            self.failed_ticker_downloads[yf_params["interval"]].append(tickers)
            return pd.DataFrame(columns=self.column_order)
        df = fix_empty_values(df)
        return df


class TickerDownloader:
    """
    Downloads and caches Yahoo tickers in memory for the duration of a Meltano tap run.
    ENSURES no duplicates and stops pagination when tickers repeat.
    """

    _memory_cache = {}
    _cache_lock = threading.Lock()

    @classmethod
    def download_yahoo_tickers(cls, segment, paginate_records=200, max_pages=50000):
        url_map = {
            "world_indices_tickers": "https://finance.yahoo.com/markets/world-indices/",
            "futures_tickers": "https://finance.yahoo.com/markets/commodities/",
            "bonds_tickers": "https://finance.yahoo.com/markets/bonds/",
            "forex_tickers": "https://finance.yahoo.com/markets/currencies/",
            "options_tickers": "https://finance.yahoo.com/markets/options/most-active/?start={start}&count={count}",
            "stock_tickers": "https://finance.yahoo.com/markets/stocks/most-active/?start={start}&count={count}",
            "crypto_tickers": "https://finance.yahoo.com/markets/crypto/all/?start={start}&count={count}",
            "private_companies_tickers": "https://finance.yahoo.com/markets/private-companies/highest-valuation/",
            "etf_tickers": "https://finance.yahoo.com/markets/etfs/most-active/",
            "mutual_fund_tickers": "https://finance.yahoo.com/markets/mutualfunds/gainers/?start={start}&count={count}",
        }
        default_url_map = {
            "world_indices_tickers": "https://finance.yahoo.com/markets/world-indices/",
            "futures_tickers": "https://finance.yahoo.com/markets/commodities/",
            "bonds_tickers": "https://finance.yahoo.com/markets/bonds/",
            "forex_tickers": "https://finance.yahoo.com/markets/currencies/",
            "options_tickers": "https://finance.yahoo.com/markets/options/most-active/",
            "stock_tickers": "https://finance.yahoo.com/markets/stocks/most-active/",
            "crypto_tickers": "https://finance.yahoo.com/markets/crypto/all/",
            "private_companies_tickers": "https://finance.yahoo.com/markets/private-companies/highest-valuation/",
            "etf_tickers": "https://finance.yahoo.com/markets/etfs/most-active/?start={start}&count={count}",
            "mutual_fund_tickers": "https://finance.yahoo.com/markets/mutualfunds/",
        }

        if segment not in url_map or segment not in default_url_map:
            raise Exception(f"Unknown segment: {segment}")

        with cls._cache_lock:
            if segment in cls._memory_cache:
                return cls._memory_cache[segment]

        base_url = url_map[segment]
        first_url = default_url_map[segment]
        paginate = "{start}" in base_url
        key_columns = ["symbol", "name"]

        session = HTMLSession()

        if not paginate:
            resp = session.get(first_url)
            tables = pd.read_html(resp.html.raw_html)
            if not tables:
                session.close()
                raise Exception(f"No tables found for {segment}")
            df = tables[0]
            df.columns = [str(x).strip().lower() for x in df.columns]
            if segment == "private_companies_tickers":
                df = df.rename(columns={"company": "name"})
            if not all(col in df.columns for col in key_columns):
                session.close()
                raise Exception(
                    f"Expected columns {key_columns} not found for tickers {segment}"
                )
            df = df[key_columns].rename(columns={"symbol": "ticker"})
            df = df.drop_duplicates(subset=["ticker"])
            df = df.reset_index(drop=True)
            df = fix_empty_values(df)
            df = df.dropna(how="all", axis=1)
            df = df.dropna(how="all", axis=0)
            session.close()

            with cls._cache_lock:
                cls._memory_cache[segment] = df
            return df

        # --- PAGINATION ---
        all_dfs = []
        seen_tickers = set()
        start = 0
        page = 0
        while page < max_pages:
            url = base_url.format(start=start, count=paginate_records)
            resp = session.get(url)
            tables = pd.read_html(resp.html.raw_html)
            if not tables:
                break
            df = tables[0]
            df.columns = [str(x).strip().lower() for x in df.columns]
            if not all(col in df.columns for col in key_columns):
                break
            df = df[key_columns]
            # Filter out tickers already seen
            df = df[~df["symbol"].astype(str).isin(seen_tickers)]
            if df.empty:
                break
            all_dfs.append(df)
            # Add the tickers from this page to seen_tickers
            seen_tickers.update(df["symbol"].astype(str))
            # If the number of unique tickers on this page is less than paginate_records, stop (last page)
            if len(df) < paginate_records:
                break
            start += paginate_records
            page += 1
        session.close()
        if not all_dfs:
            raise Exception(f"No data found for segment: {segment}")

        df_final = pd.concat(all_dfs, ignore_index=True)
        df_final = df_final.drop_duplicates(subset=["symbol"])
        df_final = df_final.rename(columns={"symbol": "ticker"})
        df_final = df_final.reset_index(drop=True)
        df_final = fix_empty_values(df_final)
        df_final = df_final.dropna(how="all", axis=1)
        df_final = df_final.dropna(how="all", axis=0)
        with cls._cache_lock:
            cls._memory_cache[segment] = df_final
        return df_final

    @staticmethod
    def download_pts_stock_tickers():
        """
        Description
        -----------
        Download py-ticker-symbols tickers
        """
        method = get_method_name()
        logging.info(f"Running method {method}")
        pts = PyTickerSymbols()
        all_getters = list(
            filter(
                lambda x: (
                    x.endswith("_yahoo_tickers") or x.endswith("_google_tickers")
                ),
                dir(pts),
            )
        )

        all_tickers = {"yahoo_tickers": [], "google_tickers": []}
        for t in all_getters:
            if t.endswith("google_tickers"):
                all_tickers["google_tickers"].append((getattr(pts, t)()))
            elif t.endswith("yahoo_tickers"):
                all_tickers["yahoo_tickers"].append((getattr(pts, t)()))
        all_tickers["google_tickers"] = flatten_list(all_tickers["google_tickers"])
        all_tickers["yahoo_tickers"] = flatten_list(all_tickers["yahoo_tickers"])
        if len(all_tickers["yahoo_tickers"]) == len(all_tickers["google_tickers"]):
            all_tickers = pd.DataFrame(all_tickers)
        else:
            all_tickers = pd.DataFrame(
                dict([(k, pd.Series(v)) for k, v in all_tickers.items()])
            )

        all_tickers = (
            all_tickers.rename(
                columns={
                    "yahoo_tickers": "yahoo_ticker",
                    "google_tickers": "google_ticker",
                }
            )
            .sort_values(by=["yahoo_ticker", "google_ticker"])
            .drop_duplicates()
        )
        all_tickers = replace_all_specified_missing(all_tickers)
        all_tickers = all_tickers.replace([-np.inf, np.inf, np.nan], None)
        all_tickers.columns = ["yahoo_ticker_pts", "google_ticker_pts"]
        return all_tickers

    # def generate_yahoo_sec_tickermap(self):
    #     method = get_method_name()
    #     logging.info(f"Running method {method}")
    #     df_pts_tickers = self.download_pts_stock_tickers()
    #     df_mapped = pd.merge(
    #         df_sec_tickers,
    #         df_pts_tickers,
    #         left_on="sec_ticker",
    #         right_on="yahoo_ticker_pts",
    #         how="outer",
    #     )
    #     df_mapped["ticker"] = df_mapped["sec_ticker"].fillna(
    #         df_mapped["yahoo_ticker_pts"]
    #     )  # likely yahoo ticker
    #     df_mapped = df_mapped.replace([np.inf, -np.inf, np.nan], None)
    #     return df_mapped


def get_valid_yfinance_start_timestamp(interval, start="1950-01-01 00:00:00"):
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

    valid_intervals = [
        "1m",
        "2m",
        "5m",
        "15m",
        "30m",
        "60m",
        "1h",
        "90m",
        "1d",
        "5d",
        "1wk",
        "1mo",
        "3mo",
    ]
    assert interval in valid_intervals, f"must pass a valid interval {valid_intervals}"

    if interval == "1m":
        updated_start = max(
            (datetime.today() - timedelta(days=5)).date(), pd.to_datetime(start).date()
        )
    elif interval in ["2m", "5m", "15m", "30m", "90m"]:
        updated_start = max(
            (datetime.today() - timedelta(days=58)).date(), pd.to_datetime(start).date()
        )
    elif interval in ["60m", "1h"]:
        updated_start = max(
            (datetime.today() - timedelta(days=728)).date(),
            pd.to_datetime(start).date(),
        )
    else:
        updated_start = pd.to_datetime(start)

    updated_start = updated_start.strftime(
        "%Y-%m-%d"
    )  # yfinance doesn't like strftime with hours, minutes, or seconds

    return updated_start


def flatten_list(lst):
    return [v for item in lst for v in (item if isinstance(item, list) else [item])]


def clean_strings(lst):
    cleaned_list = [
        re.sub(r"[^a-zA-Z0-9_]", "_", s) for s in lst
    ]  # remove special characters
    cleaned_list = [
        re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower() for s in cleaned_list
    ]  # camel case -> snake case
    cleaned_list = [
        re.sub(r"_+", "_", s).strip("_").lower() for s in cleaned_list
    ]  # clean leading and trailing underscores
    return cleaned_list


def flatten_multindex_columns(df):
    new_cols = list(
        clean_strings(
            pd.Index(
                [
                    str(e[0]).lower() + "_" + str(e[1]).lower()
                    for e in df.columns.tolist()
                ]
            )
        )
    )
    return new_cols


def replace_all_specified_missing(df, exclude_columns=None):
    """
    Replaces entire string values equal to 'nan' (case-insensitive),
    'none' (case-insensitive), and the Python None object with np.nan
    in a Pandas DataFrame.

    Args:
        df (pd.DataFrame): The input Pandas DataFrame.
        exclude_columns (list, optional): List of columns to exclude. Defaults to None.

    Returns:
        pd.DataFrame: The DataFrame with specified missing values replaced by np.nan.
    """

    if exclude_columns is None:
        exclude_columns = []

    def replace_in_series(series):
        if series.name in exclude_columns:
            return series
        if series.dtype == "object":
            coerced = series.astype(str).str.lower()
            return series.where(
                (coerced != "nan") & (coerced != "none") & (series.notna()),
                np.nan,
            )
        else:
            return series.where(series.notna(), np.nan)

    return df.apply(replace_in_series)


def check_missing_columns(df, column_order, stream_name, ignore_cols=set()):
    df_columns = set(df.columns)
    expected_columns = set(column_order)
    missing_in_df = expected_columns - df_columns - ignore_cols
    missing_in_schema = df_columns - expected_columns - ignore_cols

    if missing_in_df or missing_in_schema:
        missing_in_df_msg = (
            f"*** MISSING EXPECTED COLUMNS IN DF: {missing_in_df} ***"
            if missing_in_df
            else ""
        )
        missing_in_schema_msg = (
            f"*** URGENT!!! MISSING EXPECTED COLUMNS IN SCHEMA: {missing_in_schema} ***"
            if missing_in_schema
            else ""
        )
        warning_message = (
            f"*** For stream_name {stream_name} and ticker {df['ticker'].iloc[0]} *** "
            + " ".join(filter(None, [missing_in_df_msg, missing_in_schema_msg]))
        )
        logging.warning(warning_message)

    return {"missing_in_df": missing_in_df, "missing_in_schema": missing_in_schema}


def get_method_name():
    return inspect.currentframe().f_back.f_code.co_name


def fix_empty_values(df):
    df = df.replace([np.inf, -np.inf, np.nan], None)

    # need to replace string versions of nan and inf/-inf safely without changing values from the previous step
    regex_pattern = r"(?i)^(nan|none|infinity)$"
    mask_to_replace = df.map(
        lambda x: isinstance(x, str) and re.fullmatch(regex_pattern, x) is not None
    )
    df = df.mask(mask_to_replace, None)

    df.replace(r"(?i)^(nan|none|infinity)$", None, regex=True)
    return df
