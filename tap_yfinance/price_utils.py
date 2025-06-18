import hashlib
import logging
import threading
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr
from pytickersymbols import PyTickerSymbols
from requests_html import HTMLSession
from yfinance.exceptions import YFRateLimitError

from tap_yfinance.helpers import (
    check_missing_columns,
    clean_strings,
    fix_empty_values,
    flatten_list,
    flatten_multindex_columns,
    get_method_name,
    get_valid_yfinance_start_timestamp,
    rate_limiter,
    yfinance_backoff,
)

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

    @yfinance_backoff
    def fetch_price_history(self, ticker, yf_params=None) -> pd.DataFrame():
        """
        Description
        -----------
        Fetch a single stock price ticker from the yfinance python library.
        Minor transformations happen:
            - Add column ticker to show which ticker has been pulled
            - Set start date to the minimum start date allowed by yfinance for that ticker (passed in yf_params)
            - Clean column names
            - Set tz_aware timestamp column to be a string
        """
        method = get_method_name()
        yf_params = self.yf_params.copy() if yf_params is None else yf_params.copy()

        rate_limiter.wait_if_needed(method)

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

            df = fix_empty_values(df, exclude_columns=["ticker"])
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
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(f"Error for ticker {ticker} failed with error: {e}")
            self.failed_ticker_downloads[yf_params["interval"]].append(ticker)
            return pd.DataFrame(columns=self.column_order)

    @yfinance_backoff
    def fetch_price_history_wide(self, tickers, yf_params):
        try:
            method = get_method_name()
            logging.info(f"Running {method} for ticker {tickers}")

            rate_limiter.wait_if_needed(method)

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
            df = fix_empty_values(df, exclude_columns=["ticker"])
            return df
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {tickers}, will retry: {e}")
            raise


class TickerFetcher:
    """
    Fetch and caches Yahoo tickers in memory for the duration of a Meltano tap run.
    ENSURES no duplicates and stops pagination when tickers repeat.
    """

    _memory_cache = {}
    _cache_lock = threading.Lock()

    def fetch_all_tickers(self) -> list[dict]:
        """
        Fetch all tickers from both Yahoo segments and PyTickerSymbols.
        Returns a list of ticker dictionaries - matches tap-yfinance behavior.
        """
        all_dfs = []

        yahoo_segments = [
            "stock_tickers",
            "crypto_tickers",
            "forex_tickers",
            "etf_tickers",
            "mutual_fund_tickers",
            "world_indices_tickers",
            "futures_tickers",
            "bonds_tickers",
            "options_tickers",
            "private_companies_tickers",
        ]

        for segment in yahoo_segments:
            logging.info(f"Pulling {segment} tickers for tickers stream.")
            try:
                df = self.fetch_yahoo_tickers(segment)
                if "segment" not in df.columns:
                    df["segment"] = segment
                df = df[["ticker", "name", "segment"]].drop_duplicates()

                df = fix_empty_values(df, exclude_columns=["ticker"])
                df["ticker"] = df["ticker"].astype(str)
                all_dfs.append(df)
            except Exception as e:
                logging.warning(f"Could not fetch {segment}: {e}")
                continue

        logging.info("Pulling pts_tickers for tickers stream.")
        try:
            df = self.fetch_pts_tickers()
            df["segment"] = "pts_tickers"

            if "ticker" not in df.columns and "yahoo_ticker" in df.columns:
                df = df.rename(columns={"yahoo_ticker": "ticker"})

            df = df[["ticker", "name", "segment"]].drop_duplicates(
                subset=["ticker", "segment"]
            )
            df = fix_empty_values(df, exclude_columns=["ticker"])
            df["ticker"] = df["ticker"].astype(str)
            all_dfs.append(df)
        except Exception as e:
            logging.warning(f"Could not fetch pts_tickers: {e}")

        if all_dfs:
            df_all_tickers = pd.concat(all_dfs, ignore_index=True)
            df_all_tickers = df_all_tickers.drop_duplicates(subset=["ticker"])
            return df_all_tickers.to_dict("records")
        else:
            logging.warning("No tickers were successfully fetched from any source")
            return []

    def fetch_specific_tickers(self, ticker_list: list[str]) -> list[dict]:
        """
        Create ticker records for a specific list of tickers.
        """
        return [
            {
                "ticker": ticker.upper(),
                "name": None,
                "segment": self._guess_segment(ticker),
            }
            for ticker in ticker_list
        ]

    @staticmethod
    def _guess_segment(ticker: str) -> str:
        """Guess the segment for a ticker based on its format."""
        if isinstance(ticker, str):
            if ticker.endswith("=X"):
                return "forex_tickers"
            elif ticker.endswith("=F"):
                return "futures_tickers"
            elif "-" in ticker and ticker.endswith("USD"):
                return "crypto_tickers"
            elif ticker.isupper() and len(ticker) <= 5:
                return "stock_tickers"
            elif ".PVT" in ticker:
                return "private_companies_tickers"
            elif ticker.startswith("^"):
                return "world_indices_tickers"
        return "unknown"

    @classmethod
    def fetch_yahoo_tickers(cls, segment, paginate_records=200, max_pages=50000):
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
        session.timeout = 60

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

            mask_nan_both = df["ticker"].isna() & df["name"].isna()
            if mask_nan_both.any():
                df.loc[mask_nan_both, :] = fix_empty_values(df.loc[mask_nan_both, :])
            df = df.dropna(how="all", axis=1)
            df = df.dropna(how="all", axis=0)
            session.close()
            df[["ticker", "name"]] = df[["ticker", "name"]].astype(str)
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
        mask_nan_both = df_final["ticker"].isna() & df_final["name"].isna()
        if mask_nan_both.any():
            df_final.loc[mask_nan_both, :] = fix_empty_values(
                df_final.loc[mask_nan_both, :]
            )
        df_final = df_final.dropna(how="all", axis=1)
        df_final = df_final.dropna(how="all", axis=0)
        df_final[["ticker", "name"]] = df_final[["ticker", "name"]].astype(str)
        with cls._cache_lock:
            cls._memory_cache[segment] = df_final
        return df_final

    @staticmethod
    def fetch_pts_tickers():
        """
        Description
        -----------
        Fetch py-ticker-symbols tickers
        """
        logging.info("Pulling PTS tickers.")
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
        all_tickers = fix_empty_values(all_tickers)
        all_tickers = all_tickers.replace([-np.inf, np.inf, np.nan], None)
        all_tickers.columns = ["yahoo_ticker", "google_ticker"]

        all_stocks = pts.get_all_stocks()
        df_all_stocks = pd.json_normalize(
            all_stocks,
            record_path=["symbols"],
            meta=[
                "name",
                "symbol",
                "country",
                "indices",
                "industries",
                "isins",
                "akas",
                ["metadata", "founded"],
                ["metadata", "employees"],
            ],
            errors="ignore",
        )
        df_all_stocks = df_all_stocks.rename(
            columns={"metadata.founded": "founded", "metadata.employees": "employees"}
        ).rename(columns={"yahoo": "yahoo_ticker", "google": "google_ticker"})
        df_all_stocks["segment"] = "stocks"

        all_indices = pts.get_all_indices()
        df_all_indices = pd.DataFrame({"ticker": all_indices, "name": None})
        df_all_indices["segment"] = "indices"

        industries = pts.get_all_industries()
        df_all_industries = pd.DataFrame({"ticker": None, "name": industries})
        df_all_industries["segment"] = "industries"

        countries = pts.get_all_countries()
        df_countries = pd.DataFrame({"ticker": None, "name": countries})
        df_countries["segment"] = "countries"

        df_final = pd.concat(
            [
                all_tickers,
                df_all_stocks,
                df_all_indices,
                df_all_industries,
                df_countries,
            ],
            ignore_index=True,
        )

        df_final["ticker"] = (
            df_final["ticker"]
            .fillna(df_final["yahoo_ticker"])
            .fillna(df_final["google_ticker"])
        )
        df_final = df_final.dropna(how="all", axis=1)
        df_final = df_final.dropna(how="all", axis=0)
        df_final = fix_empty_values(df_final)
        list_cols = ["indices", "industries", "isins", "akas"]
        for col in list_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: tuple(x) if isinstance(x, list) else x
                )

        df_final["surrogate_key"] = df_final.apply(
            lambda x: hashlib.sha256(
                "".join(str(x) for x in x.values).encode("utf-8")
            ).hexdigest(),
            axis=1,
        )

        df_final[["employees", "founded"]] = df_final[["employees", "founded"]].astype(
            str
        )  # ensure no issues with singer schema
        return df_final
