import hashlib
import logging
import re
from datetime import datetime

import backoff
import numpy as np
import pandas as pd
import yfinance as yf
from requests.exceptions import RequestException
from urllib3.exceptions import MaxRetryError, NewConnectionError
from yfinance.exceptions import YFRateLimitError

from tap_yfinance.expected_schema import *
from tap_yfinance.price_utils import (
    check_missing_columns,
    clean_strings,
    fix_empty_values,
    get_method_name,
)

pd.set_option("future.no_silent_downcasting", True)


class FinancialTap:

    ### TODO: date filters? ###

    def __init__(
        self, schema, ticker, name, config=None, yf_params=None, ticker_colname="ticker"
    ):
        self.schema = schema
        self.ticker = ticker
        self.name = name
        self.config = config
        self.yf_params = {} if yf_params is None else yf_params
        self.ticker_colname = ticker_colname

        super().__init__()

        if self.config is not None and "yf_cache_params" in self.config.get(self.name):
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
                backend=SQLiteCache("yfinance.cache"),
            )

            self.yf_ticker_obj = yf.Ticker(self.ticker, session=self.session)

        else:
            self.yf_ticker_obj = yf.Ticker(self.ticker)

    @staticmethod
    def extract_ticker_tz_aware_timestamp(df, timestamp_column, ticker):
        """transformations are applied inplace to reduce memory usage"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        assert (
            "timezone" not in df.columns
        ), f"timezone cannot be a pre-existing column in the extracted df for ticker {ticker}."
        df["ticker"] = ticker
        df.columns = clean_strings(df.columns)
        df["timezone"] = str(df[timestamp_column].dt.tz)
        df.loc[:, f"{timestamp_column}_tz_aware"] = (
            df[timestamp_column].copy().dt.strftime("%Y-%m-%d %H:%M:%S%z")
        )
        df[timestamp_column] = pd.to_datetime(df[timestamp_column], utc=True)
        return df

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_analyst_price_targets(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            data = self.yf_ticker_obj.get_analyst_price_targets()
            if isinstance(data, dict) and len(data):
                df = pd.DataFrame.from_dict(data, orient="index").T
                df["timestamp_extracted"] = datetime.utcnow()
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                column_order = [
                    "timestamp_extracted",
                    "ticker",
                    "current",
                    "high",
                    "low",
                    "mean",
                    "median",
                    "surrogate_key",
                ]
                df["surrogate_key"] = df.apply(
                    lambda x: hashlib.sha256(
                        "".join(str(x) for x in x.values).encode("utf-8")
                    ).hexdigest(),
                    axis=1,
                )
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except Exception as e:
            logging.error(
                f"Error extracting data {method} for ticker {ticker}. Failed with error {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted", "ticker"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_actions(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_actions()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.reset_index().rename(columns={"Date": "timestamp"})
                self.extract_ticker_tz_aware_timestamp(df, "timestamp", ticker)
                df = fix_empty_values(df, exclude_columns=["ticker"])
                column_order = [
                    "timestamp",
                    "timestamp_tz_aware",
                    "timezone",
                    "ticker",
                    "dividends",
                    "stock_splits",
                ]
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_balance_sheet(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_balance_sheet()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = clean_strings(df.columns)
                df = df.rename(
                    columns={
                        # fmt: off
                        "financial_assets_designatedas_fair_value_through_profitor_loss_total":
                            "financial_assets_designated_as_fv_thru_profitor_loss_total"
                        # fmt: on
                    }
                )
                df.columns = [i.replace("p_p_e", "ppe") for i in df.columns]
                column_order = BALANCE_SHEET_COLUMNS
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_balancesheet(self, ticker):
        """Same output as the method get_balance_sheet"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def basic_info(self, ticker):
        """Useless information"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_calendar(self, ticker):
        """Returns calendar df"""

        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            data = self.yf_ticker_obj.get_calendar()
            if isinstance(data, pd.DataFrame) and datashape[0]:
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = clean_strings(df.columns)
                df["ticker"] = ticker
                column_order = [
                    "dividend_date",
                    "ex_dividend_date",
                    "earnings_date",
                    "ticker",
                    "earnings_high",
                    "earnings_low",
                    "earnings_average",
                    "revenue_high",
                    "revenue_low",
                    "revenue_average",
                    "surrogate_key",
                ]
                check_missing_columns(df, column_order, method)
                df[["dividend_date", "ex_dividend_date", "earnings_date"]] = df[
                    ["dividend_date", "ex_dividend_date", "earnings_date"]
                ].dt.strftime("%Y-%m-%d")
                df["surrogate_key"] = df.apply(
                    lambda x: hashlib.sha256(
                        "".join(str(x) for x in x.values).encode("utf-8")
                    ).hexdigest(),
                    axis=1,
                )
                return df[[i for i in column_order if i in df.columns]]
            elif isinstance(data, dict):
                try:
                    df = pd.DataFrame.from_dict(data)
                except ValueError:
                    df = pd.DataFrame.from_dict(data, orient="index")
                except Exception as e:
                    raise e
                for col in ["dividend_date", "ex_dividend_date", "earnings_date"]:
                    if col in df.columns:
                        df[col] = df[col].dt.strftime("%Y-%m-%d")
                df["surrogate_key"] = df.apply(
                    lambda x: hashlib.sha256(
                        "".join(str(x) for x in x.values).encode("utf-8")
                    ).hexdigest(),
                    axis=1,
                )
                return df
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["surrogate_key"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(
                columns=["dividend_date", "ex_dividend_date", "earnings_date"]
            )

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_capital_gains(self, ticker):
        """Returns empty series"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_cash_flow(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_cash_flow()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df.columns = clean_strings(df.columns)
                df.columns = [
                    i.replace("p_p_e", "ppe")
                    .replace("c_f_o", "cfo")
                    .replace("c_f_f", "cff")
                    .replace("c_f_i", "cfi")
                    for i in df.columns
                ]
                df = fix_empty_values(df, exclude_columns=["ticker"])
                column_order = CASH_FLOW_COLUMNS
                check_missing_columns(df, column_order, method)
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_cashflow(self, ticker):
        """Same output as the method get_cash_flow"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_dividends(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_dividends()
            if isinstance(df, pd.Series) and df.shape[0]:
                df = df.rename_axis("timestamp").reset_index()
                df["ticker"] = ticker
                self.extract_ticker_tz_aware_timestamp(df, "timestamp", ticker)
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = clean_strings(df.columns)
                column_order = [
                    "timestamp",
                    "timestamp_tz_aware",
                    "timezone",
                    "ticker",
                    "dividends",
                ]
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp"])

    @backoff.on_exception(
        backoff.expo,
        YFRateLimitError,
        max_tries=5,
        max_time=5000,
        jitter=backoff.full_jitter,
    )
    def get_earnings(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_earnings_estimate(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_earnings_estimate()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.reset_index()
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = clean_strings(df.columns)
                column_order = [
                    "timestamp_extracted",
                    "ticker",
                    "period",
                    "avg",
                    "low",
                    "high",
                    "year_ago_eps",
                    "number_of_analysts",
                    "growth",
                ]
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_earnings_history(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_earnings_history()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                possible_columns1 = [
                    "quarter",
                    "eps_estimate",
                    "ticker",
                    "timestamp_extracted",
                ]
                possible_columns2 = [
                    "quarter",
                    "eps_actual",
                    "eps_estimate",
                    "eps_difference",
                    "surprise_percent",
                    "ticker",
                    "timestamp_extracted",
                ]
                df = df.reset_index()
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = clean_strings(df.columns)
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                if len(df.columns) == len(possible_columns1) and all(
                    df.columns == possible_columns1
                ):
                    column_order = [
                        "quarter",
                        "ticker",
                        "eps_estimate",
                        "timestamp_extracted",
                    ]
                elif len(df.columns) == len(possible_columns2) and all(
                    df.columns == possible_columns2
                ):
                    column_order = [
                        "quarter",
                        "ticker",
                        "eps_estimate",
                        "eps_actual",
                        "eps_difference",
                        "surprise_percent",
                        "timestamp_extracted",
                    ]
                else:
                    raise ValueError(
                        f"Error validating returned columns for earnings_history with ticker {ticker}."
                    )
                df["quarter"] = df["quarter"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted", "quarter"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted", "quarter"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_earnings_dates(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_earnings_dates()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.rename_axis("timestamp").reset_index()
                df["ticker"] = ticker
                self.extract_ticker_tz_aware_timestamp(df, "timestamp", ticker)
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("e_p_s", "eps") for i in clean_strings(df.columns)
                ]
                df.rename(columns={"surprise": "pct_surprise"}, inplace=True)
                column_order = [
                    "timestamp",
                    "timestamp_tz_aware",
                    "timezone",
                    "ticker",
                    "eps_estimate",
                    "reported_eps",
                    "pct_surprise",
                ]
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_earnings_forecast(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_earnings_trend(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_eps_revisions(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_eps_revisions()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.reset_index()
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("last7", "last_7")
                    .replace("7d", "7_d")
                    .replace("7D", "7_d")
                    for i in clean_strings(df.columns)
                ]
                df.columns = [
                    i.replace("last30", "last_30")
                    .replace("30d", "30_d")
                    .replace("30D", "30_d")
                    for i in clean_strings(df.columns)
                ]
                column_order = [
                    "timestamp_extracted",
                    "ticker",
                    "period",
                    "up_last_7_days",
                    "down_last_7_days",
                    "up_last_30_days",
                    "down_last_30_days",
                ]

                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_eps_trend(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_eps_trend()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.reset_index()
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = clean_strings([rename_days_ago(col) for col in df.columns])
                column_order = [
                    "timestamp_extracted",
                    "ticker",
                    "period",
                    "current",
                    "days_ago_7",
                    "days_ago_30",
                    "days_ago_60",
                    "days_ago_90",
                ]
                check_missing_columns(df, column_order, method)
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted", "ticker"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted", "ticker"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_funds_data(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        pass

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_growth_estimates(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_growth_estimates()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.reset_index()
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = clean_strings([rename_days_ago(col) for col in df.columns])
                column_order = [
                    "ticker",
                    "period",
                    "stock_trend",
                    "index_trend",
                    "timestamp_extracted",
                ]
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_fast_info(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = pd.DataFrame.from_dict(
                dict(self.yf_ticker_obj.get_fast_info()), orient="index"
            ).T
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df.rename(columns={"timezone": "extracted_timezone"}, inplace=True)

                df["timestamp_tz_aware"] = df.apply(
                    lambda row: row["timestamp_extracted"].tz_localize(
                        row["extracted_timezone"]
                    ),
                    axis=1,
                )

                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = clean_strings(df.columns)

                column_order = [
                    "currency",
                    "day_high",
                    "day_low",
                    "exchange",
                    "fifty_day_average",
                    "last_price",
                    "last_volume",
                    "market_cap",
                    "open",
                    "previous_close",
                    "quote_type",
                    "regular_market_previous_close",
                    "shares",
                    "ten_day_average_volume",
                    "three_month_average_volume",
                    "extracted_timezone",
                    "two_hundred_day_average",
                    "year_change",
                    "year_high",
                    "year_low",
                    "ticker",
                    "timestamp_extracted",
                    "timestamp_tz_aware",
                ]

                check_missing_columns(df, column_order, method)
                return df
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_financials(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_financials().T
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("e_b_i_t_d_a", "ebitda")
                    .replace("e_p_s", "eps")
                    .replace("e_b_i_t", "ebit")
                    .replace("p_p_e", "ppe")
                    .replace(
                        "diluted_n_i_availto_com_stockholders",
                        "diluted_ni_availto_com_stockholders",
                    )
                    for i in clean_strings(df.columns)
                ]
                column_order = FINANCIAL_COLUMNS
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_history_metadata(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            data = self.yf_ticker_obj.get_history_metadata()
            if len(data):
                try:
                    if "tradingPeriods" in data.keys():
                        data["tradingPeriods"] = data["tradingPeriods"].to_dict()

                    df = pd.Series({key: data[key] for key in data.keys()}).to_frame().T
                    df.columns = clean_strings(df.columns)
                    df = df.rename(columns={"symbol": "ticker"})
                    df = fix_empty_values(df, exclude_columns=["ticker"])

                    if "current_trading_period" in df.columns:
                        df_ctp = pd.json_normalize(df["current_trading_period"])
                        df_ctp.columns = clean_strings(df_ctp.columns)
                        df_ctp = df_ctp.add_prefix("current_trading_period_")
                        df = pd.concat([df, df_ctp], axis=1)
                        df = df.drop(["current_trading_period"], axis=1)

                    if "trading_periods" in df.columns:
                        df_tp = pd.DataFrame().from_dict(df["trading_periods"].iloc[0])
                        df_tp = df_tp.add_prefix("trading_period_").reset_index(
                            drop=True
                        )
                        df = pd.concat([df, df_tp], axis=1).ffill()
                        df = df.drop("trading_periods", axis=1)

                    df["timestamp_extracted"] = datetime.utcnow()

                    column_order = HISTORY_METADATA_COLUMNS
                    check_missing_columns(df, column_order, method)
                    if "last_trade" in df.columns:
                        df["last_trade"] = df["last_trade"].astype(str)
                    return df[[i for i in column_order if i in df.columns]]
                except Exception as e:
                    logging.error(
                        f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
                    )
                    return pd.DataFrame(columns=["timestamp_extracted"])
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_info(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            data = self.yf_ticker_obj.get_info()
            if len(data):
                try:
                    df = pd.DataFrame.from_dict(data, orient="index").T
                    df.columns = clean_strings(df.columns)
                    df["timestamp_extracted"] = datetime.utcnow()
                    df["ticker"] = ticker
                    df = df.rename(
                        columns={
                            "52_week_change": "change_52wk",
                            "forward_p_e": "forward_pe",
                            "trailing_p_e": "trailing_pe",
                        }
                    )

                    df = fix_empty_values(df, exclude_columns=["ticker"])
                    column_order = INFO_COLUMNS
                    check_missing_columns(df, column_order, method)

                    str_cols = [
                        "ticker",
                        "address1",
                        "city",
                        "state",
                        "zip",
                        "country",
                        "phone",
                        "website",
                        "industry",
                        "industry_key",
                        "industry_disp",
                        "sector",
                        "sector_key",
                        "sector_disp",
                        "long_business_summary",
                        "company_officers",
                        "ir_website",
                        "executive_team",
                        "currency",
                        "last_split_factor",
                        "quote_type",
                        "recommendation_key",
                        "financial_currency",
                        "symbol",
                        "language",
                        "region",
                        "type_disp",
                        "quote_source_name",
                        "custom_price_alert_confidence",
                        "corporate_actions",
                        "post_market_time",
                        "regular_market_time",
                        "exchange",
                        "message_board_id",
                        "exchange_timezone_name",
                        "exchange_timezone_short_name",
                        "market",
                        "average_analyst_rating",
                        "regular_market_day_range",
                        "full_exchange_name",
                        "fifty_two_week_range",
                        "short_name",
                        "long_name",
                        "display_name",
                        "fax",
                        "uuid",
                        "underlying_symbol",
                    ]

                    str_cols = [i for i in str_cols if i in df.columns]
                    df[str_cols] = df[str_cols].astype(str)

                    return df[[i for i in column_order if i in df.columns]]
                except Exception as e:
                    logging.error(
                        f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
                    )
                    return pd.DataFrame(columns=["timestamp_extracted"])
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_income_stmt(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_income_stmt()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("e_b_i_t_d_a", "ebitda")
                    .replace("e_p_s", "eps")
                    .replace("e_b_i_t", "ebit")
                    .replace(
                        "diluted_n_i_availto_com_stockholders",
                        "diluted_ni_availto_com_stockholders",
                    )
                    .replace("p_p_e", "ppe")
                    for i in clean_strings(df.columns)
                ]
                column_order = INCOME_STMT_COLUMNS
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_incomestmt(self, ticker):
        """Same output as the method get_income_stmt"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_insider_purchases(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        column_order = [
            "ticker",
            "insider_purchases_last",
            "insider_purchases_last_6m",
            "shares",
            "trans",
            "timestamp_extracted",
        ]
        num_cols = ["shares", "trans", "insider_purchases_last"]
        try:
            df = self.yf_ticker_obj.get_insider_purchases()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df["timestamp_extracted"] = datetime.now()
                df.columns = clean_strings(df.columns)
                df["ticker"] = ticker
                df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
                df = fix_empty_values(df, exclude_columns=["ticker"])
                check_missing_columns(df, column_order, method)
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=column_order)

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_insider_roster_holders(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running method {method} for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_insider_roster_holders()
            if isinstance(df, pd.DataFrame) and len(df):
                df.columns = [
                    i.replace("u_r_l", "url") for i in clean_strings(df.columns)
                ]
                df["ticker"] = ticker
                column_order = [
                    "latest_transaction_date",
                    "ticker",
                    "name",
                    "position",
                    "url",
                    "most_recent_transaction",
                    "shares_owned_indirectly",
                    "position_indirect_date",
                    "shares_owned_directly",
                    "position_direct_date",
                    "position_summary",
                    "position_summary_date",
                ]

                check_missing_columns(df, column_order, method)

                abnormal_cols = ["position_summary", "position_summary_date"]
                df.loc[:, df.columns.intersection(abnormal_cols)] = df[
                    df.columns.intersection(abnormal_cols)
                ].apply(pd.to_numeric, errors="coerce")
                df = fix_empty_values(df, exclude_columns=["ticker"])
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame()
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame()

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_insider_transactions(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running method {method} for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_insider_transactions()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("u_r_l", "url") for i in clean_strings(df.columns)
                ]
                df["ticker"] = ticker
                column_order = [
                    "ticker",
                    "start_date",
                    "shares",
                    "url",
                    "text",
                    "insider",
                    "position",
                    "transaction",
                    "ownership",
                    "value",
                    "surrogate_key",
                ]
                check_missing_columns(df, column_order, method)
                df["start_date"] = df["start_date"].dt.strftime("%Y-%m-%d")
                df["surrogate_key"] = df.apply(
                    lambda x: hashlib.sha256(
                        "".join(str(x) for x in x.values).encode("utf-8")
                    ).hexdigest(),
                    axis=1,
                )
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame()
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame()

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_institutional_holders(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running method {method} for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_institutional_holders()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df = df.rename(columns={"% Out": "pct_out"})
                df.columns = clean_strings(df.columns)
                column_order = [
                    "date_reported",
                    "ticker",
                    "holder",
                    "pct_change",
                    "pct_held",
                    "shares",
                    "value",
                ]
                check_missing_columns(df, column_order, method)
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date_reported"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date_reported"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_isin(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            data = self.yf_ticker_obj.get_isin()
            if len(data):
                df = pd.DataFrame.from_dict({"value": data}, orient="index").T
                df["timestamp_extracted"] = datetime.utcnow()
                df["ticker"] = ticker
                column_order = ["ticker", "timestamp_extracted", "value"]
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date_reported"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_revenue_estimate(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_revenue_estimate()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.reset_index()
                df.columns = clean_strings(df.columns)
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df = fix_empty_values(df, exclude_columns=["ticker"])
                column_order = [
                    "ticker",
                    "timestamp_extracted",
                    "period",
                    "avg",
                    "low",
                    "high",
                    "number_of_analysts",
                    "year_ago_revenue",
                    "growth",
                ]
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_sec_filings(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            data = self.yf_ticker_obj.get_sec_filings()
            if len(data):
                try:
                    df = pd.DataFrame(data)
                except Exception:
                    raise ValueError(
                        "Error in get_sec_filings! Could not convert raw data to pandas df."
                    )
                df.columns = clean_strings(df.columns)
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df["exhibits"] = df["exhibits"].astype(str)
                df = fix_empty_values(df, exclude_columns=["ticker"])
                column_order = [
                    "ticker",
                    "date",
                    "epoch_date",
                    "type",
                    "title",
                    "edgar_url",
                    "exhibits",
                    "max_age",
                    "timestamp_extracted",
                ]
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_major_holders(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_major_holders()
            if isinstance(df, pd.DataFrame) and df.shape[0] and df.shape[1] == 2:
                df.columns = ["value", "breakdown"]
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df = fix_empty_values(df, exclude_columns=["ticker"])
                column_order = ["timestamp_extracted", "ticker", "breakdown", "value"]
                return df[[i for i in column_order if i in df.columns]]
            if (
                isinstance(df, (pd.DataFrame, pd.Series))
                and df.shape[0]
                and df.shape[1] == 1
            ):
                df = (
                    df.rename_axis("breakdown")
                    .reset_index()
                    .rename(columns={"Value": "value"})
                )
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df = fix_empty_values(df, exclude_columns=["ticker"])
                column_order = ["timestamp_extracted", "ticker", "breakdown", "value"]
                check_missing_columns(df, column_order, method)
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_mutualfund_holders(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running method {method} for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_mutualfund_holders()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df.columns = clean_strings(df.columns)
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                column_order = [
                    "date_reported",
                    "ticker",
                    "holder",
                    "pct_change",
                    "pct_held",
                    "shares",
                    "value",
                ]

                check_missing_columns(df, column_order, method)
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date_reported"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date_reported"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_news(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = pd.DataFrame(self.yf_ticker_obj.get_news())
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df[["id", "content"]] = df[["id", "content"]].astype(str)
                df.columns = clean_strings(df.columns)
                df = fix_empty_values(df, exclude_columns=["ticker"])

                column_order = ["timestamp_extracted", "ticker", "id", "content"]
                check_missing_columns(df, column_order, method)

                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date_reported"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date_reported"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_recommendations(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        column_order = [
            "ticker",
            "timestamp_extracted",
            "period",
            "strong_buy",
            "buy",
            "hold",
            "sell",
            "strong_sell",
        ]
        try:
            df = self.yf_ticker_obj.get_recommendations()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = clean_strings(df.columns)
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                check_missing_columns(df, column_order, method)
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=column_order)
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=column_order)

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_recommendations_summary(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running method {method} for ticker {ticker})")
        column_order = [
            "ticker",
            "timestamp_extracted",
            "period",
            "strong_buy",
            "buy",
            "hold",
            "sell",
            "strong_sell",
        ]
        try:
            df = self.yf_ticker_obj.get_recommendations()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = clean_strings(df.columns)
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                check_missing_columns(df, column_order, method)
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=column_order)
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=column_order)

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_rev_forecast(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_shares(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_shares_full(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_shares_full()
            if isinstance(df, pd.Series) and df.shape[0]:
                df = df.reset_index()
                df.columns = ["timestamp", "amount"]
                df["ticker"] = ticker
                self.extract_ticker_tz_aware_timestamp(df, "timestamp", ticker)
                df = fix_empty_values(df, exclude_columns=["ticker"])
                column_order = [
                    "timestamp",
                    "timestamp_tz_aware",
                    "timezone",
                    "ticker",
                    "amount",
                ]
                check_missing_columns(df, column_order, method)

                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_splits(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        column_order = [
            "timestamp",
            "timestamp_tz_aware",
            "timezone",
            "ticker",
            "stock_splits",
        ]

        try:
            df = self.yf_ticker_obj.get_splits()
            if isinstance(df, pd.Series) and df.shape[0]:
                df = df.rename_axis("timestamp").reset_index()
                df["ticker"] = ticker
                self.extract_ticker_tz_aware_timestamp(df, "timestamp", ticker)
                df.columns = clean_strings(df.columns)
                df = fix_empty_values(df, exclude_columns=["ticker"])
                check_missing_columns(df, column_order, method)
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_sustainability(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.get_sustainability()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df.columns = clean_strings(df.columns)
                column_order = [
                    "timestamp_extracted",
                    "ticker",
                    "max_age",
                    "total_esg",
                    "environment_score",
                    "social_score",
                    "governance_score",
                    "rating_year",
                    "rating_month",
                    "highest_controversy",
                    "peer_count",
                    "esg_performance",
                    "peer_group",
                    "related_controversy",
                    "peer_esg_score_performance",
                    "peer_governance_performance",
                    "peer_social_performance",
                    "peer_environment_performance",
                    "peer_highest_controversy_performance",
                    "percentile",
                    "environment_percentile",
                    "social_percentile",
                    "governance_percentile",
                    "adult",
                    "alcoholic",
                    "animal_testing",
                    "catholic",
                    "controversial_weapons",
                    "small_arms",
                    "fur_leather",
                    "gambling",
                    "gmo",
                    "military_contract",
                    "nuclear",
                    "pesticides",
                    "palm_oil",
                    "coal",
                    "tobacco",
                ]

                check_missing_columns(df, column_order, method)

                str_cols = [
                    "related_controversy",
                    "peer_esg_score_performance",
                    "peer_governance_performance",
                    "peer_social_performance",
                    "peer_environment_performance",
                    "peer_highest_controversy_performance",
                ]

                str_cols = [i for i in str_cols if i in df.columns]
                df[str_cols] = df[str_cols].astype(str)
                df = fix_empty_values(df, exclude_columns=["ticker"])
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_trend_details(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def ttm_cash_flow(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.ttm_cash_flow
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("p_p_e", "ppe") for i in clean_strings(df.columns)
                ]
                column_order = CASH_FLOW_COLUMNS
                check_missing_columns(df, column_order, method)
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def ttm_cashflow(self, ticker):
        """duplicate of ttm_cash_flow"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        pass

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def ttm_financials(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        column_order = FINANCIAL_COLUMNS
        try:
            df = self.yf_ticker_obj.ttm_financials.T
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("e_b_i_t_d_a", "ebitda")
                    .replace("e_p_s", "eps")
                    .replace("e_b_i_t", "ebit")
                    .replace("p_p_e", "ppe")
                    .replace(
                        "diluted_n_i_availto_com_stockholders",
                        "diluted_ni_availto_com_stockholders",
                    )
                    for i in clean_strings(df.columns)
                ]
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def ttm_income_stmt(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.ttm_income_stmt
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("e_b_i_t_d_a", "ebitda")
                    .replace("e_p_s", "eps")
                    .replace("e_b_i_t", "ebit")
                    .replace(
                        "diluted_n_i_availto_com_stockholders",
                        "diluted_ni_availto_com_stockholders",
                    )
                    for i in clean_strings(df.columns)
                ]
                column_order = INCOME_STMT_COLUMNS
                check_missing_columns(df, column_order, method)
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def ttm_incomestmt(self, ticker):
        """duplicate of ttm_income_stmt"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        pass

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def get_upgrades_downgrades(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running method {method} for ticker {ticker})")
        column_order = [
            "grade_date",
            "ticker",
            "firm",
            "to_grade",
            "from_grade",
            "action",
            "price_target_action",
            "current_price_target",
        ]
        try:
            df = self.yf_ticker_obj.get_upgrades_downgrades()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.reset_index()
                df.columns = clean_strings(df.columns)
                df["ticker"] = ticker
                check_missing_columns(df, column_order, method)
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=column_order)
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=column_order)

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def option_chain(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")

        option_expiration_dates = self.yf_ticker_obj.options
        if len(option_expiration_dates):
            for exp_date in option_expiration_dates:
                option_chain_data = self.yf_ticker_obj.option_chain(date=exp_date)

                if len(option_chain_data) == 3:
                    calls = option_chain_data.calls
                    puts = option_chain_data.puts
                    underlying = option_chain_data.underlying

                    assert isinstance(calls, pd.DataFrame) and isinstance(
                        puts, pd.DataFrame
                    ), "calls or puts are not a dataframe!"

                    calls["option_type"] = "call"
                    puts["option_type"] = "put"

                    if all(calls.columns == puts.columns):
                        df_options = pd.concat([calls, puts]).reset_index(drop=True)
                        df_options["metadata"] = str(underlying)
                        df_options["timestamp_extracted"] = datetime.utcnow()
                        self.extract_ticker_tz_aware_timestamp(
                            df_options, "last_trade_date", ticker
                        )
                        df_options = df_options.replace([np.inf, -np.inf, np.nan], None)
                        df_options.columns = clean_strings(df_options.columns)
                        column_order = [
                            "last_trade_date",
                            "last_trade_date_tz_aware",
                            "timezone",
                            "ticker",
                            "option_type",
                            "contract_symbol",
                            "strike",
                            "last_price",
                            "bid",
                            "ask",
                            "change",
                            "percent_change",
                            "volume",
                            "open_interest",
                            "implied_volatility",
                            "in_the_money",
                            "contract_size",
                            "currency",
                            "metadata",
                            "timestamp_extracted",
                        ]
                        check_missing_columns(df_options, column_order, method)
                        return df_options[column_order]
                    else:
                        raise ValueError(
                            "Error parsing option_chain. Column order of calls and puts do not match."
                        )
                else:
                    raise ValueError(
                        "Error parsing option_chain. Check if data passed changed"
                    )
        else:
            return pd.DataFrame(columns=["last_trade_date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def options(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            option_expiration_dates = self.yf_ticker_obj.options
            if option_expiration_dates:
                df = pd.DataFrame(option_expiration_dates, columns=["expiration_date"])
                df["ticker"] = ticker
                df["timestamp_extracted"] = datetime.utcnow()
                df["expiration_date"] = pd.to_datetime(df["expiration_date"])
                df = fix_empty_values(df, exclude_columns=["ticker"])
                column_order = ["timestamp_extracted", "ticker", "expiration_date"]
                check_missing_columns(df, column_order, method)
                return df[[i for i in column_order if i in df.columns]]
            else:
                return pd.DataFrame(
                    columns=["timestamp_extracted", "ticker", "expiration_date"]
                )
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting option expiration dates for {ticker} in method {method}. Error: {e}. Skipping..."
            )
            return pd.DataFrame(
                columns=["timestamp_extracted", "ticker", "expiration_date"]
            )

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def quarterly_balance_sheet(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running method {method} for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.quarterly_balance_sheet
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df.columns = [
                    i.replace("p_p_e", "ppe") for i in clean_strings(df.columns)
                ]
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df = df.rename(
                    columns={
                        # fmt: off
                        "financial_assets_designatedas_fair_value_through_profitor_loss_total":
                            "financial_assets_designated_as_fv_thru_profitor_loss_total"
                        # fmt: on
                    }
                )
                column_order = BALANCE_SHEET_COLUMNS
                check_missing_columns(df, column_order, method)
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def quarterly_balancesheet(self, ticker):
        """Same output as the method quarterly_balance_sheet"""
        logging.info(f"*** Running method {method} for ticker {ticker})")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def quarterly_cash_flow(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.quarterly_cash_flow
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("p_p_e", "ppe") for i in clean_strings(df.columns)
                ]
                column_order = CASH_FLOW_COLUMNS
                check_missing_columns(df, column_order, method)
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def quarterly_cashflow(self, ticker):
        """Same output as the method quarterly_cash_flow"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def quarterly_financials(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.quarterly_financials
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("e_b_i_t_d_a", "ebitda")
                    .replace("e_p_s", "eps")
                    .replace("e_b_i_t", "ebit")
                    .replace(
                        "diluted_n_i_availto_com_stockholders",
                        "diluted_ni_availto_com_stockholders",
                    )
                    for i in clean_strings(df.columns)
                ]
                column_order = FINANCIAL_COLUMNS
                check_missing_columns(df, column_order, method)
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def quarterly_income_stmt(self, ticker):
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        try:
            df = self.yf_ticker_obj.quarterly_income_stmt
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df = fix_empty_values(df, exclude_columns=["ticker"])
                df.columns = [
                    i.replace("e_b_i_t_d_a", "ebitda")
                    .replace("e_p_s", "eps")
                    .replace("e_b_i_t", "ebit")
                    .replace(
                        "diluted_n_i_availto_com_stockholders",
                        "diluted_ni_availto_com_stockholders",
                    )
                    for i in clean_strings(df.columns)
                ]
                column_order = INCOME_STMT_COLUMNS
                check_missing_columns(df, column_order, method)
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                return df[[i for i in column_order if i in df.columns]]
            else:
                logging.warning(
                    f"No data found for method {method} and ticker {ticker}."
                )
                return pd.DataFrame(columns=["date"])
        except YFRateLimitError as e:
            logging.warning(f"Rate limit hit for {ticker}, will retry: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Error extracting data for method {method} and ticker {ticker}. Failed with error: {e}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def quarterly_incomestmt(self, ticker):
        """Same output as the method quarterly_income_stmt"""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return

    @backoff.on_exception(
        backoff.expo,
        (YFRateLimitError, RequestException, MaxRetryError, NewConnectionError, HTTPError),
        max_tries=10,
        max_time=10000,
        jitter=backoff.full_jitter,
    )
    def session(self, ticker):
        """Returns NoneType."""
        method = get_method_name()
        logging.info(f"*** Running {method} for ticker {ticker}")
        return


def rename_days_ago(col):
    match = re.match(r"^(\d+)[Dd]ays[Aa]go$", col)
    if match:
        return f"days_ago_{match.group(1)}"
    return col
