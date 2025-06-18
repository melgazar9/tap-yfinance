import contextlib
import inspect
import logging
import re
import time
from datetime import datetime, timedelta
from io import StringIO
from uuid import uuid4

import backoff
import numpy as np
import pandas as pd
from requests.exceptions import ConnectionError, HTTPError, RequestException
from urllib3.exceptions import MaxRetryError, NewConnectionError
from yfinance.exceptions import YFRateLimitError


class RateLimitManager:
    def __init__(self):
        self.last_request_time = {}
        self.min_delay = 1.0  # Minimum 1 second between requests

    def wait_if_needed(self, endpoint_name):
        """Wait if we need to respect rate limits"""
        now = datetime.now()
        last_time = self.last_request_time.get(endpoint_name)

        if last_time:
            time_since_last = (now - last_time).total_seconds()
            if time_since_last < self.min_delay:
                sleep_time = self.min_delay - time_since_last
                logging.info(
                    f"Rate limiting: sleeping {sleep_time:.1f}s for {endpoint_name}"
                )
                time.sleep(sleep_time)

        self.last_request_time[endpoint_name] = now


rate_limiter = RateLimitManager()


class EmptyDataException(Exception):
    """Raised when data is empty but likely should contain data... Used to trigger backoff."""

    pass


def yfinance_backoff(func):
    """
    Enhanced backoff that treats 401/429 as rate limits.
    Everything else is a permanent error.
    """

    def wrapped_func(*args, **kwargs):
        stderr_capture = StringIO()
        with contextlib.redirect_stderr(stderr_capture):
            result = func(*args, **kwargs)

        stderr_output = stderr_capture.getvalue()
        if "HTTP Error" in stderr_output:
            logging.info(f"Detected yfinance HTTP error: {stderr_output.strip()}")

            status_match = re.search(r"HTTP Error (\d+):", stderr_output)
            if status_match:
                status_code = int(status_match.group(1))

                # Treat 401/429 as rate limits
                if status_code in [401, 429]:
                    logging.info(
                        f"Treating HTTP {status_code} as rate limit - will retry"
                    )
                    raise RequestException(f"Rate limit: HTTP {status_code}")
                else:
                    # 404, 500, 403, etc. - permanent errors
                    logging.info(f"HTTP {status_code} is permanent error - giving up")
                    raise HTTPError(f"HTTP {status_code}: {stderr_output.strip()}")
            else:
                # Unknown HTTP error format - treat as permanent
                raise HTTPError(f"Unknown HTTP error: {stderr_output.strip()}")
        return result

    def giveup_on_http_errors(exception):
        # Give up on HTTPError (404, 500, 403, etc.)
        # Retry on RequestException (401, 429) and YFRateLimitError
        return isinstance(exception, HTTPError)

    def backoff_handler(details):
        logging.info(
            f"Backing off {details['target'].__name__} for {details['wait']:.1f}s "
            f"(attempt {details['tries']}) - {details['exception']}"
        )

    return backoff.on_exception(
        backoff.expo,
        (
            YFRateLimitError,
            RequestException,
            MaxRetryError,
            NewConnectionError,
            ConnectionError,
            EmptyDataException,
        ),
        max_tries=5,
        max_time=300,
        base=3,
        max_value=60,
        jitter=backoff.full_jitter,
        giveup=giveup_on_http_errors,
        on_backoff=backoff_handler,
    )(wrapped_func)


def yfinance_light_backoff(func):
    """
    Lighter backoff for less critical operations.
    """
    return backoff.on_exception(
        backoff.expo,
        (
            YFRateLimitError,
            RequestException,
            MaxRetryError,
            NewConnectionError,
            ConnectionError,
            EmptyDataException,
        ),
        max_tries=5,
        max_time=5000,
        jitter=backoff.full_jitter,
    )(func)


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


def fix_empty_values(df, exclude_columns=None, to_value=None):
    """
    Replaces np.nan, inf, -inf, None, and string versions of 'nan', 'none', 'infinity'
    recursively with a specified value (default None), except for columns listed in exclude_columns.

    Args:
        df (pd.DataFrame): The input DataFrame.
        exclude_columns (list, optional): Columns to skip. Defaults to None.
        to_value: What to replace missing values with (None or np.nan). Defaults to None.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    if exclude_columns is None:
        exclude_columns = []
    if to_value is None:
        to_value = None

    regex_pattern = r"(?i)^(nan|none|infinity)$"

    def clean_obj(val):
        if isinstance(val, dict):
            return {k: clean_obj(v) for k, v in val.items()}
        if isinstance(val, list):
            return [clean_obj(x) for x in val]
        if isinstance(val, str) and re.match(regex_pattern, val):
            return to_value
        if val in [None, np.nan] or (isinstance(val, float) and not np.isfinite(val)):
            return to_value
        return val

    def replace_col(col):
        if col.name in exclude_columns:
            return col
        if pd.api.types.is_numeric_dtype(col) or pd.api.types.is_datetime64_any_dtype(
            col
        ):
            return col.replace([np.nan, np.inf, -np.inf, None], to_value)

        uuid = str(uuid4())
        return (
            col.replace([np.nan, np.inf, -np.inf, None], to_value)
            .replace(regex_pattern, to_value, regex=True)
            .apply(clean_obj)
            .fillna(uuid)
            .replace(uuid, to_value)
        )

    return df.apply(replace_col)


def check_missing_columns(df, column_order, stream_name, ignore_cols=None):
    ignore_cols = set() if ignore_cols is None else ignore_cols
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
