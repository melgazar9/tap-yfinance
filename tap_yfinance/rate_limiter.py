import contextlib
import logging
import re
import time
from datetime import datetime
from io import StringIO

import backoff
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
