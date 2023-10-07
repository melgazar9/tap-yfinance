"""Stream type classes for tap-yfinance."""
from __future__ import annotations
import typing as t
from singer_sdk import typing as th
from tap_yfinance.client import YFinanceStream


class YFinanceStream(YFinanceStream):
    """Define custom stream for your dataset."""

    name = "yfinance_stream"
    primary_keys: t.ClassVar[list[str]] = ["timestamp", "yahoo_ticker"]  # Define your primary keys
    replication_key = 'replication_key'

    schema = th.PropertiesList(
        th.Property("replication_key", th.StringType),
        th.Property("timestamp", th.DateTimeType),
        th.Property("timestamp_tz_aware", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("yahoo_ticker", th.StringType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("dividends", th.NumberType),
        th.Property("stock_splits", th.NumberType)
    ).to_dict()