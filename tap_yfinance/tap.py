"""YFinance tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_yfinance import streams


class TapYFinance(Tap):
    """YFinance tap class."""

    name = "tap-yfinance"

    # config_jsonschema = th.PropertiesList(
    #     th.Property("timestamp", th.DateTimeType, required=True),
    #     th.Property("timestamp_tz_aware", th.StringType, required=True),
    #     th.Property("timezone", th.StringType, required=True),
    #     th.Property("yahoo_ticker", th.StringType, required=True),
    #     th.Property("open", th.NumberType, required=True),
    #     th.Property("high", th.NumberType, required=True),
    #     th.Property("low", th.NumberType, required=True),
    #     th.Property("close", th.NumberType, required=True),
    #     th.Property("volume", th.NumberType, required=True),
    #     th.Property("dividends", th.NumberType, required=True),
    #     th.Property("stock_splits", th.NumberType, required=True)
    # ).to_dict()

    def discover_streams(self) -> list[streams.YFinanceStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [streams.YFinanceStream(self)]


if __name__ == "__main__":
    TapYFinance.cli()