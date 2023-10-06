"""YFinance tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_yfinance import streams


class TapYFinance(Tap):
    """YFinance tap class."""

    name = "tap-yfinance"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        )
    ).to_dict()

    def discover_streams(self) -> list[streams.YFinanceStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.YFinanceStream(self),
        ]


if __name__ == "__main__":
    TapYFinance.cli()
