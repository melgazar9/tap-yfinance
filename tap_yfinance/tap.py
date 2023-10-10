"""YFinance tap class."""

from __future__ import annotations

from singer_sdk import Tap
from tap_yfinance import streams

class TapYFinance(Tap):
    """YFinance tap class."""
    name = "tap-yfinance"

    def discover_streams(self) -> list[streams.YFinanceStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [streams.YFinanceStream(self)]

if __name__ == "__main__":
    TapYFinance.cli()