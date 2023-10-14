"""YFinance tap class."""

from __future__ import annotations

from singer_sdk import Tap
from tap_yfinance import streams

class TapYFinance(Tap):
    """YFinance tap class."""
    name = "tap-yfinance"

    def discover_streams(self) -> list[streams.YFinanceStream]:
        """Return a list of discovered streams.Î

        Returns:
            A list of discovered streams.
        """

        output_streams = []

        asset_classes = [key for key in self.config["asset_class"]]
        stream_dict = {asset_class: list(self.config["asset_class"][asset_class].keys()) for asset_class in asset_classes}

        for asset_class, tables in stream_dict.items():
            for table_name in tables:
                stream = streams.YFinanceStream(self, name=table_name, asset_class=asset_class)
                output_streams.append(stream)

        return output_streams

if __name__ == "__main__":
    TapYFinance.cli()
    # import subprocess
    #
    # # Run the `meltano elt` command with configuration and state files
    # subprocess.run(
    #     ["meltano", "el", "tap-yfinance", "target-jsonl"]
    # )