"""Custom client handling, including YFinanceStream base class."""

from __future__ import annotations

from typing import Iterable
from singer_sdk.streams import Stream
from price_utils import *


class YFinanceStream(Stream):
    name = "tap-yfinance"
    """Stream class for YFinance streams."""

    # def __init__(self):
    #     super().__init__(TapPrices)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            NotImplementedError: If the implementation is TODO
        """

        price_tap = YFinancePriceTap(asset_class=self.config['asset_class'])

        yf_params = {
            'interval': '1m',
            'start': '1950-01-01',
            'prepost': True,
            'repair': True,
            'auto_adjust': True,
            'back_adjust': False
        }

        tickers = ['AAPL', 'AMZN']
        for ticker in tickers:
            df = price_tap.download_single_symbol_price_history(ticker=ticker, yf_history_params=yf_params)
            for record in df.to_dict():
                yield record