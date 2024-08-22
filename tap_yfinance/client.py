from __future__ import annotations

from abc import ABC

import pandas as pd

from singer_sdk import Tap
from typing import Iterable, Optional, Any
from singer_sdk.streams import Stream
from tap_yfinance.price_utils import *
from tap_yfinance.financial_utils import *
from singer_sdk import typing as th
from singer_sdk.helpers._state import increment_state

CUSTOM_JSON_SCHEMA = {
    "additionalProperties": True,
    "description": "Custom JSON typing.",
    "type": ["object", "null"],
}


class BaseStream(Stream, ABC):
    def __init__(self, tap: Tap) -> None:
        super().__init__(tap=tap)

        self.stream_params = self.config.get(self.name)
        self.tickers = None
        self.df_tickers = None
        self._ticker_download_calls = 0

    @property
    def partitions(self) -> list[dict]:
        if self._ticker_download_calls == 0:
            logging.info(f"Tickers have not been downloaded yet. Downloading now...")
            self.download_tickers(self.stream_params)

        assert isinstance(
            self.tickers, list
        ), f"self.tickers must be a list, but it is of type {type(self.tickers)}."
        return [{"ticker": t} for t in self.tickers]

    def download_tickers(self, stream_params):
        assert (
            self._ticker_download_calls == 0
        ), f"self._ticker_download_calls should be set to 0 but is {self._ticker_download_calls}."

        ticker_downloader = TickerDownloader()

        if stream_params.get("tickers") == "*":
            ticker_download_method = self.get_ticker_download_method()
            self.df_tickers = getattr(ticker_downloader, ticker_download_method)()
            self.tickers = self.df_tickers["ticker"].unique().tolist()
        else:
            self.df_tickers = pd.DataFrame({"ticker": stream_params["tickers"]})
            self.tickers = stream_params["tickers"]

        self._ticker_download_calls += 1

        assert (
            self._ticker_download_calls == 1
        ), f"""
                self.download_tickers has been called too many times.
                It should only be called once but has been called {self._ticker_download_calls} times.
            """

        assert isinstance(self.tickers, list)
        return self

    def get_ticker_download_method(self):
        if self.name.startswith("stock"):
            return "download_valid_stock_tickers"
        elif self.name.startswith("futures"):
            return "download_futures_tickers"
        elif self.name.startswith("forex"):
            return "download_forex_pairs"
        elif self.name.startswith("crypto") and self.name != "crypto_tickers_top_250":
            return "download_crypto_tickers"
        elif self.name == "crypto_tickers_top_250":
            return "download_top_250_crypto_tickers"
        elif self.name in (
            "actions",
            "balance_sheet",
            "calendar",
            "cash_flow",
            "dividends",
            "earnings_dates",
            "fast_info",
            "financials",
            "history_metadata",
            "income_stmt",
            "insider_purchases",
            "insider_roster_holders",
            "insider_transactions",
            "institutional_holders",
            "major_holders",
            "mutualfund_holders",
            "news",
            "recommendations",
            "shares_full",
            "splits",
            "upgrades_downgrades",
            "option_chain",
            "options",
            "quarterly_balance_sheet",
            "quarterly_cash_flow",
            "quarterly_financials",
            "quarterly_income_stmt",
        ):
            return (
                "download_valid_stock_tickers"  # only stock tickers for financial data
            )
        else:
            raise ValueError(
                f"Could not determine ticker_download_method. Variable self.name is set to {self.name}"
            )


class TickerStream(BaseStream):
    replication_key = "ticker"
    is_timestamp_replication_key = False

    def get_records(self, context: dict | None) -> Iterable[dict]:
        state = self.get_context_state(context)

        record = self.df_tickers[
            self.df_tickers["ticker"] == context["ticker"]
        ].to_dict(orient="records")[0]

        increment_state(
            state,
            replication_key=self.replication_key,
            latest_record=record,
            is_sorted=self.is_sorted,
            check_sorted=self.check_sorted,
        )

        yield record


class BasePriceStream(BaseStream):
    replication_key = "timestamp"
    is_timestamp_replication_key = True
    # primary_keys = ["timestamp", "ticker"]
    # is_sorted = True  #  TODO: Test with is_sorted = True and add @backoff rate limiter

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self.yf_params = (
            self.stream_params.get("yf_params")
            if self.stream_params is not None
            else None
        )

    def get_records(self, context: dict | None) -> Iterable[dict]:
        logging.info(f"\n\n\n*** Running ticker {context['ticker']} *** \n\n\n")
        yf_params = self.yf_params.copy()
        state = self.get_context_state(context)

        if state and "progress_markers" in state.keys():
            start_date = datetime.fromisoformat(
                state.get("progress_markers").get("replication_key_value")
            ).strftime("%Y-%m-%d")
        else:
            start_date = self.config.get("default_start_date")

        yf_params["start"] = start_date

        price_tap = PriceTap(
            schema=self.schema,
            config=self.config,
            name=self.name,
            ticker=context["ticker"],
        )

        df = price_tap.download_price_history(
            ticker=context["ticker"], yf_params=yf_params
        )

        for record in df.to_dict(orient="records"):
            increment_state(
                state,
                replication_key=self.replication_key,
                latest_record=record,
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted,
            )

            yield record


class StockPricesStream(BasePriceStream):
    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("timestamp_tz_aware", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("ticker", th.StringType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("dividends", th.NumberType),
        th.Property("stock_splits", th.NumberType),
        th.Property("repaired", th.BooleanType),
        th.Property("replication_key", th.StringType),
    ).to_dict()


class DerivativePricesStream(BasePriceStream):
    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("timestamp_tz_aware", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("ticker", th.StringType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("repaired", th.BooleanType),
        th.Property("replication_key", th.StringType),
    ).to_dict()


class PricesStreamWide(BaseStream):
    replication_key = "timestamp"
    is_timestamp_replication_key = True

    schema = th.PropertiesList(  # potentially a dynamic number of columns
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("data", th.CustomType(CUSTOM_JSON_SCHEMA), required=True),
    ).to_dict()

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self.yf_params = (
            self.stream_params.get("yf_params")
            if self.stream_params is not None
            else None
        )

    @property
    def partitions(self):
        """
        No partitions when running wide stream. Performance will increase because all relevant tickers are batch
        downloaded at once, but data integrity will be likely be sacrificed (see yfinance docs).
        """

        return None

    def get_records(self, context: dict | None) -> Iterable[dict]:
        if self._ticker_download_calls == 0:
            logging.info(f"Tickers have not been downloaded yet. Downloading now...")
            self.download_tickers(self.stream_params)

        assert isinstance(
            self.tickers, list
        ), f"self.tickers must be a list, but it is of type {type(self.tickers)}."

        yf_params = self.yf_params.copy()
        state = self.get_context_state(context)

        if state and "progress_markers" in state.keys():
            start_date = datetime.fromisoformat(
                state.get("progress_markers").get("replication_key_value")
            ).strftime("%Y-%m-%d")
        else:
            start_date = self.config.get("default_start_date")

        yf_params["start"] = start_date

        price_tap = PriceTap(schema=self.schema, config=self.config, name=self.name)

        df = price_tap.download_price_history_wide(
            tickers=self.tickers, yf_params=yf_params
        )
        df.sort_values(by="timestamp", inplace=True)

        for record in df.to_dict(orient="records"):
            record["replication_key"] = record["timestamp"]

            increment_state(
                state,
                replication_key=self.replication_key,
                latest_record=record,
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted,
            )

            cleaned_record = {
                "data": str(record),
                self.replication_key: record["replication_key"],
            }
            yield cleaned_record


class FinancialStream(BaseStream):
    is_timestamp_replication_key = True

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self.yf_params = (
            self.stream_params.get("yf_params")
            if self.stream_params is not None
            else None
        )

    def get_records(self, context: dict | None) -> Iterable[dict]:
        logging.info(f"\n\n\n*** Running ticker {context['ticker']} *** \n\n\n")
        state = self.get_context_state(context)

        financial_tap = FinancialTap(
            schema=self.schema,
            ticker=context["ticker"],
            config=self.config,
            name=self.name,
        )
        df = getattr(financial_tap, self.method_name)(ticker=context["ticker"])

        for record in df.to_dict(orient="records"):
            increment_state(
                state,
                replication_key=self.replication_key,
                latest_record=record,
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted,
            )

            yield record
