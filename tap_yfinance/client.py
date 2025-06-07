from __future__ import annotations

import logging
from abc import ABC
from typing import Iterable

import pandas as pd
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.helpers._state import increment_state
from singer_sdk.streams import Stream

from tap_yfinance.financial_utils import *
from tap_yfinance.price_utils import *

CUSTOM_JSON_SCHEMA = {
    "additionalProperties": True,
    "description": "Custom JSON typing.",
    "type": ["object", "null"],
}

ALL_SEGMENTS = [
    "stock_tickers",
    "pts_tickers",
    "bonds_tickers",
    "forex_tickers",
    "futures_tickers",
    "crypto_tickers",
    "options_tickers",
    "world_indices_tickers",
    "etf_tickers",
    "private_companies_tickers",
    "mutual_fund_tickers",
]


class BaseStream(Stream, ABC):
    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)

    def get_ticker_segment(self):
        n = self.name.lower()
        if n.startswith("stock") or n in (
            "actions",
            "analyst_price_targets",
            "balance_sheet",
            "calendar",
            "cash_flow",
            "dividends",
            "earnings_dates",
            "earnings_estimate",
            "earnings_history",
            "eps_revisions",
            "eps_trend",
            "fast_info",
            "financials",
            "growth_estimates",
            "history_metadata",
            "info",
            "income_stmt",
            "insider_purchases",
            "insider_roster_holders",
            "insider_transactions",
            "institutional_holders",
            "isin",
            "major_holders",
            "mutualfund_holders",
            "news",
            "sec_tickers",
            "recommendations",
            "recommendations_summary",
            "revenue_estimate",
            "sec_filings",
            "shares_full",
            "splits",
            "sustainability",
            "ttm_cash_flow",
            "ttm_financials",
            "ttm_income_stmt",
            "option_chain",
            "options",
            "quarterly_balance_sheet",
            "quarterly_cash_flow",
            "quarterly_financials",
            "quarterly_income_stmt",
            "upgrades_downgrades",
        ):
            return "stock_tickers"
        elif n.startswith("futures"):
            return "futures_tickers"
        elif n.startswith("forex"):
            return "forex_tickers"
        elif n.startswith("bonds"):
            return "bonds_tickers"
        elif n == "crypto_tickers_top_250":
            return "crypto_tickers_top_250"
        elif n.startswith("crypto"):
            return "crypto_tickers"
        elif n == "mutual_fund_tickers":
            return "mutual_fund_tickers"
        elif n.startswith("options"):
            return "options_tickers"
        elif n.startswith("etf"):
            return "etf_tickers"
        elif n.startswith("indices") or n == "world_indices_tickers":
            return "world_indices_tickers"
        elif n.startswith("private_companies") or n == "private_companies_tickers":
            return "private_companies_tickers"
        else:
            raise ValueError(f"Could not determine ticker segment for stream: {n}")

    def fetch_and_cache_tickers(self):
        """
        For non-prices streams, behaves as before.
        For prices_* streams, will aggregate tickers by config.
        If tickers specified: Use those, with best-guess segment assignment.
        If tickers == "*": Aggregate all tickers from all supported segments.
        """
        n = self.name.lower()
        if n.startswith("prices"):
            if not hasattr(self._tap, "ticker_cache"):
                self._tap.ticker_cache = {}
            tickers_cfg = self.config.get(self.name, {}).get("tickers")
            if tickers_cfg and tickers_cfg != "*":
                tickers = tickers_cfg

                # Guess segment for each ticker (for info only)
                def guess_segment(t):
                    if isinstance(t, str) and t.endswith("=X"):
                        return "forex_tickers"
                    if isinstance(t, str) and ("-" in t and t.endswith("USD")):
                        return "crypto_tickers"
                    if isinstance(t, str) and t.isupper() and len(t) <= 5:
                        return "stock_tickers"
                    if isinstance(t, str) and ".PVT" in t:
                        return "private_companies_tickers"
                    if isinstance(t, str) and t.startswith("^"):
                        return "world_indices_tickers"
                    # fallback
                    return "unknown"

                segment_list = [guess_segment(t) for t in tickers]
                self.df_tickers = pd.DataFrame(
                    {
                        "ticker": tickers,
                        "name": [None] * len(tickers),
                        "segment": segment_list,
                    }
                )
                self._tap.ticker_cache[self.name] = self.df_tickers
            else:
                # tickers == "*" means ALL tickers from all supported segments
                if self.name not in self._tap.ticker_cache:
                    all_dfs = []
                    for segment in ALL_SEGMENTS:
                        logging.info(
                            f"Pulling {segment} tickers for {self.name} stream..."
                        )
                        try:
                            if segment == "pts_tickers":
                                df = TickerDownloader.download_pts_tickers()
                                if (
                                    "ticker" not in df.columns
                                    and "yahoo_ticker" in df.columns
                                ):
                                    df = df.rename(columns={"yahoo_ticker": "ticker"})
                                df = df[["ticker", "name", "segment"]].drop_duplicates(
                                    subset=["ticker", "segment"]
                                )
                            else:
                                df = TickerDownloader.download_yahoo_tickers(segment)
                                if "segment" not in df.columns:
                                    df["segment"] = segment
                                df = df[["ticker", "name", "segment"]].drop_duplicates()
                            df = fix_empty_values(df, exclude_columns=["ticker"])
                            df["ticker"] = df["ticker"].astype(str)
                            all_dfs.append(df)
                        except Exception as e:
                            self._tap.logger.warning(
                                f"Could not download {segment}: {e}"
                            )
                    if all_dfs:
                        all_tickers = pd.concat(all_dfs, ignore_index=True)
                        all_tickers = all_tickers.drop_duplicates(subset=["ticker"])
                        self._tap.ticker_cache[self.name] = all_tickers
                    else:
                        self._tap.ticker_cache[self.name] = pd.DataFrame(
                            columns=["ticker", "name", "segment"]
                        )
                self.df_tickers = self._tap.ticker_cache[self.name]
            self.cached_tickers = self.df_tickers["ticker"].drop_duplicates().tolist()
        else:
            segment = self.get_ticker_segment()
            if not hasattr(self._tap, "ticker_cache"):
                self._tap.ticker_cache = {}
            tickers_cfg = self.config.get(self.name, {}).get("tickers")
            if tickers_cfg and tickers_cfg != "*":
                tickers = tickers_cfg
                logging.info(
                    f"Using tickers from config for segment {segment}: {tickers}"
                )
                self.df_tickers = pd.DataFrame(
                    {
                        "ticker": tickers,
                        "name": [None] * len(tickers),
                        "segment": [segment] * len(tickers),
                    }
                )
                self._tap.ticker_cache[segment] = self.df_tickers
            else:
                if segment not in self._tap.ticker_cache:
                    try:
                        logging.info(f"Pulling all tickers for segment {segment}...")
                        if segment == "stock_tickers":
                            df = TickerDownloader.download_pts_tickers()
                            df = df[["ticker", "name", "segment"]].drop_duplicates(
                                subset=["ticker", "segment"]
                            )
                        else:
                            df = TickerDownloader.download_yahoo_tickers(segment)
                            if "segment" not in df.columns:
                                df["segment"] = segment
                            df = df[["ticker", "name", "segment"]].drop_duplicates()
                        df = fix_empty_values(df, exclude_columns=["ticker"])
                        df["ticker"] = df["ticker"].astype(str)
                        self._tap.ticker_cache[segment] = df
                    except Exception as e:
                        self._tap.logger.warning(f"Could not download {segment}: {e}")
                        self._tap.ticker_cache[segment] = pd.DataFrame(
                            columns=["ticker", "name", "segment"]
                        )
                self.df_tickers = self._tap.ticker_cache[segment]
            self.cached_tickers = self.df_tickers["ticker"].drop_duplicates().tolist()


class TickerStream(BaseStream):
    primary_keys = ["ticker"]
    replication_method = "FULL_TABLE"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("segment", th.StringType),
    ).to_dict()

    def get_records(self, context: dict | None) -> Iterable[dict]:
        self.fetch_and_cache_tickers()
        for record in self.df_tickers.to_dict(orient="records"):
            yield record


class BasePriceStream(BaseStream):
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    primary_keys = ["timestamp", "ticker"]

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)

    @property
    def partitions(self):
        if getattr(self, "cached_tickers", None) is None:
            self.fetch_and_cache_tickers()
        return [{"ticker": t} for t in self.cached_tickers]

    def get_records(self, context: dict | None) -> Iterable[dict]:
        assert (
            context is not None and "ticker" in context
        ), f"Missing ticker in context for BasePriceStream {self.name}!"
        yf_params = self.config.get(self.name).get("yf_params")
        assert isinstance(
            yf_params, dict
        ), f"could not parse yf_params for stream {self.name}"
        self.fetch_and_cache_tickers()

        ticker = context["ticker"]
        logging.info(f"\n\n\n*** Running ticker {ticker} *** \n\n\n")
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
            ticker=ticker,
        )

        df = price_tap.download_price_history(ticker=ticker, yf_params=yf_params)
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
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True

    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("data", th.CustomType(CUSTOM_JSON_SCHEMA), required=True),
    ).to_dict()

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        self.fetch_and_cache_tickers()
        yf_params = self.yf_params.copy() if self.yf_params else {}
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
            tickers=self.cached_tickers, yf_params=yf_params
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

    @property
    def partitions(self):
        if getattr(self, "cached_tickers", None) is None:
            self.fetch_and_cache_tickers()
        return [{"ticker": t} for t in self.cached_tickers]

    def get_records(self, context: dict | None) -> Iterable[dict]:
        assert (
            context is not None and "ticker" in context
        ), f"Missing ticker in context for FinancialStream {self.name}!"
        self.fetch_and_cache_tickers()
        yf_params = self.config.get(self.name).get("yf_params")
        ticker = context["ticker"]
        logging.info(f"\n\n\n*** Running ticker {ticker} *** \n\n\n")
        state = self.get_context_state(context)

        financial_tap = FinancialTap(
            schema=self.schema,
            ticker=ticker,
            config=self.config,
            name=self.name,
            yf_params=yf_params,
        )
        df = getattr(financial_tap, self.method_name)(ticker=ticker)

        for record in df.to_dict(orient="records"):
            if self.replication_method == "INCREMENTAL":
                increment_state(
                    state,
                    replication_key=self.replication_key,
                    latest_record=record,
                    is_sorted=self.is_sorted,
                    check_sorted=self.check_sorted,
                )
            yield record


class AllTickersStream(TickerStream):
    """A stream that yields all tickers from all Yahoo Finance segments."""

    name = "all_tickers"
    primary_keys = ["ticker"]

    def fetch_and_cache_tickers(self):
        if not hasattr(self._tap, "ticker_cache"):
            self._tap.ticker_cache = {}
        if "all_tickers" not in self._tap.ticker_cache:
            all_dfs = []
            for segment in ALL_SEGMENTS:
                logging.info(f"Pulling {segment} tickers for {self.name} stream.")
                try:
                    if segment == "pts_tickers":
                        df = TickerDownloader.download_pts_tickers()
                        if "ticker" not in df.columns and "yahoo_ticker" in df.columns:
                            df = df.rename(columns={"yahoo_ticker": "ticker"})
                        df = df[["ticker", "name", "segment"]].drop_duplicates(
                            subset=["ticker", "segment"]
                        )
                    else:
                        df = TickerDownloader.download_yahoo_tickers(segment)
                        if "segment" not in df.columns:
                            df["segment"] = segment
                        df = df[["ticker", "name", "segment"]].drop_duplicates()
                    df = fix_empty_values(df, exclude_columns=["ticker"])
                    df["ticker"] = df["ticker"].astype(str)
                    all_dfs.append(df)
                except Exception as e:
                    self._tap.logger.warning(f"Could not download {segment}: {e}")
            if all_dfs:
                all_tickers = pd.concat(all_dfs, ignore_index=True)
                all_tickers = all_tickers.drop_duplicates(subset=["ticker"])
                self._tap.ticker_cache["all_tickers"] = all_tickers
            else:
                self._tap.ticker_cache["all_tickers"] = pd.DataFrame(
                    columns=["ticker", "name", "segment"]
                )
        self.df_tickers = self._tap.ticker_cache["all_tickers"]
        self.cached_tickers = self.df_tickers["ticker"].drop_duplicates().tolist()

    def get_records(self, context: dict | None) -> list[dict]:
        self.fetch_and_cache_tickers()
        for record in self.df_tickers.to_dict(orient="records"):
            yield record


class PriceStream(BasePriceStream):
    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType, required=True),
        th.Property("timestamp_tz_aware", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("adj_close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("dividends", th.NumberType),
        th.Property("stock_splits", th.NumberType),
        th.Property("capital_gains", th.NumberType),
        th.Property("repaired", th.BooleanType),
        th.Property("replication_key", th.StringType),
    ).to_dict()
