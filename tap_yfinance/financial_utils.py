import logging
from datetime import datetime
import pandas as pd
import numpy as np
import yfinance as yf
from tap_yfinance.price_utils import clean_strings
import re
from tap_yfinance.config import *


class FinancialTap:

    ### TODO: date filters? ###

    def __init__(
        self, schema, ticker, name, config=None, yf_params=None, ticker_colname="ticker"
    ):
        self.schema = schema
        self.ticker = ticker
        self.name = name
        self.config = config
        self.yf_params = {} if yf_params is None else yf_params
        self.ticker_colname = ticker_colname

        super().__init__()

        if self.config is not None and "yf_cache_params" in self.config.get(self.name):
            rate_request_limit = (
                self.config.get(self.name)
                .get("yf_cache_params")
                .get("rate_request_limit")
            )
            rate_seconds_limit = (
                self.config.get(self.name)
                .get("yf_cache_params")
                .get("rate_seconds_limit")
            )

            from requests import Session
            from requests_cache import CacheMixin, SQLiteCache
            from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
            from pyrate_limiter import Duration, RequestRate, Limiter

            class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
                pass

            self.session = CachedLimiterSession(
                limiter=Limiter(
                    RequestRate(
                        rate_request_limit, Duration.SECOND * rate_seconds_limit
                    )
                ),
                bucket_class=MemoryQueueBucket,
                backend=SQLiteCache("yfinance.cache"),
            )

            self.yf_ticker_obj = yf.Ticker(self.ticker, session=self.session)

        else:
            self.yf_ticker_obj = yf.Ticker(self.ticker)

    @staticmethod
    def extract_ticker_tz_aware_timestamp(df, timestamp_column, ticker):
        """transformations are applied inplace to reduce memory usage"""
        logging.info(f"*** Running function extract_ticker_tz_aware_timestamp for ticker {ticker})")
        assert (
            "timezone" not in df.columns
        ), "timezone cannot be a pre-existing column in the extracted df."
        df["ticker"] = ticker
        df.columns = clean_strings(df.columns)
        df["timezone"] = str(df[timestamp_column].dt.tz)
        df.loc[:, f"{timestamp_column}_tz_aware"] = (
            df[timestamp_column].copy().dt.strftime("%Y-%m-%d %H:%M:%S%z")
        )
        df[timestamp_column] = pd.to_datetime(df[timestamp_column], utc=True)
        return df

    def get_analyst_price_targets(self, ticker):
        logging.info(f"*** Running function get_analyst_price_targets for ticker {ticker})")
        try:
            data = self.yf_ticker_obj.get_analyst_price_targets()
        except Exception:
            logging.warning(
                f"Error extracting data get_actions for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted", "ticker"])

        if isinstance(data, dict) and len(data):
            df = pd.DataFrame.from_dict(data, orient="index").T
            df["timestamp_extracted"] = datetime.utcnow()
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
        else:
            return pd.DataFrame()
        column_order = [
            "timestamp_extracted",
            "ticker",
            "current",
            "high",
            "low",
            "mean",
            "median",
        ]
        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
        return df[[i for i in column_order if i in df.columns]]

    def get_actions(self, ticker):
        logging.info(f"*** Running function get_actions for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_actions()
        except Exception:
            logging.warning(
                f"Error extracting data get_actions for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.reset_index().rename(columns={"Date": "timestamp"})
            self.extract_ticker_tz_aware_timestamp(df, "timestamp", ticker)
            df = df.replace([np.inf, -np.inf, np.nan], None)
        else:
            return pd.DataFrame(columns=["timestamp"])

        column_order = [
            "timestamp",
            "timestamp_tz_aware",
            "timezone",
            "ticker",
            "dividends",
            "stock_splits",
        ]

        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")

        return df[[i for i in column_order if i in df.columns]]

    def get_analyst_price_target(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        logging.info(f"*** Running function get_analyst_price_target for ticker {ticker})")
        return

    def get_balance_sheet(self, ticker):
        logging.info(f"*** Running function get_balance_sheet for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_balance_sheet()
        except Exception:
            logging.warning(
                f"Error extracting data get_balance_sheet for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis("date").reset_index()
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = clean_strings(df.columns)
            df = df.rename(
                columns={
                    "financial_assets_designatedas_fair_value_through_profitor_loss_total": "financial_assets_designatedas_fv_thru_profitor_loss_total"
                }
            )
            df.columns = [i.replace("p_p_e", "ppe") for i in df.columns]
        else:
            return pd.DataFrame(columns=["date"])

        column_order = BALANCE_SHEET_COLUMNS
        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")

        return df[[i for i in column_order if i in df.columns]]

    def get_balancesheet(self, ticker):
        """Same output as the method get_balance_sheet"""
        logging.info(f"*** Running function get_balancesheet for ticker {ticker})")
        return

    def basic_info(self, ticker):
        """Useless information"""
        logging.info(f"*** Running function basic_info for ticker {ticker})")
        return

    def get_calendar(self, ticker):
        """Returns calendar df"""
        logging.info(f"*** Running function get_calendar for ticker {ticker})")
        try:
            df = pd.DataFrame(self.yf_ticker_obj.get_calendar())
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = clean_strings(df.columns)
            df["ticker"] = ticker
            column_order = [
                "dividend_date",
                "ex_dividend_date",
                "earnings_date",
                "ticker",
                "earnings_high",
                "earnings_low",
                "earnings_average",
                "revenue_high",
                "revenue_low",
                "revenue_average",
            ]
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        except Exception:
            logging.warning(
                f"Error extracting data get_calendar for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(
                columns=["dividend_date", "ex_dividend_date", "earnings_date"]
            )

    def get_capital_gains(self, ticker):
        """Returns empty series"""
        logging.info(f"*** Running function get_capital_gains for ticker {ticker})")
        return

    def get_cash_flow(self, ticker):
        logging.info(f"*** Running function get_cash_flow for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_cash_flow()
            if isinstance(df, pd.DataFrame) and df.shape[0]:
                df = df.T.rename_axis("date").reset_index()
                df["ticker"] = ticker
                df.columns = clean_strings(df.columns)
                df.columns = [
                    i.replace("p_p_e", "ppe")
                    .replace("c_f_o", "cfo")
                    .replace("c_f_f", "cff")
                    .replace("c_f_i", "cfi")
                    for i in df.columns
                ]
                df = df.replace([np.inf, -np.inf, np.nan], None)
            else:
                return pd.DataFrame(columns=["date"])
            column_order = CASH_FLOW_COLUMNS
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        except Exception:
            logging.warning(
                f"Error extracting data get_cash_flow for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    def get_cashflow(self, ticker):
        """Same output as the method get_cash_flow"""
        logging.info(f"*** Running function get_cashflow for ticker {ticker})")
        return

    def get_dividends(self, ticker):
        logging.info(f"*** Running function get_dividends for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_dividends()
        except Exception:
            logging.warning(
                f"Error extracting data get_dividends for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp"])

        if isinstance(df, pd.Series) and df.shape[0]:
            df = df.rename_axis("timestamp").reset_index()
            df["ticker"] = ticker
            self.extract_ticker_tz_aware_timestamp(df, "timestamp", ticker)
            df = df.replace([np.inf, -np.inf, np.nan], None)
        else:
            return pd.DataFrame(columns=["timestamp"])

        df.columns = clean_strings(df.columns)
        column_order = ["timestamp", "timestamp_tz_aware", "timezone", "ticker", "dividends"]
        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
        return df[[i for i in column_order if i in df.columns]]

    def get_earnings(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        logging.info(f"*** Running function get_earnings for ticker {ticker})")
        return

    def get_earnings_estimate(self, ticker):
        logging.info(f"*** Running function get_earnings_estimate for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_earnings_estimate()
        except Exception:
            logging.warning(
                f"Error extracting data get_earnings_estimate for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.reset_index()
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df = df.replace([np.inf, -np.inf, np.nan], None)
        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

        df.columns = clean_strings(df.columns)
        column_order = [
            "timestamp_extracted",
            "ticker",
            "period",
            "avg",
            "low",
            "high",
            "year_ago_eps",
            "number_of_analysts",
            "growth",
        ]
        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
        return df[[i for i in column_order if i in df.columns]]

    def get_earnings_history(self, ticker):
        logging.info(f"*** Running function get_earnings_history for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_earnings_history()
        except Exception:
            logging.warning(
                f"Error extracting data get_earnings_history for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted", "quarter"])

        possible_columns1 = ["quarter", "eps_estimate", "ticker", "timestamp_extracted"]
        possible_columns2 = [
            "quarter",
            "eps_actual",
            "eps_estimate",
            "eps_difference",
            "surprise_percent",
            "ticker",
            "timestamp_extracted",
        ]

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.reset_index()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = clean_strings(df.columns)
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
        else:
            return pd.DataFrame(columns=["timestamp_extracted", "quarter"])

        if len(df.columns) == len(possible_columns1) and all(
            df.columns == possible_columns1
        ):
            column_order = ["quarter", "ticker", "eps_estimate", "timestamp_extracted"]
        elif len(df.columns) == len(possible_columns2) and all(
            df.columns == possible_columns2
        ):
            column_order = [
                "quarter",
                "ticker",
                "eps_estimate",
                "eps_actual",
                "eps_difference",
                "surprise_percent",
                "timestamp_extracted",
            ]
        else:
            raise ValueError(
                f"Error validating returned columns for earnings_history with ticker {ticker}."
            )

        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")

        return df[[i for i in column_order if i in df.columns]]

    def get_earnings_dates(self, ticker):
        logging.info(f"*** Running function get_earnings_dates for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_earnings_dates()
        except Exception:
            logging.warning(
                f"Error extracting get_earnings_dates as dictionary for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.rename_axis("timestamp").reset_index()
            df["ticker"] = ticker
            self.extract_ticker_tz_aware_timestamp(df, "timestamp", ticker)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace("e_p_s", "eps") for i in clean_strings(df.columns)]
            df.rename(columns={"surprise": "pct_surprise"}, inplace=True)
        else:
            return pd.DataFrame(columns=["timestamp"])

        column_order = [
            "timestamp",
            "timestamp_tz_aware",
            "timezone",
            "ticker",
            "eps_estimate",
            "reported_eps",
            "pct_surprise",
        ]

        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")

        return df[[i for i in column_order if i in df.columns]]

    def get_earnings_forecast(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        logging.info(f"*** Running function get_earnings_forecast for ticker {ticker})")
        return

    def get_earnings_trend(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        logging.info(f"*** Running function get_earnings_trend for ticker {ticker})")
        return

    def get_eps_revisions(self, ticker):
        logging.info(f"*** Running function get_eps_revisions for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_eps_revisions()
        except Exception:
            logging.warning(
                f"Error extracting get_earnings_dates as dictionary for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.reset_index()
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [
                i.replace("last7", "last_7").replace("7d", "7_d").replace("7D", "7_d")
                for i in clean_strings(df.columns)
            ]
            df.columns = [
                i.replace("last30", "last_30")
                .replace("30d", "30_d")
                .replace("30D", "30_d")
                for i in clean_strings(df.columns)
            ]
        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

        column_order = [
            "timestamp_extracted",
            "ticker",
            "period",
            "up_last_7_days",
            "down_last_7_days",
            "up_last_30_days",
            "down_last_30_days",
        ]

        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")

        return df[[i for i in column_order if i in df.columns]]

    def get_eps_trend(self, ticker):
        logging.info(f"*** Running function get_eps_trend for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_eps_trend()
        except Exception:
            logging.warning(
                f"Error extracting get_earnings_dates as dictionary for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted", "ticker"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.reset_index()
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = clean_strings([rename_days_ago(col) for col in df.columns])
            column_order = [
                "timestamp_extracted",
                "ticker",
                "period",
                "current",
                "days_ago_7",
                "days_ago_30",
                "days_ago_60",
                "days_ago_90",
            ]
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            return pd.DataFrame(columns=["timestamp_extracted", "ticker"])

    def get_funds_data(self, ticker):
        logging.info(f"*** Running function get_funds_data for ticker {ticker})")
        pass

    def get_growth_estimates(self, ticker):
        logging.info(f"*** Running function get_growth_estimates for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_growth_estimates()
        except Exception:
            logging.warning(
                f"Error extracting get_growth_estimates as dictionary for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.reset_index()
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = clean_strings([rename_days_ago(col) for col in df.columns])
            column_order = [
                "ticker",
                "period",
                "stock_trend",
                "index_trend",
                "timestamp_extracted",
            ]
        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
        return df[[i for i in column_order if i in df.columns]]

    def get_fast_info(self, ticker):
        logging.info(f"*** Running function get_fast_info for ticker {ticker})")
        try:
            df = pd.DataFrame.from_dict(
                dict(self.yf_ticker_obj.get_fast_info()), orient="index"
            ).T
        except Exception:
            logging.warning(
                f"Error extracting get_fast_info as dictionary for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df.rename(columns={"timezone": "extracted_timezone"}, inplace=True)

            df["timestamp_tz_aware"] = df.apply(
                lambda row: row["timestamp_extracted"].tz_localize(
                    row["extracted_timezone"]
                ),
                axis=1,
            )

            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = clean_strings(df.columns)

            column_order = [
                "currency",
                "day_high",
                "day_low",
                "exchange",
                "fifty_day_average",
                "last_price",
                "last_volume",
                "market_cap",
                "open",
                "previous_close",
                "quote_type",
                "regular_market_previous_close",
                "shares",
                "ten_day_average_volume",
                "three_month_average_volume",
                "extracted_timezone",
                "two_hundred_day_average",
                "year_change",
                "year_high",
                "year_low",
                "ticker",
                "timestamp_extracted",
                "timestamp_tz_aware",
            ]

            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df

        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

    def get_financials(self, ticker):
        logging.info(f"*** Running function get_financials for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_financials().T
        except Exception:
            logging.warning(
                f"Error extracting get_financials as dictionary for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.rename_axis("date").reset_index()
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [
                i.replace("e_b_i_t_d_a", "ebitda")
                .replace("e_p_s", "eps")
                .replace("e_b_i_t", "ebit")
                .replace("p_p_e", "ppe")
                .replace(
                    "diluted_n_i_availto_com_stockholders",
                    "diluted_ni_availto_com_stockholders",
                )
                for i in clean_strings(df.columns)
            ]
            first_cols = ["date", "ticker"]
        else:
            return pd.DataFrame(columns=["date"])

        column_order = FINANCIAL_COLUMNS
        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
        return df[[i for i in column_order if i in df.columns]]

    def get_history_metadata(self, ticker):
        logging.info(f"*** Running function get_history_metadata for ticker {ticker})")
        try:
            data = self.yf_ticker_obj.get_history_metadata()
        except Exception:
            logging.warning(
                f"Error extracting get_history_metadata as dictionary for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if len(data):
            try:
                if "tradingPeriods" in data.keys():
                    data["tradingPeriods"] = data["tradingPeriods"].to_dict()
                df = pd.Series({key: data[key] for key in data.keys()}).to_frame().T
                df.columns = clean_strings(df.columns)
                df = df.rename(columns={"symbol": "ticker"})
                df = df.replace([np.inf, -np.inf, np.nan], None)

                if "current_trading_period" in df.columns:
                    df_ctp = pd.json_normalize(df["current_trading_period"])
                    df_ctp.columns = clean_strings(df_ctp.columns)
                    df_ctp = df_ctp.add_prefix("current_trading_period_")
                    df = pd.concat([df, df_ctp], axis=1)
                    df = df.drop(["current_trading_period"], axis=1)

                if "trading_periods" in df.columns:
                    df_tp = pd.DataFrame().from_dict(df["trading_periods"].iloc[0])
                    df_tp = df_tp.add_prefix("trading_period_").reset_index(drop=True)
                    df = pd.concat([df, df_tp], axis=1).ffill()
                    df = df.drop("trading_periods", axis=1)

                df["timestamp_extracted"] = datetime.utcnow()

                column_order = HISTORY_METADATA_COLUMNS
                missing_columns = check_missing_columns(df, column_order)
                if len(missing_columns):
                    logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
                return df[[i for i in column_order if i in df.columns]]
            except Exception:
                logging.warning(
                    f"Error parsing get_history_metadata as dictionary for ticker {ticker}. Skipping..."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])

        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

    def get_info(self, ticker):
        logging.info(f"*** Running function get_info for ticker {ticker})")
        try:
            data = self.yf_ticker_obj.get_info()
        except Exception:
            logging.warning(
                f"Error extracting get_info for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if len(data):
            try:
                df = pd.DataFrame.from_dict(data, orient="index").T
                df.columns = clean_strings(df.columns)
                df["timestamp_extracted"] = datetime.utcnow()
                df["ticker"] = ticker
                df = df.rename(columns={"52_week_change": "change_52wk"})
                df = df.replace([np.inf, -np.inf, np.nan], None)

                column_order = INFO_COLUMNS
                missing_columns = check_missing_columns(df, column_order)
                if len(missing_columns):
                    logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")

                str_cols = [
                    "ticker",
                    "address1",
                    "city",
                    "state",
                    "zip",
                    "country",
                    "phone",
                    "website",
                    "industry",
                    "industry_key",
                    "industry_disp",
                    "sector",
                    "sector_key",
                    "sector_disp",
                    "long_business_summary",
                    "company_officers",
                    "ir_website",
                    "executive_team",
                    "currency",
                    "last_split_factor",
                    "quote_type",
                    "recommendation_key",
                    "financial_currency",
                    "symbol",
                    "language",
                    "region",
                    "type_disp",
                    "quote_source_name",
                    "custom_price_alert_confidence",
                    "corporate_actions",
                    "post_market_time",
                    "regular_market_time",
                    "exchange",
                    "message_board_id",
                    "exchange_timezone_name",
                    "exchange_timezone_short_name",
                    "market",
                    "average_analyst_rating",
                    "regular_market_day_range",
                    "full_exchange_name",
                    "fifty_two_week_range",
                    "short_name",
                    "long_name",
                    "display_name",
                ]

                str_cols = [i for i in str_cols if i in df.columns]
                df[str_cols] = df[str_cols].astype(str)

                return df[[i for i in column_order if i in df.columns]]
            except Exception:
                logging.warning(
                    f"Error parsing get_info as dictionary for ticker {ticker}. Skipping..."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])
        else:
            logging.warning(
                f"Data has no length in method get_info for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    def get_income_stmt(self, ticker):
        logging.info(f"*** Running function get_income_stmt for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_income_stmt()
        except Exception:
            logging.warning(
                f"Error extracting get_income_stmt as dictionary for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis("date").reset_index()
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [
                i.replace("e_b_i_t_d_a", "ebitda")
                .replace("e_p_s", "eps")
                .replace("e_b_i_t", "ebit")
                .replace(
                    "diluted_n_i_availto_com_stockholders",
                    "diluted_ni_availto_com_stockholders",
                )
                .replace("p_p_e", "ppe")
                for i in clean_strings(df.columns)
            ]
            column_order = INCOME_STMT_COLUMNS
        else:
            return pd.DataFrame(columns=["date"])
        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
        return df[[i for i in column_order if i in df.columns]]

    def get_incomestmt(self, ticker):
        """Same output as the method get_income_stmt"""
        logging.info(f"*** Running function get_incomestmt for ticker {ticker})")
        return

    def get_insider_purchases(self, ticker):
        logging.info(f"*** Running function get_insider_purchases for ticker {ticker})")
        column_order = [
            "ticker",
            "insider_purchases_last_6m",
            "shares",
            "trans",
            "timestamp_extracted",
        ]
        num_cols = ["shares", "trans"]
        try:
            df = self.yf_ticker_obj.get_insider_purchases()
            df["timestamp_extracted"] = datetime.now()
            df.columns = clean_strings(df.columns)
            df["ticker"] = ticker
            df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
            df = df.replace([np.inf, -np.inf, np.nan], None)
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        except Exception:
            return pd.DataFrame(columns=column_order)

    def get_insider_roster_holders(self, ticker):
        logging.info(f"*** Running function get_insider_roster_holders for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_insider_roster_holders()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace("u_r_l", "url") for i in clean_strings(df.columns)]
            df["ticker"] = ticker
            column_order = [
                "latest_transaction_date",
                "ticker",
                "name",
                "position",
                "url",
                "most_recent_transaction",
                "shares_owned_indirectly",
                "position_indirect_date",
                "shares_owned_directly",
                "position_direct_date",
            ]
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        except Exception:
            return pd.DataFrame()

    def get_insider_transactions(self, ticker):
        logging.info(f"*** Running function get_insider_transactions for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_insider_transactions()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace("u_r_l", "url") for i in clean_strings(df.columns)]
            df["ticker"] = ticker
            column_order = [
                "ticker",
                "start_date",
                "shares",
                "url",
                "text",
                "insider",
                "position",
                "transaction",
                "ownership",
                "value",
            ]
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        except Exception:
            return pd.DataFrame()

    def get_institutional_holders(self, ticker):
        logging.info(f"*** Running function get_institutional_holders for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_institutional_holders()
        except Exception:
            logging.warning(
                f"Could not extract institutional_holders for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date_reported"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df = df.rename(columns={"% Out": "pct_out"})
            df.columns = clean_strings(df.columns)
            column_order = [
                "date_reported",
                "ticker",
                "holder",
                "pct_change",
                "pct_held",
                "shares",
                "value",
            ]
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            logging.warning(
                f"Inconsistent fields for institutional_holders for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date_reported"])

    def get_isin(self, ticker):
        logging.info(f"*** Running function get_isin for ticker {ticker})")
        try:
            data = self.yf_ticker_obj.get_isin()
        except Exception:
            logging.warning(f"Could not extract isin for ticker {ticker}. Skipping...")
            return pd.DataFrame(columns=["timestamp_extracted"])
        df = pd.DataFrame.from_dict({"value": data}, orient="index").T
        df["timestamp_extracted"] = datetime.utcnow()
        df["ticker"] = ticker
        column_order = ["ticker", "timestamp_extracted", "value"]
        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
        return df[[i for i in column_order if i in df.columns]]

    def get_revenue_estimate(self, ticker):
        logging.info(f"*** Running function get_revenue_estimate for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_revenue_estimate()
        except Exception:
            logging.warning(
                f"Could not extract get_revenue_estimate for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.reset_index()
            df.columns = clean_strings(df.columns)
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            column_order = [
                "ticker",
                "timestamp_extracted",
                "period",
                "avg",
                "low",
                "high",
                "number_of_analysts",
                "year_ago_revenue",
                "growth",
            ]
        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")

        return df[[i for i in column_order if i in df.columns]]

    def get_sec_filings(self, ticker):
        logging.info(f"*** Running function get_sec_filings for ticker {ticker})")
        try:
            data = self.yf_ticker_obj.get_sec_filings()
        except Exception:
            logging.warning(
                f"Could not extract get_sec_filings for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if len(data):
            try:
                df = pd.DataFrame(data)
            except Exception:
                raise ValueError(
                    "Error in get_sec_filings! Could not convert raw data to pandas df."
                )
            df.columns = clean_strings(df.columns)
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df["exhibits"] = df["exhibits"].astype(str)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            column_order = [
                "ticker",
                "date",
                "epoch_date",
                "type",
                "title",
                "edgar_url",
                "exhibits",
                "max_age",
                "timestamp_extracted",
            ]
        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
        return df[[i for i in column_order if i in df.columns]]

    def get_major_holders(self, ticker):
        logging.info(f"*** Running function get_major_holders for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_major_holders()
        except Exception:
            logging.warning(
                f"Could not extract get_major_holders for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if isinstance(df, pd.DataFrame) and df.shape[0] and df.shape[1] == 2:
            df.columns = ["value", "breakdown"]
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            column_order = ["timestamp_extracted", "ticker", "breakdown", "value"]
            return df[[i for i in column_order if i in df.columns]]
        if (
            isinstance(df, (pd.DataFrame, pd.Series))
            and df.shape[0]
            and df.shape[1] == 1
        ):
            df = (
                df.rename_axis("breakdown")
                .reset_index()
                .rename(columns={"Value": "value"})
            )
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            column_order = ["timestamp_extracted", "ticker", "breakdown", "value"]
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            logging.warning(
                f"Inconsistent fields for get_major_holders for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    def get_mutualfund_holders(self, ticker):
        logging.info(f"*** Running function get_mutualfund_holders for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_mutualfund_holders()
        except Exception:
            logging.warning(
                f"Could not extract get_mutualfund_holders for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date_reported"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df.columns = clean_strings(df.columns)
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            column_order = [
                "date_reported",
                "ticker",
                "holder",
                "pct_change",
                "pct_held",
                "shares",
                "value",
            ]

            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            logging.warning(
                f"Inconsistent fields for get_mutualfund_holders for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date_reported"])

    def get_news(self, ticker):
        logging.info(f"*** Running function get_news for ticker {ticker})")
        try:
            df = pd.DataFrame(self.yf_ticker_obj.get_news())
        except Exception:
            logging.warning(
                f"Could not extract get_news for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date_reported"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df[["id", "content"]] = df[["id", "content"]].astype(str)
            df.columns = clean_strings(df.columns)
            df = df.replace([np.inf, -np.inf, np.nan], None)

            column_order = ["timestamp_extracted", "ticker", "id", "content"]
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            logging.warning(
                f"Inconsistent fields for get_news for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    def get_recommendations(self, ticker):
        logging.info(f"*** Running function get_recommendations for ticker {ticker})")
        column_order = [
            "ticker",
            "timestamp_extracted",
            "period",
            "strong_buy",
            "buy",
            "hold",
            "sell",
            "strong_sell",
        ]
        try:
            df = self.yf_ticker_obj.get_recommendations()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = clean_strings(df.columns)
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        except Exception:
            return pd.DataFrame(columns=column_order)

    def get_recommendations_summary(self, ticker):
        logging.info(f"*** Running function get_recommendations_summary for ticker {ticker})")
        column_order = [
            "ticker",
            "timestamp_extracted",
            "period",
            "strong_buy",
            "buy",
            "hold",
            "sell",
            "strong_sell",
        ]
        try:
            df = self.yf_ticker_obj.get_recommendations()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = clean_strings(df.columns)
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        except Exception:
            return pd.DataFrame(columns=column_order)

    def get_rev_forecast(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        logging.info(f"*** Running function get_rev_forecast for ticker {ticker})")
        return

    def get_shares(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        logging.info(f"*** Running function get_shares for ticker {ticker})")
        return

    def get_shares_full(self, ticker):
        logging.info(f"*** Running function get_shares_full for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_shares_full()
        except Exception:
            logging.warning(
                f"Could not extract get_shares_full for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp"])

        if isinstance(df, pd.Series) and df.shape[0]:
            df = df.reset_index()
            df.columns = ["timestamp", "amount"]
            df["ticker"] = ticker
            self.extract_ticker_tz_aware_timestamp(df, "timestamp", ticker)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            column_order = ["timestamp", "timestamp_tz_aware", "timezone", "ticker", "amount"]
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            return pd.DataFrame(columns=["timestamp"])

    def get_splits(self, ticker):
        logging.info(f"*** Running function get_splits for ticker {ticker})")
        column_order = [
            "timestamp",
            "timestamp_tz_aware",
            "timezone",
            "ticker",
            "stock_splits",
        ]

        try:
            df = self.yf_ticker_obj.get_splits()
        except Exception:
            logging.warning(
                f"Could not extract get_splits for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp"])

        if isinstance(df, pd.Series) and df.shape[0]:
            df = df.rename_axis("timestamp").reset_index()
            df["ticker"] = ticker
            self.extract_ticker_tz_aware_timestamp(df, "timestamp", ticker)
            df.columns = clean_strings(df.columns)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            return pd.DataFrame(columns=column_order)

    def get_sustainability(self, ticker):
        logging.info(f"*** Running function get_sustainability for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.get_sustainability()
        except Exception:
            logging.warning(
                f"Could not extract get_splits for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df.columns = clean_strings(df.columns)
            column_order = [
                "timestamp_extracted",
                "ticker",
                "max_age",
                "total_esg",
                "environment_score",
                "social_score",
                "governance_score",
                "rating_year",
                "rating_month",
                "highest_controversy",
                "peer_count",
                "esg_performance",
                "peer_group",
                "related_controversy",
                "peer_esg_score_performance",
                "peer_governance_performance",
                "peer_social_performance",
                "peer_environment_performance",
                "peer_highest_controversy_performance",
                "percentile",
                "environment_percentile",
                "social_percentile",
                "governance_percentile",
                "adult",
                "alcoholic",
                "animal_testing",
                "catholic",
                "controversial_weapons",
                "small_arms",
                "fur_leather",
                "gambling",
                "gmo",
                "military_contract",
                "nuclear",
                "pesticides",
                "palm_oil",
                "coal",
                "tobacco",
            ]
            
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            
            str_cols = [
                "related_controversy",
                "peer_esg_score_performance",
                "peer_governance_performance",
                "peer_social_performance",
                "peer_environment_performance",
                "peer_highest_controversy_performance",
            ]
            
            str_cols = [i for i in str_cols if i in df.columns]
            df[str_cols] = df[str_cols].astype(str)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            return df[[i for i in column_order if i in df.columns]]
        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

    def get_trend_details(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        logging.info(f"*** Running function get_trend_details for ticker {ticker})")
        return

    def ttm_cash_flow(self, ticker):
        logging.info(f"*** Running function ttm_cash_flow for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.ttm_cash_flow
        except Exception:
            logging.warning(
                f"Could not extract ttm_cash_flow for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis("date").reset_index()
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace("p_p_e", "ppe") for i in clean_strings(df.columns)]
            column_order = CASH_FLOW_COLUMNS
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

    def ttm_cashflow(self, ticker):
        """duplicate of ttm_cash_flow"""
        logging.info(f"*** Running function ttm_cashflow for ticker {ticker})")
        pass

    def ttm_financials(self, ticker):
        logging.info(f"*** Running function ttm_financials for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.ttm_financials.T
        except Exception:
            logging.warning(
                f"Error extracting ttm_financials as dictionary for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.rename_axis("date").reset_index()
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [
                i.replace("e_b_i_t_d_a", "ebitda")
                .replace("e_p_s", "eps")
                .replace("e_b_i_t", "ebit")
                .replace("p_p_e", "ppe")
                .replace(
                    "diluted_n_i_availto_com_stockholders",
                    "diluted_ni_availto_com_stockholders",
                )
                for i in clean_strings(df.columns)
            ]
            first_cols = ["date", "ticker"]
        else:
            return pd.DataFrame(columns=["date"])

        column_order = FINANCIAL_COLUMNS
        missing_columns = check_missing_columns(df, column_order)
        if len(missing_columns):
            logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
        return df[[i for i in column_order if i in df.columns]]

    def ttm_income_stmt(self, ticker):
        logging.info(f"*** Running function ttm_income_stmt for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.ttm_income_stmt
        except Exception:
            logging.warning(
                f"Could not extract ttm_income_stmt for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis("date").reset_index()
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [
                i.replace("e_b_i_t_d_a", "ebitda")
                .replace("e_p_s", "eps")
                .replace("e_b_i_t", "ebit")
                .replace(
                    "diluted_n_i_availto_com_stockholders",
                    "diluted_ni_availto_com_stockholders",
                )
                for i in clean_strings(df.columns)
            ]
            column_order = INCOME_STMT_COLUMNS
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            return pd.DataFrame(columns=["timestamp"])

    def ttm_incomestmt(self, ticker):
        """duplicate of ttm_income_stmt"""
        logging.info(f"*** Running function ttm_incomestmt for ticker {ticker})")
        pass

    def get_upgrades_downgrades(self, ticker):
        logging.info(f"*** Running function get_upgrades_downgrades for ticker {ticker})")
        column_order = [
            "grade_date",
            "ticker",
            "firm",
            "to_grade",
            "from_grade",
            "action",
        ]
        try:
            df = self.yf_ticker_obj.get_upgrades_downgrades()
            df = df.reset_index()
            df.columns = clean_strings(df.columns)
            df["ticker"] = ticker
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        except Exception:
            logging.warning(
                f"Could not extract get_upgrades_downgrades for ticker {ticker}. Skipping..."
            )
        return pd.DataFrame(columns=column_order)

    def option_chain(self, ticker):
        logging.info(f"*** Running function option_chain for ticker {ticker})")

        first_cols = [
            "last_trade_date",
            "last_trade_date_tz_aware",
            "timezone",
            "ticker",
            "option_type",
        ]

        num_tries = 3
        n = 0
        while n < num_tries:
            try:
                option_expiration_dates = self.yf_ticker_obj.options
                if len(option_expiration_dates):
                    for exp_date in option_expiration_dates:
                        option_chain_data = self.yf_ticker_obj.option_chain(
                            date=exp_date
                        )

                        if len(option_chain_data) == 3:
                            calls, puts, metadata = (
                                option_chain_data[0],
                                option_chain_data[1],
                                option_chain_data[2],
                            )
                            assert isinstance(calls, pd.DataFrame) and isinstance(
                                puts, pd.DataFrame
                            ), "calls or puts are not a dataframe!"
                            calls["option_type"] = "call"
                            puts["option_type"] = "put"
                            if all(calls.columns == puts.columns):
                                df_options = pd.concat([calls, puts]).reset_index(
                                    drop=True
                                )
                                df_options["metadata"] = str(metadata)
                                df_options["timestamp_extracted"] = datetime.utcnow()
                                self.extract_ticker_tz_aware_timestamp(
                                    df_options, "last_trade_date", ticker
                                )
                                df_options = df_options.replace(
                                    [np.inf, -np.inf, np.nan], None
                                )
                                df_options.columns = clean_strings(df_options.columns)
                                column_order = [
                                    "last_trade_date",
                                    "last_trade_date_tz_aware",
                                    "timezone",
                                    "ticker",
                                    "option_type",
                                    "contract_symbol",
                                    "strike",
                                    "last_price",
                                    "bid",
                                    "ask",
                                    "change",
                                    "percent_change",
                                    "volume",
                                    "open_interest",
                                    "implied_volatility",
                                    "in_the_money",
                                    "contract_size",
                                    "currency",
                                    "metadata",
                                    "timestamp_extracted",
                                ]
                                missing_columns = check_missing_columns(df, column_order)
                                if len(missing_columns):
                                    logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
                                return df_options[column_order]
                            else:
                                raise ValueError(
                                    "Error parsing option_chain. Column order of calls and puts do not match."
                                )
                        else:
                            raise ValueError(
                                "Error parsing option_chain. Check if data passed changed"
                            )
                else:
                    return pd.DataFrame(columns=["last_trade_date"])

            except Exception:
                n += 1
                if n < num_tries:
                    logging.warning(
                        f"try-catch failed {n} times for method option_chain for ticker {ticker}. Trying again..."
                    )
                else:
                    logging.warning(
                        f"try-catch failed {n} times for method option_chain for ticker {ticker}. Skipping..."
                    )
                    return pd.DataFrame(columns=["last_trade_date"])

    def options(self, ticker):
        logging.info(f"*** Running function options for ticker {ticker})")
        num_tries = 3
        n = 0
        while n < num_tries:
            try:
                option_expiration_dates = self.yf_ticker_obj.options
                if option_expiration_dates:
                    df = pd.DataFrame(
                        option_expiration_dates, columns=["expiration_date"]
                    )
                    df["ticker"] = ticker
                    df["timestamp_extracted"] = datetime.utcnow()
                    df["expiration_date"] = pd.to_datetime(df["expiration_date"])
                    df = df.replace([np.inf, -np.inf, np.nan], None)
                    column_order = ["timestamp_extracted", "ticker", "expiration_date"]
                    missing_columns = check_missing_columns(df, column_order)
                    if len(missing_columns):
                        logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
                    return df[[i for i in column_order if i in df.columns]]
                else:
                    return pd.DataFrame(
                        columns=["timestamp_extracted", "ticker", "expiration_date"]
                    )
            except Exception:
                n += 1
                if n < num_tries:
                    logging.warning(
                        f"try-catch failed {n} times for method options for ticker {ticker}. Trying again..."
                    )
                else:
                    logging.warning(
                        f"try-catch failed {n} times for method options for ticker {ticker}. Skipping..."
                    )
                    return pd.DataFrame(
                        columns=["timestamp_extracted", "ticker", "expiration_date"]
                    )

    def quarterly_balance_sheet(self, ticker):
        logging.info(f"*** Running function quarterly_balance_sheet for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.quarterly_balance_sheet
        except Exception:
            logging.warning(
                f"Could not extract quarterly_balance_sheet for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis("date").reset_index()
            df["ticker"] = ticker
            df.columns = [i.replace("p_p_e", "ppe") for i in clean_strings(df.columns)]
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df = df.rename(
                columns={
                    "financial_assets_designatedas_fair_value_through_profitor_loss_total": "financial_assets_designatedas_fv_thru_profitor_loss_total"
                }
            )
            column_order = BALANCE_SHEET_COLUMNS
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            return pd.DataFrame(columns=["timestamp"])

    def quarterly_balancesheet(self, ticker):
        """Same output as the method quarterly_balance_sheet"""
        logging.info(f"*** Running function quarterly_balancesheet for ticker {ticker})")
        return

    def quarterly_cash_flow(self, ticker):
        logging.info(f"*** Running function quarterly_cash_flow for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.quarterly_cash_flow
        except Exception:
            logging.warning(
                f"Could not extract quarterly_cash_flow for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis("date").reset_index()
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace("p_p_e", "ppe") for i in clean_strings(df.columns)]
            column_order = CASH_FLOW_COLUMNS
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            return pd.DataFrame(columns=["timestamp"])

    def quarterly_cashflow(self, ticker):
        """Same output as the method quarterly_cash_flow"""
        logging.info(f"*** Running function quarterly_cashflow for ticker {ticker})")
        return

    def quarterly_financials(self, ticker):
        logging.info(f"*** Running function quarterly_financials for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.quarterly_financials
        except Exception:
            logging.warning(
                f"Could not extract quarterly_financials for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis("date").reset_index()
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [
                i.replace("e_b_i_t_d_a", "ebitda")
                .replace("e_p_s", "eps")
                .replace("e_b_i_t", "ebit")
                .replace(
                    "diluted_n_i_availto_com_stockholders",
                    "diluted_ni_availto_com_stockholders",
                )
                for i in clean_strings(df.columns)
            ]
            column_order = FINANCIAL_COLUMNS
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            return pd.DataFrame(columns=["date"])

    def quarterly_income_stmt(self, ticker):
        logging.info(f"*** Running function quarterly_income_stmt for ticker {ticker})")
        try:
            df = self.yf_ticker_obj.quarterly_income_stmt
        except Exception:
            logging.warning(
                f"Could not extract quarterly_income_stmt for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis("date").reset_index()
            df["ticker"] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [
                i.replace("e_b_i_t_d_a", "ebitda")
                .replace("e_p_s", "eps")
                .replace("e_b_i_t", "ebit")
                .replace(
                    "diluted_n_i_availto_com_stockholders",
                    "diluted_ni_availto_com_stockholders",
                )
                for i in clean_strings(df.columns)
            ]
            column_order = INCOME_STMT_COLUMNS
            missing_columns = check_missing_columns(df, column_order)
            if len(missing_columns):
                logging.warning(f"*** MISSING COLUMNS: {missing_columns} ***")
            return df[[i for i in column_order if i in df.columns]]
        else:
            return pd.DataFrame(columns=["timestamp"])

    def quarterly_incomestmt(self, ticker):
        """Same output as the method quarterly_income_stmt"""
        logging.info(f"*** Running function quarterly_incomestmt for ticker {ticker})")
        return

    def session(self, ticker):
        """Returns NoneType."""
        logging.info(f"*** Running function session for ticker {ticker})")
        return


def rename_days_ago(col):
    match = re.match(r"^(\d+)[Dd]ays[Aa]go$", col)
    if match:
        return f"days_ago_{match.group(1)}"
    return col

def check_missing_columns(df, column_order):
    df_columns = set(df.columns)
    expected_columns = set(column_order)

    missing_in_df = expected_columns - df_columns
    missing_in_order = df_columns - expected_columns

    return {
        "missing_in_df": missing_in_df,
        "missing_in_column_order": missing_in_order
    }
