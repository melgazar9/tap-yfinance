import logging
from datetime import datetime
import pandas as pd
import numpy as np
import yfinance as yf
from tap_yfinance.price_utils import clean_strings
import re

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
        return df[column_order]

    def get_actions(self, ticker):
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
        return df[column_order]

    def get_analyst_price_target(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        return

    def get_balance_sheet(self, ticker):
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

        column_order = ["date", "ticker"] + [
            i for i in df.columns if i not in ["date", "ticker"]
        ]
        return df[column_order]

    def get_balancesheet(self, ticker):
        """Same output as the method get_balance_sheet"""
        return

    def basic_info(self, ticker):
        """Useless information"""
        return

    def get_calendar(self, ticker):
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
            return df[column_order]
        except Exception:
            logging.warning(
                f"Error extracting data get_calendar for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(
                columns=["dividend_date", "ex_dividend_date", "earnings_date"]
            )

    def get_capital_gains(self, ticker):
        """Returns empty series"""
        return

    def get_cash_flow(self, ticker):
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
            column_order = ["date", "ticker"] + sorted(
                [i for i in df.columns if i not in ["date", "ticker"]]
            )
            return df[column_order]
        except Exception:
            logging.warning(
                f"Error extracting data get_cash_flow for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date"])

    def get_cashflow(self, ticker):
        """Same output as the method get_cash_flow"""
        return

    def get_dividends(self, ticker):
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
        return df[
            ["timestamp", "timestamp_tz_aware", "timezone", "ticker", "dividends"]
        ]

    def get_earnings(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        return

    def get_earnings_estimate(self, ticker):
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
        return df[column_order]

    def get_earnings_history(self, ticker):
        try:
            df = self.yf_ticker_obj.get_earnings_history()
        except Exception:
            logging.warning(
                f"Error extracting data get_earnings_history for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted", "quarter"])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.reset_index()
            df["ticker"] = ticker
            df["timestamp_extracted"] = datetime.utcnow()
            df = df.replace([np.inf, -np.inf, np.nan], None)
        else:
            return pd.DataFrame(columns=["timestamp_extracted", "quarter"])

        df.columns = clean_strings(df.columns)
        column_order = [
            "quarter",
            "ticker",
            "eps_actual",
            "eps_estimate",
            "eps_difference",
            "surprise_percent",
            "timestamp_extracted",
        ]
        return df[column_order]

    def get_earnings_dates(self, ticker):
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

        return df[column_order]

    def get_earnings_forecast(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        return

    def get_earnings_trend(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        return

    def get_eps_revisions(self, ticker):
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

        return df[column_order]

    def get_eps_trend(self, ticker):
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
            return df[column_order]
        else:
            return pd.DataFrame(columns=["timestamp_extracted", "ticker"])

    def get_funds_data(self, ticker):
        pass

    def get_growth_estimates(self, ticker):
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

        return df[column_order]

    def get_fast_info(self, ticker):
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
            return df

        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

    def get_financials(self, ticker):
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

        column_order = first_cols + sorted(
            [i for i in df.columns if i not in first_cols]
        )
        return df[column_order]

    def get_history_metadata(self, ticker):
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

                first_cols = ["ticker", "timezone", "currency"]
                column_order = first_cols + [
                    i for i in df.columns if i not in first_cols
                ]
                return df[column_order]
            except Exception:
                logging.warning(
                    f"Error parsing get_history_metadata as dictionary for ticker {ticker}. Skipping..."
                )
                return pd.DataFrame(columns=["timestamp_extracted"])

        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

    def get_income_stmt(self, ticker):
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
            column_order = ["date", "ticker"] + sorted(
                [i for i in df.columns if i not in ["date", "ticker"]]
            )
        else:
            return pd.DataFrame(columns=["date"])

        return df[column_order]

    def get_incomestmt(self, ticker):
        """Same output as the method get_income_stmt"""
        return

    def get_info(self, ticker):
        """Returns NoneType."""
        return

    def get_insider_purchases(self, ticker):
        column_order = [
            "ticker",
            "insider_purchases_last_6m",
            "shares",
            "trans",
            "timestamp_extracted",
        ]
        try:
            df = self.yf_ticker_obj.get_insider_purchases()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df["timestamp_extracted"] = datetime.now()
            df.columns = clean_strings(df.columns)
            df["ticker"] = ticker
            return df[column_order]
        except Exception:
            return pd.DataFrame(columns=column_order)

    def get_insider_roster_holders(self, ticker):
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
            return df[column_order]
        except Exception:
            return pd.DataFrame()

    def get_insider_transactions(self, ticker):
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
            return df[column_order]
        except Exception:
            return pd.DataFrame()

    def get_institutional_holders(self, ticker):
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
            mismatched_columns = [i for i in df.columns if i not in column_order]
            if len(mismatched_columns):
                logging.warning(
                    f"There exists columns in mutualfund_holders that are not in schema {mismatched_columns}"
                )
            return df[column_order]
        else:
            logging.warning(
                f"Inconsistent fields for institutional_holders for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date_reported"])

    def get_isin(self, ticker):
        try:
            data = self.yf_ticker_obj.get_isin()
        except Exception:
            logging.warning(f"Could not extract isin for ticker {ticker}. Skipping...")
            return pd.DataFrame(columns=["timestamp_extracted"])
        df = pd.DataFrame.from_dict({"value": data}, orient="index").T
        df["timestamp_extracted"] = datetime.utcnow()
        df["ticker"] = ticker
        return df[["ticker", "timestamp_extracted", "value"]]

    def get_revenue_estimate(self, ticker):
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

        return df[column_order]

    def get_sec_filings(self, ticker):
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

        return df[column_order]

    def get_major_holders(self, ticker):
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
            return df[column_order]
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
            return df[column_order]
        else:
            logging.warning(
                f"Inconsistent fields for get_major_holders for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    def get_mutualfund_holders(self, ticker):
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
            mismatched_columns = [i for i in df.columns if i not in column_order]
            if len(mismatched_columns):
                logging.warning(
                    f"There exists columns in mutualfund_holders that are not in schema {mismatched_columns}"
                )
            return df[column_order]
        else:
            logging.warning(
                f"Inconsistent fields for get_mutualfund_holders for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["date_reported"])

    def get_news(self, ticker):
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

            return df[column_order]
        else:
            logging.warning(
                f"Inconsistent fields for get_news for ticker {ticker}. Skipping..."
            )
            return pd.DataFrame(columns=["timestamp_extracted"])

    def get_recommendations(self, ticker):
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
            return df[column_order]
        except Exception:
            return pd.DataFrame(columns=column_order)

    def get_recommendations_summary(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        return

    def get_rev_forecast(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        return

    def get_shares(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        return

    def get_shares_full(self, ticker):
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
            return df[
                ["timestamp", "timestamp_tz_aware", "timezone", "ticker", "amount"]
            ]
        else:
            return pd.DataFrame(columns=["timestamp"])

    def get_splits(self, ticker):
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
            return df[column_order]
        else:
            return pd.DataFrame(columns=column_order)

    def get_sustainability(self, ticker):
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
            str_cols = [
                "related_controversy",
                "peer_esg_score_performance",
                "peer_governance_performance",
                "peer_social_performance",
                "peer_environment_performance",
                "peer_highest_controversy_performance",
            ]
            df[str_cols] = df[str_cols].astype(str)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            column_order = ["timestamp_extracted", "ticker"] + [
                i for i in df.columns if i not in ["timestamp_extracted", "ticker"]
            ]
            return df[column_order]
        else:
            return pd.DataFrame(columns=column_order)
        return df[column_order]

    def get_trend_details(self, ticker):
        """yfinance.exceptions.YFNotImplementedError"""
        return

    def ttm_cash_flow(self, ticker):
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
            column_order = ["date", "ticker"] + sorted(
                [i for i in df.columns if i not in ["date", "ticker"]]
            )
            return df[column_order]
        else:
            return pd.DataFrame(columns=["timestamp_extracted"])

    def ttm_cashflow(self, ticker):
        """ duplicate of ttm_cash_flow """
        pass

    def ttm_financials(self, ticker):
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

        column_order = first_cols + sorted(
            [i for i in df.columns if i not in first_cols]
        )
        return df[column_order]

    def ttm_income_stmt(self, ticker):
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
            column_order = ["date", "ticker"] + sorted(
                [i for i in df.columns if i not in ["date", "ticker"]]
            )
            return df[column_order]
        else:
            return pd.DataFrame(columns=["timestamp"])

    def ttm_incomestmt(self, ticker):
        pass

    def get_upgrades_downgrades(self, ticker):
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
            return df[column_order]
        except Exception:
            logging.warning(
                f"Could not extract get_upgrades_downgrades for ticker {ticker}. Skipping..."
            )
        return pd.DataFrame(columns=column_order)

    def option_chain(self, ticker):
        # TODO: clean option extraction

        first_cols = [
            "last_trade_date",
            "last_trade_date_tz_aware",
            "timezone",
            "timestamp_extracted",
            "ticker",
        ]

        num_tries = 3
        n = 0
        while n < num_tries:
            try:
                df = pd.DataFrame()
                option_expiration_dates = self.yf_ticker_obj.options
                if option_expiration_dates and len(option_expiration_dates):
                    for exp_date in option_expiration_dates:
                        option_chain_data = self.yf_ticker_obj.option_chain(
                            date=exp_date
                        )
                        if len(option_chain_data):
                            for ocd in option_chain_data[0:-1]:  # exclude metadata
                                ocd.columns = clean_strings(ocd.columns)
                                ocd["last_trade_date_tz_aware"] = ocd[
                                    "last_trade_date"
                                ].copy()
                                ocd["last_trade_date"] = pd.to_datetime(
                                    ocd["last_trade_date"], utc=True
                                )
                                df = pd.concat([df, ocd])
                                df["ticker"] = ticker
                                df["timestamp_extracted"] = datetime.utcnow()
                                df = df.drop_duplicates()
                                df["metadata"] = str(option_chain_data[-1])
                                # df["metadata"] = df["metadata"].astype(str)
                        try:
                            df.columns = clean_strings(df.columns)
                            self.extract_ticker_tz_aware_timestamp(
                                df, "last_trade_date", ticker
                            )
                            df = df.replace([np.inf, -np.inf, np.nan], None)

                            column_order = first_cols + [
                                i for i in df.columns if i not in first_cols
                            ]
                            return df[column_order]

                        except Exception:
                            return pd.DataFrame(columns=["last_trade_date"])
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
                    return df[["timestamp_extracted", "ticker", "expiration_date"]]
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
            column_order = ["date", "ticker"] + sorted(
                [i for i in df.columns if i not in ["date", "ticker"]]
            )
            return df[column_order]
        else:
            return pd.DataFrame(columns=["timestamp"])

    def quarterly_balancesheet(self, ticker):
        """Same output as the method quarterly_balance_sheet"""
        return

    def quarterly_cash_flow(self, ticker):
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
            column_order = ["date", "ticker"] + sorted(
                [i for i in df.columns if i not in ["date", "ticker"]]
            )
            return df[column_order]
        else:
            return pd.DataFrame(columns=["timestamp"])

    def quarterly_cashflow(self, ticker):
        """Same output as the method quarterly_cash_flow"""
        return

    def quarterly_financials(self, ticker):
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
            column_order = ["date", "ticker"] + sorted(
                [i for i in df.columns if i not in ["date", "ticker"]]
            )
            return df[column_order]
        else:
            return pd.DataFrame(columns=["date"])

    def quarterly_income_stmt(self, ticker):
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
            column_order = ["date", "ticker"] + sorted(
                [i for i in df.columns if i not in ["date", "ticker"]]
            )
            return df[column_order]
        else:
            return pd.DataFrame(columns=["timestamp"])

    def quarterly_incomestmt(self, ticker):
        """Same output as the method quarterly_income_stmt"""
        return

    def session(self, ticker):
        """Returns NoneType."""
        return


def rename_days_ago(col):
    match = re.match(r"^(\d+)[Dd]ays[Aa]go$", col)
    if match:
        return f"days_ago_{match.group(1)}"
    return col
