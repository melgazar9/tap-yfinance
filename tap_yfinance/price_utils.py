from datetime import datetime, timedelta
import time
import logging
import pandas as pd
import numpy as np
import yfinance as yf
from pytickersymbols import PyTickerSymbols
from pandas_datareader import data as pdr
import re
from requests.exceptions import ChunkedEncodingError

import requests


class PriceTap:
    """
    Parameters
    ----------
    yf_params: dict - passed to yf.Ticker(<ticker>).history(**yf_params) - see docs for yfinance ticker.history() params
    ticker_colname: str of column name to set of output yahoo ticker columns
    """

    def __init__(
        self, schema, config, name, ticker=None, yf_params=None, ticker_colname="ticker"
    ):
        self.schema = schema
        self.ticker = ticker
        self.name = name
        self.config = config
        self.yf_params = {} if yf_params is None else yf_params
        self.ticker_colname = ticker_colname

        self.column_order = list(self.schema.get("properties").keys())

        super().__init__()

        if (
            isinstance(self.ticker, str)
            and self.config is not None
            and "yf_cache_params" in self.config.get(self.name)
        ):
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
                backend=SQLiteCache("~/yfinance.cache"),
            )

            self.yf_ticker_obj = yf.Ticker(self.ticker, session=self.session)

        elif self.ticker is not None:
            self.yf_ticker_obj = yf.Ticker(self.ticker)
        else:
            self.yf_ticker_obj = None

        if "prepost" not in self.yf_params.keys():
            self.yf_params["prepost"] = True
        if "start" not in self.yf_params.keys():
            self.yf_params["start"] = "1950-01-01"

        self.start_date = self.yf_params["start"]

        assert (
            pd.Timestamp(self.start_date) <= datetime.today()
        ), "Start date cannot be after the current date!"

        self.n_requests = 0
        self.failed_ticker_downloads = {}

    def _request_limit_check(self):
        """
        Description
        -----------
        Check if too many requests were made to yfinance within their allowed number of requests.
        """

        self.request_start_timestamp = datetime.now()
        self.current_runtime_seconds = (
            datetime.now() - self.request_start_timestamp
        ).seconds

        if self.n_requests > 1900 and self.current_runtime_seconds > 3500:
            logging.info(
                f"\nToo many requests per hour. Pausing requests for {self.current_runtime_seconds} seconds.\n"
            )
            time.sleep(np.abs(3600 - self.current_runtime_seconds))
        if self.n_requests > 45000 and self.current_runtime_seconds > 85000:
            logging.info(
                f"\nToo many requests per day. Pausing requests for {self.current_runtime_seconds} seconds.\n"
            )
            time.sleep(np.abs(86400 - self.current_runtime_seconds))
        return self

    def download_price_history(self, ticker, yf_params=None) -> pd.DataFrame():
        """
        Description
        -----------
        Download a single stock price ticker from the yfinance python library.
        Minor transformations happen:
            - Add column ticker to show which ticker has been pulled
            - Set start date to the minimum start date allowed by yfinance for that ticker (passed in yf_params)
            - Clean column names
            - Set tz_aware timestamp column to be a string
        """

        yf_params = self.yf_params.copy() if yf_params is None else yf_params.copy()
        assert (
            "interval" in yf_params.keys()
        ), "must pass interval parameter to yf_params"

        if yf_params["interval"] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_params["interval"]] = []

        if "start" not in yf_params.keys():
            yf_params["start"] = "1950-01-01 00:00:00"
            logging.info(
                f"\n*** YF params start set to 1950-01-01 for ticker {ticker}! ***\n"
            )

        yf_params["start"] = get_valid_yfinance_start_timestamp(
            interval=yf_params["interval"], start=yf_params["start"]
        )

        try:
            df = self.yf_ticker_obj.history(**yf_params).rename_axis(index="timestamp")
            df.columns = clean_strings(df.columns)

            self.n_requests += 1
            df.loc[:, self.ticker_colname] = ticker
            df = df.reset_index()
            df.loc[:, "timestamp_tz_aware"] = df["timestamp"].copy()
            df.loc[:, "timezone"] = str(df["timestamp_tz_aware"].dt.tz)
            df["timestamp_tz_aware"] = df["timestamp_tz_aware"].dt.strftime(
                "%Y-%m-%d %H:%M:%S%z"
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

            if df is not None and not df.shape[0]:
                self.failed_ticker_downloads[yf_params["interval"]].append(ticker)
                return pd.DataFrame(columns=self.column_order)

            df = df.replace(
                [np.inf, -np.inf, np.nan], None
            )  # None can be handled by json.dumps but inf and NaN can't be
            df["replication_key"] = (
                df["ticker"] + "|" + df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            )
            df = df[self.column_order]
            return df

        except Exception:
            self.failed_ticker_downloads[yf_params["interval"]].append(ticker)
            return pd.DataFrame(columns=self.column_order)

    def download_price_history_wide(self, tickers, yf_params):
        yf_params = self.yf_params.copy() if yf_params is None else yf_params.copy()

        assert (
            "interval" in yf_params.keys()
        ), "must pass interval parameter to yf_params"

        if yf_params["interval"] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_params["interval"]] = []

        if "start" not in yf_params.keys():
            yf_params["start"] = "1950-01-01 00:00:00"
            logging.info(
                f"\n*** YF params start set to 1950-01-01 for ticker {ticker}! ***\n"
            )

        yf_params["start"] = get_valid_yfinance_start_timestamp(
            interval=yf_params["interval"], start=yf_params["start"]
        )

        yf.pdr_override()

        df = (
            pdr.get_data_yahoo(tickers, progress=False, **yf_params)
            .rename_axis(index="timestamp")
            .reset_index()
        )
        self.n_requests += 1

        df.columns = flatten_multindex_columns(df)
        df.loc[:, "timestamp_tz_aware"] = df["timestamp"].copy()
        df.loc[:, "timezone"] = str(df["timestamp_tz_aware"].dt.tz)
        df["timestamp_tz_aware"] = df["timestamp_tz_aware"].dt.strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        if df is not None and not df.shape[0]:
            self.failed_ticker_downloads[yf_params["interval"]].append(ticker)
            return pd.DataFrame(columns=self.column_order)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        return df


class TickerDownloader:
    """
    Description
    -----------
    Class to download PyTickerSymbols attempting to link Yahoo tickers to SEC ticker symbols into a single dataframe.
    A mapping between all symbols is returned when calling the method generate_yahoo_sec_tickermap().
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def download_pts_stock_tickers():
        """
        Description
        -----------
        Download py-ticker-symbols tickers
        """
        pts = PyTickerSymbols()
        all_getters = list(
            filter(
                lambda x: (
                    x.endswith("_yahoo_tickers") or x.endswith("_google_tickers")
                ),
                dir(pts),
            )
        )

        all_tickers = {"yahoo_tickers": [], "google_tickers": []}
        for t in all_getters:
            if t.endswith("google_tickers"):
                all_tickers["google_tickers"].append((getattr(pts, t)()))
            elif t.endswith("yahoo_tickers"):
                all_tickers["yahoo_tickers"].append((getattr(pts, t)()))
        all_tickers["google_tickers"] = flatten_list(all_tickers["google_tickers"])
        all_tickers["yahoo_tickers"] = flatten_list(all_tickers["yahoo_tickers"])
        if len(all_tickers["yahoo_tickers"]) == len(all_tickers["google_tickers"]):
            all_tickers = pd.DataFrame(all_tickers)
        else:
            all_tickers = pd.DataFrame(
                dict([(k, pd.Series(v)) for k, v in all_tickers.items()])
            )

        all_tickers = (
            all_tickers.rename(
                columns={
                    "yahoo_tickers": "yahoo_ticker",
                    "google_tickers": "google_ticker",
                }
            )
            .sort_values(by=["yahoo_ticker", "google_ticker"])
            .drop_duplicates()
        )
        all_tickers = all_tickers.replace([-np.inf, np.inf, np.nan], None)
        all_tickers.columns = ["yahoo_ticker_pts", "google_ticker_pts"]
        return all_tickers

    @staticmethod
    def download_crypto_tickers(num_currencies_per_loop=250):
        """
        Description
        -----------
        Download cryptocurrency pairs
        """

        from requests_html import HTMLSession

        session = HTMLSession()

        df = pd.DataFrame()

        seen_symbols = set()

        start = 0
        while True:
            try:
                resp = session.get(
                    f"https://finance.yahoo.com/markets/crypto/all/?start={start}&count={num_currencies_per_loop}"
                )

                df_i = pd.read_html(resp.html.raw_html)[0]
                df = pd.concat([df, df_i], axis=0)

                if start > 0 and (
                    df_i.iloc[0]["Symbol"] == "BTC-USD Bitcoin USD"
                    or df_i.empty
                    or all(df_i["Symbol"].isin(list(seen_symbols)))
                ):
                    break

                seen_symbols.update(set(df_i["Symbol"]))
                start += num_currencies_per_loop
            except ChunkedEncodingError as e:
                logging.warning(f"ChunkedEncodingError encountered: {e}. Retrying...")
                time.sleep(5)
                continue

        session.close()

        df = df.drop_duplicates().reset_index(drop=True)

        df = df.rename(
            columns={
                "Symbol": "ticker",
                "% Change": "pct_change",
                "Change %": "pct_change",
                "Volume in Currency (Since 0:00 UTC)": "volume_in_currency_since_0_00_utc",
                "Total Volume All Currencies (24hr)": "total_volume_all_currencies_24h",
                "52 Wk Change %": "change_pct_52wk",
                "52 Wk Range": "range_52wk",
            }
        )

        df.columns = clean_strings(df.columns)
        df = df.dropna(how="all", axis=1)

        # df.loc[:, "bloomberg_ticker"] = df["name"].apply(lambda x: f"{x[4:]}-{x[0:3]}")
        if df["ticker"].iloc[-1][-2] == "=" and "name" not in df.columns:
            df["name"] = df["ticker"].str.split("=").apply(lambda x: x[0])

        if len(df["price"].iloc[0].split(" ")) == 3:
            if df["change"].isnull().any():
                df["change"] = df["change"].fillna(
                    df["price"].apply(lambda x: x.split(" ")[1])
                )
            if df["pct_change"].isnull().any():
                df["pct_change"] = df["pct_change"].fillna(
                    df["price"]
                    .apply(lambda x: x.split(" ")[2])
                    .str.replace("(", "")
                    .str.replace(")", "")
                )
            # df["price"] = df["price"].apply(lambda x: x.split(" ")[0]).str.replace(",", "").astype(float)
            df["price"] = df["price"].apply(lambda x: x.split(" ")[0]).astype(str)

        # df["ticker"] = df["ticker"].str.split(" ").apply(lambda x: x[0])

        df = df.replace([np.inf, -np.inf, np.nan], None)

        str_cols = [
            "ticker",
            "price",
            "change",
            "pct_change",
            "market_cap",
            "volume",
            "volume_in_currency_24hr",
            "total_volume_all_currencies_24h",
            "circulating_supply",
            "change_pct_52wk",
            "name",
        ]

        df[str_cols] = df[str_cols].astype(str)

        column_order = [
            "ticker",
            "name",
            "price",
            "change",
            "pct_change",
            "market_cap",
            "volume",
            "volume_in_currency_24hr",
            "total_volume_all_currencies_24h",
            "circulating_supply",
            "change_pct_52wk",
        ]
        return df[column_order]

    @staticmethod
    def download_top_250_crypto_tickers(num_currencies=250):
        """
        Description
        -----------
        Download the top 250 cryptocurrencies
        Note: At the time of coding, setting num_currencies higher than 250 results in only 25 crypto tickers returned.
        """

        from requests_html import HTMLSession

        session = HTMLSession()

        resp = session.get(
            f"https://finance.yahoo.com/markets/crypto/all/?start=0&count={num_currencies}"
        )
        tables = pd.read_html(resp.html.raw_html)
        session.close()

        df = tables[0].copy()
        df = df.rename(
            columns={
                "Symbol": "ticker",
                "% Change": "pct_change",
                "Change %": "pct_change",
                "Volume in Currency (Since 0:00 UTC)": "volume_in_currency_since_0_00_utc",
                "Total Volume All Currencies (24hr)": "total_volume_all_currencies_24h",
                "52 Wk Change %": "change_pct_52wk",
                "52 Wk Range": "range_52wk",
            }
        )

        # df["name"] = df["ticker"].str.split(" ").apply(lambda x: x[1])
        # df["ticker"] = df["ticker"].str.split(" ").apply(lambda x: x[0])

        df.columns = clean_strings(df.columns)
        df = df.dropna(how="all", axis=1)

        if df["ticker"].iloc[-1][-2] == "=" and "name" not in df.columns:
            df["name"] = df["ticker"].str.split("=").apply(lambda x: x[0])

        if len(df["price"].iloc[0].split(" ")) == 3:
            if df["change"].isnull().any():
                df["change"] = df["change"].fillna(
                    df["price"].apply(lambda x: x.split(" ")[1])
                )
            if df["pct_change"].isnull().any():
                df["pct_change"] = df["pct_change"].fillna(
                    df["price"]
                    .apply(lambda x: x.split(" ")[2])
                    .str.replace("(", "")
                    .str.replace(")", "")
                )
            # df["price"] = df["price"].apply(lambda x: x.split(" ")[0]).str.replace(",", "").astype(float)
            df["price"] = df["price"].apply(lambda x: x.split(" ")[0]).astype(str)

        df = df.replace([np.inf, -np.inf, np.nan], None)

        str_cols = [
            "ticker",
            "price",
            "pct_change",
            "change",
            "market_cap",
            "volume",
            "volume_in_currency_24hr",
            "total_volume_all_currencies_24h",
            "circulating_supply",
            "change_pct_52wk",
            "name",
        ]

        df[str_cols] = df[str_cols].astype(str)

        column_order = [
            "ticker",
            "name",
            "price",
            "change",
            "pct_change",
            "market_cap",
            "volume",
            "volume_in_currency_24hr",
            "total_volume_all_currencies_24h",
            "circulating_supply",
            "change_pct_52wk",
        ]

        return df[column_order]

    @staticmethod
    def download_forex_pairs():
        """
        Description
        -----------
        Download the yfinance forex pair ticker names
        Note: At the time of coding, setting num_currencies higher than 250 results in only 25 crypto tickers returned.
        """

        from requests_html import HTMLSession

        session = HTMLSession()
        resp = session.get(f"https://finance.yahoo.com/currencies/")
        tables = pd.read_html(resp.html.raw_html)
        session.close()

        df = tables[0].copy()

        df = df.rename(
            columns={
                "Symbol": "ticker",
                "% Change": "pct_change",
                "Change %": "pct_change",
                "52 Wk Range": "range_52wk",
            }
        )

        df.columns = clean_strings(df.columns)
        df = df.dropna(how="all", axis=1)

        df.columns = clean_strings(df.columns)
        df = df.dropna(how="all", axis=1)

        if df["ticker"].iloc[-1][-2] == "=" and "name" not in df.columns:
            df["name"] = df["ticker"].str.split(" ").apply(lambda x: x[1])
        elif df["ticker"].iloc[-1][-2] == "=" and "name" not in df.columns:
            df["name"] = df["ticker"].str.split("=").apply(lambda x: x[0])

        if len(df["price"].iloc[0].split(" ")) == 3:
            if df["change"].isnull().any():
                df["change"] = df["change"].fillna(
                    df["price"].apply(lambda x: x.split(" ")[1])
                )
            if df["pct_change"].isnull().any():
                df["pct_change"] = df["pct_change"].fillna(
                    df["price"]
                    .apply(lambda x: x.split(" ")[2])
                    .str.replace("(", "")
                    .str.replace(")", "")
                )
            # df["price"] = df["price"].apply(lambda x: x.split(" ")[0]).str.replace(",", "").astype(float)
            df["price"] = df["price"].apply(lambda x: x.split(" ")[0]).astype(str)

        # df.loc[:, "bloomberg_ticker"] = df["name"].apply(lambda x: f"{x[4:]}-{x[0:3]}")
        if df["ticker"].iloc[-1][-2] == "=" and "name" not in df.columns:
            df["name"] = df["ticker"].str.split("=").apply(lambda x: x[0])

        # df["ticker"] = df["ticker"].str.split(" ").apply(lambda x: x[0])

        df = df.replace([np.inf, -np.inf, np.nan], None)
        df[df.columns] = df[df.columns].astype(str)

        first_cols = ["ticker", "name"]
        df = df[first_cols + [i for i in df.columns if i not in first_cols]].dropna(
            how="all", axis=1
        )

        return df

    @staticmethod
    def download_futures_tickers():
        """
        Description
        -----------
        Download the yfinance future contract ticker names
        Note: At the time of coding, setting num_currencies higher than 250 results in only 25 crypto tickers returned.
        """

        from requests_html import HTMLSession

        session = HTMLSession()
        resp = session.get(f"https://finance.yahoo.com/commodities/")
        tables = pd.read_html(resp.html.raw_html)
        session.close()

        df = tables[0].copy()
        df = df.rename(
            columns={
                "Symbol": "ticker",
                "% Change": "pct_change",
                "Change %": "pct_change",
                "Unnamed: 7": "open_interest",
            }
        )
        df.columns = clean_strings(df.columns)
        str_cols = ["volume", "open_interest", "change"]
        df[str_cols] = df[str_cols].astype(str)
        df = df.dropna(how="all", axis=1)
        df = df.replace([np.inf, -np.inf, np.nan], None)

        return df

    @staticmethod
    def pull_sec_tickers():
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {
            "User-Agent": "MyScraper/1.1.0 (myemail2@example3.com)",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.sec.gov",
            "Connection": "keep-alive",
        }

        session = requests.Session()
        session.headers.update(headers)
        response = session.get(url)

        if response.status_code == 200:
            df_sec_tickers = pd.DataFrame.from_dict(response.json()).T
            df_sec_tickers.columns = ["sec_cik_str", "sec_ticker", "sec_title"]
        else:
            raise f"Error fetching data from SEC: {response.status_code}"

        return df_sec_tickers

    def generate_yahoo_sec_tickermap(self):
        df_sec_tickers = self.pull_sec_tickers()
        df_pts_tickers = self.download_pts_stock_tickers()
        df_mapped = pd.merge(
            df_sec_tickers,
            df_pts_tickers,
            left_on="sec_ticker",
            right_on="yahoo_ticker_pts",
            how="outer",
        )
        df_mapped["ticker"] = df_mapped["sec_ticker"].fillna(
            df_mapped["yahoo_ticker_pts"]
        )  # likely yahoo ticker
        df_mapped = df_mapped.replace([np.inf, -np.inf, np.nan], None)
        return df_mapped

    ###### numerai deprecated their yahoo-bloomberg ticker mapping ######

    # def download_numerai_signals_ticker_map(
    #     self,
    #     numerai_ticker_link="https://numerai-signals-public-data.s3-us-west-2.amazonaws.com/signals_ticker_map_w_bbg.csv",
    #     yahoo_ticker_colname="yahoo",
    # ):
    #     """Download numerai to yahoo ticker mapping"""
    #
    #     ticker_map = pd.read_csv(numerai_ticker_link)
    #
    #     logging.info("Number of eligible tickers in map: %s", str(ticker_map.shape[0]))
    #
    #     ticker_map = ticker_map.replace([np.inf, -np.inf, np.nan], None)
    #     valid_tickers = [
    #         i for i in ticker_map[yahoo_ticker_colname] if i is not None and len(i) > 0
    #     ]
    #     logging.info(f"tickers before cleaning: %s", ticker_map.shape)
    #     ticker_map = ticker_map[ticker_map[yahoo_ticker_colname].isin(valid_tickers)]
    #     logging.info(f"tickers after cleaning: %s", ticker_map.shape)
    #     return ticker_map

    # @classmethod
    # def download_valid_stock_tickers(cls):
    #     """Download the valid tickers from py-ticker-symbols"""
    #
    #     def handle_duplicate_columns(columns):
    #         seen_columns = {}
    #         new_columns = []
    #
    #         for column in columns:
    #             if column not in seen_columns:
    #                 new_columns.append(column)
    #                 seen_columns[column] = 1
    #             else:
    #                 seen_columns[column] += 1
    #                 new_columns.append(f"{column}_{seen_columns[column]}")
    #         return new_columns
    #
    #     df_pts_tickers = cls.download_pts_stock_tickers()
    #
    #     numerai_yahoo_tickers = (
    #         cls()
    #         .download_numerai_signals_ticker_map()
    #         .rename(columns={"yahoo": "yahoo_ticker", "ticker": "numerai_ticker"})
    #     )
    #
    #     df1 = pd.merge(
    #         df_pts_tickers, df_sec_tickers, on="yahoo_ticker", how="left"
    #     ).set_index("yahoo_ticker")
    #
    #     df2 = pd.merge(
    #         numerai_yahoo_tickers, df_pts_tickers, on="yahoo_ticker", how="left"
    #     ).set_index("yahoo_ticker")
    #
    #     df3 = (
    #         pd.merge(
    #             df_pts_tickers,
    #             numerai_yahoo_tickers,
    #             left_on="yahoo_ticker",
    #             right_on="numerai_ticker",
    #             how="left",
    #         )
    #         .rename(
    #             columns={
    #                 "yahoo_ticker_x": "yahoo_ticker",
    #                 "yahoo_ticker_y": "yahoo_ticker_old",
    #             }
    #         )
    #         .set_index("yahoo_ticker")
    #     )
    #
    #     df4 = (
    #         pd.merge(
    #             df_pts_tickers,
    #             numerai_yahoo_tickers,
    #             left_on="yahoo_ticker",
    #             right_on="bloomberg_ticker",
    #             how="left",
    #         )
    #         .rename(
    #             columns={
    #                 "yahoo_ticker_x": "yahoo_ticker",
    #                 "yahoo_ticker_y": "yahoo_ticker_old",
    #             }
    #         )
    #         .set_index("yahoo_ticker")
    #     )
    #
    #     df_tickers_wide = pd.concat([df1, df2, df3, df4], axis=1)
    #     df_tickers_wide.columns = handle_duplicate_columns(df_tickers_wide.columns)
    #     df_tickers_wide.columns = clean_strings(df_tickers_wide.columns)
    #
    #     for col in df_tickers_wide.columns:
    #         suffix = col[-1]
    #         if suffix.isdigit():
    #             root_col = col.strip("_" + suffix)
    #             df_tickers_wide.loc[:, root_col] = df_tickers_wide[root_col].fillna(
    #                 df_tickers_wide[col]
    #             )
    #
    #     df_tickers = (
    #         df_tickers_wide.reset_index()[
    #             [
    #                 "yahoo_ticker",
    #                 "google_ticker",
    #                 "bloomberg_ticker",
    #                 "numerai_ticker",
    #                 "yahoo_ticker_old",
    #             ]
    #         ]
    #         .sort_values(
    #             by=[
    #                 "yahoo_ticker",
    #                 "google_ticker",
    #                 "bloomberg_ticker",
    #                 "numerai_ticker",
    #                 "yahoo_ticker_old",
    #             ]
    #         )
    #         .drop_duplicates()
    #     )
    #
    #     df_tickers.loc[:, "yahoo_valid_pts"] = False
    #     df_tickers.loc[:, "yahoo_valid_numerai"] = False
    #
    #     df_tickers.loc[
    #         df_tickers["yahoo_ticker"].isin(df_pts_tickers["yahoo_ticker"].tolist()),
    #         "yahoo_valid_pts",
    #     ] = True
    #
    #     df_tickers.loc[
    #         df_tickers["yahoo_ticker"].isin(
    #             numerai_yahoo_tickers["numerai_ticker"].tolist()
    #         ),
    #         "yahoo_valid_numerai",
    #     ] = True
    #
    #     df_tickers = df_tickers.replace([np.inf, -np.inf, np.nan], None)
    #     df_tickers = df_tickers.rename(
    #         columns={"yahoo_ticker": "ticker"}
    #     )  # necessary to allow schema partitioning
    #     return df_tickers


def get_valid_yfinance_start_timestamp(interval, start="1950-01-01 00:00:00"):
    """
    Description
    -----------
    Get a valid yfinance date to lookback.
    Valid intervals with maximum lookback period
    1m: 7 days
    2m: 60 days
    5m: 60 days
    15m: 60 days
    30m: 60 days
    60m: 730 days
    90m: 60 days
    1h: 730 days
    1d: 50+ years
    5d: 50+ years
    1wk: 50+ years
    1mo: 50+ years --- Buggy!
    3mo: 50+ years --- Buggy!

    Note: Often times yfinance returns an error even when looking back maximum number of days - 1,
        by default, return a date 2 days closer to the current date than the maximum specified in the yfinance docs

    """

    valid_intervals = [
        "1m",
        "2m",
        "5m",
        "15m",
        "30m",
        "60m",
        "1h",
        "90m",
        "1d",
        "5d",
        "1wk",
        "1mo",
        "3mo",
    ]
    assert interval in valid_intervals, f"must pass a valid interval {valid_intervals}"

    if interval == "1m":
        updated_start = max(
            (datetime.today() - timedelta(days=5)).date(), pd.to_datetime(start).date()
        )
    elif interval in ["2m", "5m", "15m", "30m", "90m"]:
        updated_start = max(
            (datetime.today() - timedelta(days=58)).date(), pd.to_datetime(start).date()
        )
    elif interval in ["60m", "1h"]:
        updated_start = max(
            (datetime.today() - timedelta(days=728)).date(),
            pd.to_datetime(start).date(),
        )
    else:
        updated_start = pd.to_datetime(start)

    updated_start = updated_start.strftime(
        "%Y-%m-%d"
    )  # yfinance doesn't like strftime with hours, minutes, or seconds

    return updated_start


def flatten_list(lst):
    return [v for item in lst for v in (item if isinstance(item, list) else [item])]


def clean_strings(lst):
    cleaned_list = [
        re.sub(r"[^a-zA-Z0-9_]", "_", s) for s in lst
    ]  # remove special characters
    cleaned_list = [
        re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower() for s in cleaned_list
    ]  # camel case -> snake case
    cleaned_list = [
        re.sub(r"_+", "_", s).strip("_").lower() for s in cleaned_list
    ]  # clean leading and trailing underscores
    return cleaned_list


def flatten_multindex_columns(df):
    new_cols = list(
        clean_strings(
            pd.Index(
                [
                    str(e[0]).lower() + "_" + str(e[1]).lower()
                    for e in df.columns.tolist()
                ]
            )
        )
    )
    return new_cols
