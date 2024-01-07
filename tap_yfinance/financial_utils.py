import logging
from datetime import datetime
import pandas as pd
import numpy as np
import yfinance as yf
from tap_yfinance.price_utils import YFinanceLogger, clean_strings


### failed ###
# 'analyst_price_target',
# 'calendar',
# 'earnings',
# 'earnings_forecasts',
# 'earnings_trend',
# 'quarterly_earnings',
# 'recommendations',
# 'recommendations_summary',
# 'revenue_forecasts',
# 'shares',
# 'sustainability',
# 'trend_details'


class FinancialTap(YFinanceLogger):

    ### TODO: date filters? ###

    def __init__(self, schema_category, yf_params=None, ticker_colname='ticker'):
        self.schema_category = schema_category
        self.yf_params = {} if yf_params is None else yf_params
        self.ticker_colname = ticker_colname

        super().__init__()

    @staticmethod
    def extract_ticker_tz_aware_timestamp(df, timestamp_column, ticker):
        """ transformations are applied inplace to reduce memory usage """
        assert 'timezone' not in df.columns, 'timezone cannot be a pre-existing column in the extracted df.'
        df['ticker'] = ticker
        df.columns = clean_strings(df.columns)
        df['timezone'] = str(df[timestamp_column].dt.tz)
        df.loc[:, f'{timestamp_column}_tz_aware'] = df[timestamp_column].copy().dt.strftime('%Y-%m-%d %H:%M:%S%z')
        df[timestamp_column] = pd.to_datetime(df[timestamp_column], utc=True)
        return df

    def get_actions(self, ticker):
        df = yf.Ticker(ticker).get_actions()
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.reset_index().rename(columns={'Date': 'timestamp'})
            self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
            df = df.replace([np.inf, -np.inf, np.nan], None)
        else:
            return pd.DataFrame(columns=['timestamp'])
        column_order = ['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'dividends', 'stock_splits']
        return df[column_order]

    def get_analyst_price_target(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_balance_sheet(self, ticker):
        df = yf.Ticker(ticker).get_balance_sheet()
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis('date').reset_index()
            df['ticker'] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = clean_strings(df.columns)
            df.columns = [i.replace('p_p_e', 'ppe') for i in df.columns]
        else:
            return pd.DataFrame(columns=['date'])
        column_order = ['date', 'ticker'] + [i for i in df.columns if i not in ['date', 'ticker']]
        return df[column_order]

    def get_balancesheet(self, ticker):
        """ Same output as the method get_balance_sheet """
        return

    def basic_info(self, ticker):
        """ Useless information """
        return

    def get_calendar(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_capital_gains(self, ticker):
        """ Returns empty array. """
        return

    def get_cash_flow(self, ticker):
        df = yf.Ticker(ticker).get_cash_flow()
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis('date').reset_index()
            df['ticker'] = ticker
            df.columns = clean_strings(df.columns)
            df.columns = [
                i.replace('p_p_e', 'ppe').replace('c_f_o', 'cfo').replace('c_f_f', 'cff').replace('c_f_i', 'cfi')
                for i in df.columns]
            df = df.replace([np.inf, -np.inf, np.nan], None)
        else:
            return pd.DataFrame(columns=['date'])
        column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
        return df[column_order]

    def get_cashflow(self, ticker):
        """ Same output as the method get_cash_flow """
        return

    def get_dividends(self, ticker):
        df = yf.Ticker(ticker)
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.get_dividends().rename_axis('timestamp').reset_index()
            df['ticker'] = ticker
            self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
            df = df.replace([np.inf, -np.inf, np.nan], None)
        else:
            return pd.DataFrame(columns=['timestamp'])
        df.columns = clean_strings(df.columns)
        return df[['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'dividends']]

    def get_earnings(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_earnings_dates(self, ticker):
        df = yf.Ticker(ticker).get_earnings_dates()
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.rename_axis('timestamp').reset_index()
            df['ticker'] = ticker
            self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace('e_p_s', 'eps') for i in clean_strings(df.columns)]
            df.rename(columns={'surprise': 'pct_surprise'}, inplace=True)
        else:
            return pd.DataFrame(columns=['timestamp'])
        column_order = ['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'eps_estimate', 'reported_eps',
                        'pct_surprise']
        return df[column_order]

    def get_earnings_forecast(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_earnings_trend(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_fast_info(self, ticker):
        try:
            df = pd.DataFrame.from_dict(dict(yf.Ticker(ticker).get_fast_info()), orient='index').T
        except:
            logging.warning(f'Error loading get_fast_info as dictionary for ticker {ticker}. Skipping this ticker...')
            return pd.DataFrame(columns=['timestamp_extracted'])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df['ticker'] = ticker
            df['timestamp_extracted'] = datetime.utcnow()
            df.rename(columns={'timezone': 'extracted_timezone'}, inplace=True)

            df['timestamp_tz_aware'] = df.apply(
                lambda row: row['timestamp_extracted'].tz_localize(row['extracted_timezone']),
                axis=1
            )

            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = clean_strings(df.columns)
            return df

        else:
            return pd.DataFrame(columns=['timestamp_extracted'])

    def get_financials(self, ticker):
        df = yf.Ticker(ticker).get_financials().T
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.rename_axis('timestamp').reset_index()
            df['ticker'] = ticker
            self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace('e_b_i_t_d_a', 'ebitda').replace('e_p_s', 'eps').replace('e_b_i_t', 'ebit')
                           .replace('p_p_e', 'ppe')
                           .replace('diluted_n_i_availto_com_stockholders', 'diluted_ni_availto_com_stockholders')
                          for i in clean_strings(df.columns)]
            first_cols = ['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker']
        else:
            return pd.DataFrame(columns=['timestamp'])
        column_order = first_cols + sorted([i for i in df.columns if i not in first_cols])
        return df[column_order]

    def get_history_metadata(self, ticker):
        data = yf.Ticker(ticker).get_history_metadata()

        if len(data):
            if 'tradingPeriods' in data.keys():
                data['tradingPeriods'] = data['tradingPeriods'].to_dict()
            df = pd.Series({key: data[key] for key in data.keys()}).to_frame().T
            df.columns = clean_strings(df.columns)
            df = df.rename(columns={'symbol': 'ticker'})
            df = df.replace([np.inf, -np.inf, np.nan], None)

            if 'current_trading_period' in df.columns:
                df_ctp = pd.json_normalize(df['current_trading_period'])
                df_ctp.columns = clean_strings(df_ctp.columns)
                df_ctp = df_ctp.add_prefix('current_trading_period_')
                df = pd.concat([df, df_ctp], axis=1)
                df = df.drop(['current_trading_period'], axis=1)

            if 'trading_periods' in df.columns:
                df_tp = pd.DataFrame().from_dict(df['trading_periods'].iloc[0])
                df_tp = df_tp.add_prefix('trading_period_').reset_index(drop=True)
                df = pd.concat([df, df_tp], axis=1).ffill()
                df = df.drop('trading_periods', axis=1)

            df['timestamp_extracted'] = datetime.utcnow()

            first_cols = ['ticker', 'timezone', 'currency']
            column_order = first_cols + [i for i in df.columns if i not in first_cols]
            return df[column_order]

        else:
            return pd.DataFrame(columns=['timestamp_extracted'])

    def get_income_stmt(self, ticker):
        df = yf.Ticker(ticker).get_income_stmt()
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis('date').reset_index()
            df['ticker'] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace('e_b_i_t_d_a', 'ebitda').replace('e_p_s', 'eps').replace('e_b_i_t', 'ebit') \
                              .replace('diluted_n_i_availto_com_stockholders', 'diluted_ni_availto_com_stockholders') \
                              .replace('p_p_e', 'ppe')
                          for i in clean_strings(df.columns)]
            column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
        else:
            return pd.DataFrame(columns=['date'])
        return df[column_order]

    def get_incomestmt(self, ticker):
        """ Same output as the method get_income_stmt """
        return

    def get_info(self, ticker):
        """ Returns NoneType. """
        return

    def get_institutional_holders(self, ticker):
        try:
            df = yf.Ticker(ticker).get_institutional_holders()
        except:
            logging.warning(f"Could not extract institutional_holders for ticker {ticker}. Skipping...")
            return pd.DataFrame(columns=['date_reported'])
        if isinstance(df, pd.DataFrame) and df.shape[0] and df.shape[1] == 5:
            df['ticker'] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df = df.rename(columns={'% Out': 'pct_out'})
            df.columns = clean_strings(df.columns)
            column_order = ['date_reported', 'ticker'] + sorted(
                [i for i in df.columns if i not in ['date_reported', 'ticker']])
            return df[column_order]
        else:
            logging.warning(f"Inconsistent fields for institutional_holders for ticker {ticker}. Skipping...")
            return pd.DataFrame(columns=['date_reported'])

    def get_isin(self, ticker):
        """ Returns NoneType. """
        return

    def get_major_holders(self, ticker):
        try:
            df = yf.Ticker(ticker).get_major_holders()
        except:
            logging.warning(f"Could not extract get_major_holders for ticker {ticker}. Skipping...")
            return pd.DataFrame(columns=['timestamp_extracted'])
        if isinstance(df, pd.DataFrame) and df.shape[0] and df.shape[1] == 2:
            df.columns = ['value', 'category']
            df['ticker'] = ticker
            df['timestamp_extracted'] = datetime.utcnow()
            df = df.replace([np.inf, -np.inf, np.nan], None)
            return df[['timestamp_extracted', 'ticker', 'category', 'value']]
        else:
            logging.warning(f"Inconsistent fields for get_major_holders for ticker {ticker}. Skipping...")
            return pd.DataFrame(columns=['timestamp_extracted'])

    def get_mutualfund_holders(self, ticker):
        try:
            df = yf.Ticker(ticker).get_mutualfund_holders()
        except:
            logging.warning(f"Could not extract get_mutualfund_holders for ticker {ticker}. Skipping...")
            return pd.DataFrame(columns=['date_reported'])

        if isinstance(df, pd.DataFrame) and df.shape[0] and df.shape[1] == 5:
            df.rename(columns={'% Out': 'pct_out'}, inplace=True)
            df.columns = clean_strings(df.columns)
            df['ticker'] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            column_order = ['date_reported', 'ticker'] + sorted(
                [i for i in df.columns if i not in ['date_reported', 'ticker']])
            return df[column_order]
        else:
            logging.warning(f"Inconsistent fields for get_mutualfund_holders for ticker {ticker}. Skipping...")
            return pd.DataFrame(columns=['date_reported'])

    def get_news(self, ticker):
        try:
            df = pd.DataFrame(yf.Ticker(ticker).get_news())
        except:
            logging.warning(f"Could not extract get_news for ticker {ticker}. Skipping...")
            return pd.DataFrame(columns=['date_reported'])

        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df['ticker'] = ticker
            df['timestamp_extracted'] = datetime.utcnow()
            df.columns = clean_strings(df.columns)

            if 'thumbnail' in df.keys():
                na_fill = {
                    'resolutions': [
                        {'url': None, 'width': None, 'height': None, 'tag': None},
                        {'url': None, 'width': None, 'height': None, 'tag': None}
                    ]
                }
                df['thumbnail'] = df['thumbnail'].apply(lambda x: na_fill if pd.isna(x) else x)
                df_thumbnail_0 = pd.json_normalize(pd.json_normalize(pd.json_normalize(df['thumbnail'])['resolutions'])[0])
                df_thumbnail_0 = df_thumbnail_0.add_suffix('_0')
                df_thumbnail_1 = pd.json_normalize(pd.json_normalize(pd.json_normalize(df['thumbnail'])['resolutions'])[1])
                df_thumbnail_1 = df_thumbnail_1.add_suffix('_1')
                df_thumbnail = pd.concat([df_thumbnail_0, df_thumbnail_1], axis=1)

                df = pd.concat([df, df_thumbnail], axis=1)
                df = df.drop('thumbnail', axis=1)

            df = df.replace([np.inf, -np.inf, np.nan], None)

            column_order = ['timestamp_extracted', 'ticker'] + \
                           sorted([i for i in df.columns if i not in ['timestamp_extracted', 'ticker']])
            return df[column_order]
        else:
            logging.warning(f"Inconsistent fields for get_news for ticker {ticker}. Skipping...")
            return pd.DataFrame(columns=['timestamp_extracted'])

    def get_recommendations(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_recommendations_summary(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_rev_forecast(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_shares(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_shares_full(self, ticker):
        df = yf.Ticker(ticker).get_shares_full()
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.reset_index()
            df.columns = ['timestamp', 'amount']
            df['ticker'] = ticker
            self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            return df[['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'amount']]
        else:
            return pd.DataFrame(columns=['timestamp'])

    def get_splits(self, ticker):
        df = yf.Ticker(ticker).get_splits()
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.rename_axis('timestamp').reset_index()
            df['ticker'] = ticker
            self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
            df.columns = clean_strings(df.columns)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            return df[['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'stock_splits']]
        else:
            return pd.DataFrame(columns=['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'stock_splits'])

    def get_sustainability(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_trend_details(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def option_chain(self, ticker):
        # TODO: clean option extraction
        df = pd.DataFrame()
        option_expiration_dates = yf.Ticker(ticker).options
        if len(option_expiration_dates):
            for exp_date in option_expiration_dates:
                option_chain_data = yf.Ticker(ticker).option_chain(date=exp_date)
                if len(option_chain_data):
                    for ocd in option_chain_data[0: -1]:
                        ocd.columns = clean_strings(ocd.columns)
                        ocd['last_trade_date_tz_aware'] = ocd['last_trade_date'].copy()
                        ocd['last_trade_date'] = pd.to_datetime(ocd['last_trade_date'], utc=True)
                        df = pd.concat([df, ocd])
                        df['ticker'] = ticker
                        df['timestamp_extracted'] = datetime.utcnow()
                        df.drop_duplicates(inplace=True)
                        df['metadata'] = option_chain_data[-1]
                try:
                    df.columns = clean_strings(df.columns)
                    extract_ticker_tz_aware_timestamp(df, 'last_trade_date', ticker)
                    df = df.replace([np.inf, -np.inf, np.nan], None)

                    column_order = \
                        ['last_trade_date', 'last_trade_date_tz_aware', 'timezone', 'timestamp_extracted', 'ticker'] + \
                        [i for i in df.columns if i not in ['last_trade_date', 'last_trade_date_tz_aware', 'timezone',
                                                            'timestamp_extracted', 'ticker']]

                    return df[column_order]

                except:
                    return pd.DataFrame(columns=['last_trade_date'])
        else:
            return pd.DataFrame(columns=['last_trade_date'])

    def options(self, ticker):
        option_expiration_dates = pd.DataFrame(yf.Ticker(ticker).options, columns=['expiration_date'])
        if len(option_expiration_dates):
            option_expiration_dates['ticker'] = ticker
            option_expiration_dates['timestamp_extracted'] = datetime.utcnow()
            option_expiration_dates = option_expiration_dates.replace([np.inf, -np.inf, np.nan], None)
            return option_expiration_dates[['timestamp_extracted', 'ticker', 'expiration_date']]
        else:
            return pd.DataFrame(columns=['timestamp_extracted', 'ticker', 'expiration_date'])

    def quarterly_balance_sheet(self, ticker):
        df = yf.Ticker(ticker).quarterly_balance_sheet
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis('date').reset_index()
            df['ticker'] = ticker
            df.columns = [i.replace('p_p_e', 'ppe') for i in clean_strings(df.columns)]
            df = df.replace([np.inf, -np.inf, np.nan], None)
            column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
            return df[column_order]
        else:
            return pd.DataFrame(columns=['timestamp'])

    def quarterly_balancesheet(self, ticker):
        """ Same output as the method quarterly_balance_sheet """
        return

    def quarterly_cash_flow(self, ticker):
        df = yf.Ticker(ticker).quarterly_cash_flow
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis('date').reset_index()
            df['ticker'] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace('p_p_e', 'ppe') for i in clean_strings(df.columns)]
            column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
            return df[column_order]
        else:
            return pd.DataFrame(columns=['timestamp'])

    def quarterly_cashflow(self, ticker):
        """ Same output as the method quarterly_cash_flow """
        return

    def quarterly_financials(self, ticker):
        df = yf.Ticker(ticker).quarterly_financials
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis('date').reset_index()
            df['ticker'] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace('e_b_i_t_d_a', 'ebitda').replace('e_p_s', 'eps').replace('e_b_i_t', 'ebit') \
                              .replace('diluted_n_i_availto_com_stockholders', 'diluted_ni_availto_com_stockholders')
                          for i in clean_strings(df.columns)]
            column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
            return df[column_order]
        else:
            return pd.DataFrame(columns=['timestamp'])

    def quarterly_income_stmt(self, ticker):
        df = yf.Ticker(ticker).quarterly_income_stmt
        if isinstance(df, pd.DataFrame) and df.shape[0]:
            df = df.T.rename_axis('date').reset_index()
            df['ticker'] = ticker
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df.columns = [i.replace('e_b_i_t_d_a', 'ebitda').replace('e_p_s', 'eps').replace('e_b_i_t', 'ebit') \
                              .replace('diluted_n_i_availto_com_stockholders', 'diluted_ni_availto_com_stockholders')
                          for i in clean_strings(df.columns)]
            column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
            return df[column_order]
        else:
            return pd.DataFrame(columns=['timestamp'])

    def quarterly_incomestmt(self, ticker):
        """ Same output as the method quarterly_income_stmt """
        return

    def session(self, ticker):
        """ Returns NoneType. """
        return
