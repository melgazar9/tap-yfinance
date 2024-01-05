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
        df = yf.Ticker(ticker).get_actions().reset_index().rename(columns={'Date': 'timestamp'})
        self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        column_order = ['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'dividends', 'stock_splits']
        return df[column_order]

    def get_analyst_price_target(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_balance_sheet(self, ticker):
        df = yf.Ticker(ticker).get_balance_sheet().T.rename_axis('date').reset_index()
        df['ticker'] = ticker
        df = df.replace([np.inf, -np.inf, np.nan], None)
        df.columns = clean_strings(df.columns)
        df.columns = [i.replace('p_p_e', 'ppe') for i in df.columns]
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
        df = yf.Ticker(ticker).get_cash_flow().T.rename_axis('date').reset_index()
        df['ticker'] = ticker
        df.columns = clean_strings(df.columns)
        df.columns = [i.replace('p_p_e', 'ppe') for i in df.columns]
        df = df.replace([np.inf, -np.inf, np.nan], None)
        column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
        return df[column_order]

    def get_cashflow(self, ticker):
        """ Same output as the method get_cash_flow """
        return

    def get_dividends(self, ticker):
        df = yf.Ticker(ticker).get_dividends().rename_axis('timestamp').reset_index()
        df['ticker'] = ticker
        self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        df.columns = clean_strings(df.columns)
        return df[['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'dividends']]

    def get_earnings(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_earnings_dates(self, ticker):
        df = yf.Ticker(ticker).get_earnings_dates().rename_axis('timestamp').reset_index()
        df['ticker'] = ticker
        self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        df.columns = [i.replace('e_p_s', 'eps') for i in clean_strings(df.columns)]
        df.rename(columns={'surprise': 'pct_surprise'}, inplace=True)
        column_order = ['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'eps_estimate', 'reported_eps', 'pct_surprise']
        return df[column_order]

    def get_earnings_forecast(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_earnings_trend(self, ticker):
        """ yfinance.exceptions.YFNotImplementedError """
        return

    def get_fast_info(self, ticker):
        df = pd.DataFrame.from_dict(d, orient='index').T
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

    def get_financials(self, ticker):
        df = yf.Ticker(ticker).get_financials().T.rename_axis('date').reset_index()
        df['ticker'] = ticker
        self.extract_ticker_tz_aware_timestamp(df, 'date', ticker)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        df.columns = [i.replace('e_b_i_t_d_a', 'ebitda').replace('e_p_s', 'eps').replace('e_b_i_t', 'ebit') \
                          .replace('diluted_n_i_availto_com_stockholders', 'diluted_ni_availto_com_stockholders')
                      for i in clean_strings(df.columns)]
        column_order = ['date', 'date_tz_aware', 'timezone', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
        return df[column_order]

    def get_history_metadata(self, ticker):
        data = yf.Ticker(ticker).get_history_metadata()
        data['tradingPeriods'] = data['tradingPeriods'].to_dict()
        df = pd.Series({key: data[key] for key in data.keys()}).to_frame().T
        df.columns = clean_strings(df.columns)
        df = df.replace([np.inf, -np.inf, np.nan], None)

        column_order = \
            ['symbol', 'timezone', 'currency'] + \
            [i for i in df.columns if i not in ['symbol', 'timezone', 'currency', 'trading_periods', 'data_granularity', 'valid_ranges']] + \
            ['trading_periods', 'data_granularity', 'valid_ranges']

        return df[column_order]

    def get_income_stmt(self, ticker):
        df = yf.Ticker(ticker).get_income_stmt().T.rename_axis('date').reset_index()
        df['ticker'] = ticker
        self.extract_ticker_tz_aware_timestamp(df, 'date', ticker)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        df.columns = [i.replace('e_b_i_t_d_a', 'ebitda').replace('e_p_s', 'eps').replace('e_b_i_t', 'ebit')\
                       .replace('diluted_n_i_availto_com_stockholders', 'diluted_ni_availto_com_stockholders')
                      for i in clean_strings(df.columns)]
        column_order = ['date', 'date_tz_aware', 'timezone', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
        return df[column_order]

    def get_incomestmt(self, ticker):
        """ Same output as the method get_income_stmt """
        return

    def get_info(self, ticker):
        """ Returns NoneType. """
        return

    def get_institutional_holders(self, ticker):
        df = yf.Ticker(ticker).get_institutional_holders()
        df['ticker'] = ticker
        df = df.replace([np.inf, -np.inf, np.nan], None)
        df.columns = clean_strings(df.columns)
        column_order = ['date_reported', 'ticker'] + sorted([i for i in df.columns if i not in ['date_reported', 'ticker']])
        return df[column_order]

    def get_isin(self, ticker):
        """ Returns NoneType. """
        return

    def get_major_holders(self, ticker):
        df = yf.Ticker(ticker).get_major_holders()
        df.columns = ['pct', 'category']
        df['ticker'] = ticker
        df['timestamp_extracted'] = datetime.utcnow()
        df = df.replace([np.inf, -np.inf, np.nan], None)
        return df[['timestamp_extracted', 'ticker', 'category', 'pct']]

    def get_mutualfund_holders(self, ticker):
        df = yf.Ticker(ticker).get_mutualfund_holders()
        df.rename(columns={'% Out': 'pct_out'}, inplace=True)
        df.columns = clean_strings(df.columns)
        df['ticker'] = ticker
        df = df.replace([np.inf, -np.inf, np.nan], None)
        column_order = ['date_reported', 'ticker'] + sorted([i for i in df.columns if i not in ['date_reported', 'ticker']])
        return df[column_order]

    def get_news(self, ticker):
        df = pd.DataFrame(yf.Ticker(ticker).get_news())
        df['ticker'] = ticker
        df['timestamp_extracted'] = datetime.utcnow()
        df = df.replace([np.inf, -np.inf, np.nan], None)
        column_order = ['timestamp_extracted', 'ticker'] + sorted([i for i in df.columns if i not in ['timestamp_extracted', 'ticker']])
        return df[column_order]

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
        df = yf.Ticker(ticker).get_shares_full().reset_index()
        df.columns = ['timestamp', 'amount']
        df['ticker'] = ticker
        self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        return df[['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'amount']]

    def get_splits(self, ticker):
        df = yf.Ticker(ticker).get_splits().rename_axis('timestamp').reset_index()
        df['ticker'] = ticker
        self.extract_ticker_tz_aware_timestamp(df, 'timestamp', ticker)
        df.columns = clean_strings(df.columns)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        return df[['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'stock_splits']]

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

        for exp_date in option_expiration_dates:
            option_chain_data = yf.Ticker(ticker).option_chain(date=exp_date)
            for ocd in option_chain_data[0: -1]:
                df = pd.concat([df, ocd])
                df['ticker'] = ticker
                df['timestamp_extracted'] = datetime.utcnow()
                df.drop_duplicates(inplace=True)
                df['metadata'] = option_chain_data[-1]

        df.columns = clean_strings(df.columns)
        self.extract_ticker_tz_aware_timestamp(df, 'last_trade_date', ticker)
        df = df.replace([np.inf, -np.inf, np.nan], None)

        column_order = ['last_trade_date', 'last_trade_date_tz_aware', 'timezone', 'timestamp_extracted', 'ticker'] + \
            [i for i in df.columns if i not in ['last_trade_date', 'last_trade_date_tz_aware', 'timezone',
                                                'timestamp_extracted', 'ticker']]
        
        return df[column_order]

    def options(self, ticker):
        option_expiration_dates = pd.DataFrame(yf.Ticker(ticker).options, columns=['expiration_date'])
        option_expiration_dates['ticker'] = ticker
        option_expiration_dates['timestamp_extracted'] = datetime.utcnow()
        option_expiration_dates = option_expiration_dates.replace([np.inf, -np.inf, np.nan], None)
        return option_expiration_dates[['timestamp_extracted', 'ticker', 'expiration_date']]


    def quarterly_balance_sheet(self, ticker):
        df = yf.Ticker(ticker).quarterly_balance_sheet.T.rename_axis('date').reset_index()
        df['ticker'] = ticker
        df.columns = [i.replace('p_p_e', 'ppe') for i in clean_strings(df.columns)]
        df = df.replace([np.inf, -np.inf, np.nan], None)
        column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
        return df[column_order]

    def quarterly_balancesheet(self, ticker):
        """ Same output as the method quarterly_balance_sheet """
        return

    def quarterly_cash_flow(self, ticker):
        df = yf.Ticker(ticker).quarterly_cash_flow.T.rename_axis('date').reset_index()
        df['ticker'] = ticker
        df = df.replace([np.inf, -np.inf, np.nan], None)
        df.columns = [i.replace('p_p_e', 'ppe') for i in clean_strings(df.columns)]
        column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
        return df[column_order]

    def quarterly_cashflow(self, ticker):
        """ Same output as the method quarterly_cash_flow """
        return

    def quarterly_financials(self, ticker):
        df = yf.Ticker(ticker).quarterly_financials.T.rename_axis('date').reset_index()
        df['ticker'] = ticker
        df = df.replace([np.inf, -np.inf, np.nan], None)
        df.columns = [i.replace('e_b_i_t_d_a', 'ebitda').replace('e_p_s', 'eps').replace('e_b_i_t', 'ebit') \
                          .replace('diluted_n_i_availto_com_stockholders', 'diluted_ni_availto_com_stockholders')
                      for i in clean_strings(df.columns)]
        column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
        return df[column_order]

    def quarterly_income_stmt(self, ticker):
        df = yf.Ticker(ticker).quarterly_income_stmt.T.rename_axis('date').reset_index()
        df['ticker'] = ticker
        df = df.replace([np.inf, -np.inf, np.nan], None)
        df.columns = [i.replace('e_b_i_t_d_a', 'ebitda').replace('e_p_s', 'eps').replace('e_b_i_t', 'ebit') \
                       .replace('diluted_n_i_availto_com_stockholders', 'diluted_ni_availto_com_stockholders')
                      for i in clean_strings(df.columns)]
        column_order = ['date', 'ticker'] + sorted([i for i in df.columns if i not in ['date', 'ticker']])
        return df[column_order]

    def quarterly_incomestmt(self, ticker):
        """ Same output as the method quarterly_income_stmt """
        return

    def session(self, ticker):
        """ Returns NoneType. """
        return