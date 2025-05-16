from __future__ import annotations
from tap_yfinance.client import *
from tap_yfinance.schema import *

###### ticker streams ######


class StockTickersStream(TickerStream):
    name = "stock_tickers"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("sec_ticker", th.StringType),
        th.Property("yahoo_ticker_pts", th.StringType),
        th.Property("google_ticker_pts", th.StringType),
        th.Property("sec_cik_str", th.NumberType),
        th.Property("sec_title", th.StringType),
    ).to_dict()


class FuturesTickersStream(TickerStream):
    name = "futures_tickers"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("last_price", th.NumberType),
        th.Property("price", th.StringType),
        th.Property("market_time", th.StringType),
        th.Property("change", th.StringType),
        th.Property("pct_change", th.StringType),
        th.Property("volume", th.StringType),
        th.Property("open_interest", th.StringType),
    ).to_dict()


class ForexTickersStream(TickerStream):
    name = "forex_tickers"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("bloomberg_ticker", th.StringType),
        th.Property("last_price", th.NumberType),
        th.Property("price", th.StringType),
        th.Property("change", th.StringType),
        th.Property("pct_change", th.StringType),
    ).to_dict()


class CryptoTickersStream(TickerStream):
    name = "crypto_tickers"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("price", th.StringType),
        th.Property("change", th.StringType),
        th.Property("pct_change", th.StringType),
        th.Property("market_cap", th.StringType),
        th.Property("volume", th.StringType),
        th.Property("volume_in_currency_24hr", th.StringType),
        th.Property("total_volume_all_currencies_24h", th.StringType),
        th.Property("circulating_supply", th.StringType),
        th.Property("change_pct_52wk", th.StringType),
        th.Property("open_interest", th.StringType),
    ).to_dict()


###### price streams ######


class CryptoTickersTop250Stream(CryptoTickersStream):
    name = "crypto_tickers_top_250"


class StockPrices1mStream(StockPricesStream):
    name = "stock_prices_1m"


class StockPrices2mStream(StockPricesStream):
    name = "stock_prices_2m"


class StockPrices5mStream(StockPricesStream):
    name = "stock_prices_5m"


class StockPrices1hStream(StockPricesStream):
    name = "stock_prices_1h"


class StockPrices1dStream(StockPricesStream):
    name = "stock_prices_1d"


class FuturesPrices1mStream(DerivativePricesStream):
    name = "futures_prices_1m"


class FuturesPrices2mStream(DerivativePricesStream):
    name = "futures_prices_2m"


class FuturesPrices5mStream(DerivativePricesStream):
    name = "futures_prices_5m"


class FuturesPrices1hStream(DerivativePricesStream):
    name = "futures_prices_1h"


class FuturesPrices1dStream(DerivativePricesStream):
    name = "futures_prices_1d"


class ForexPrices1mStream(DerivativePricesStream):
    name = "forex_prices_1m"


class ForexPrices2mStream(DerivativePricesStream):
    name = "forex_prices_2m"


class ForexPrices5mStream(DerivativePricesStream):
    name = "forex_prices_5m"


class ForexPrices1hStream(DerivativePricesStream):
    name = "forex_prices_1h"


class ForexPrices1dStream(DerivativePricesStream):
    name = "forex_prices_1d"


class CryptoPrices1mStream(DerivativePricesStream):
    name = "crypto_prices_1m"


class CryptoPrices2mStream(DerivativePricesStream):
    name = "crypto_prices_2m"


class CryptoPrices5mStream(DerivativePricesStream):
    name = "crypto_prices_5m"


class CryptoPrices1hStream(DerivativePricesStream):
    name = "crypto_prices_1h"


class CryptoPrices1dStream(DerivativePricesStream):
    name = "crypto_prices_1d"


###### prices wide streams ######


class StockPricesWide1mStream(PricesStreamWide):
    name = "stock_prices_wide_1m"


class StockPricesWide2mStream(PricesStreamWide):
    name = "stock_prices_wide_2m"


class StockPricesWide5mStream(PricesStreamWide):
    name = "stock_prices_wide_5m"


class StockPricesWide1hStream(PricesStreamWide):
    name = "stock_prices_wide_1h"


class StockPricesWide1dStream(PricesStreamWide):
    name = "stock_prices_wide_1d"


class FuturesPricesWide1mStream(PricesStreamWide):
    name = "futures_prices_wide_1m"


class FuturesPricesWide2mStream(PricesStreamWide):
    name = "futures_prices_wide_2m"


class FuturesPricesWide5mStream(PricesStreamWide):
    name = "futures_prices_wide_5m"


class FuturesPricesWide1hStream(PricesStreamWide):
    name = "futures_prices_wide_1h"


class FuturesPricesWide1dStream(PricesStreamWide):
    name = "futures_prices_wide_1d"


class ForexPricesWide1mStream(PricesStreamWide):
    name = "forex_prices_wide_1m"


class ForexPricesWide2mStream(PricesStreamWide):
    name = "forex_prices_wide_2m"


class ForexPricesWide5mStream(PricesStreamWide):
    name = "forex_prices_wide_5m"


class ForexPricesWide1hStream(PricesStreamWide):
    name = "forex_prices_wide_1h"


class ForexPricesWide1dStream(PricesStreamWide):
    name = "forex_prices_wide_1d"


class CryptoPricesWide1mStream(PricesStreamWide):
    name = "crypto_prices_wide_1m"


class CryptoPricesWide2mStream(PricesStreamWide):
    name = "crypto_prices_wide_2m"


class CryptoPricesWide5mStream(PricesStreamWide):
    name = "crypto_prices_wide_5m"


class CryptoPricesWide1hStream(PricesStreamWide):
    name = "crypto_prices_wide_1h"


class CryptoPricesWide1dStream(PricesStreamWide):
    name = "crypto_prices_wide_1d"


###### financial streams ######


class ActionsStream(FinancialStream):
    name = "actions"
    method_name = "get_actions"

    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("timestamp_tz_aware", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("dividends", th.NumberType),
        th.Property("stock_splits", th.NumberType),
    ).to_dict()


class AnalystPriceTargetsStream(FinancialStream):
    name = "analyst_price_targets"
    method_name = "get_analyst_price_targets"

    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType, required=True),
        th.Property("current", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("mean", th.NumberType),
        th.Property("median", th.NumberType),
    ).to_dict()


class BalanceSheetStream(FinancialStream):
    name = "balance_sheet"
    method_name = "get_balance_sheet"
    schema = BALANCE_SHEET_SCHEMA


class CalendarStream(FinancialStream):
    name = "calendar"
    method_name = "get_calendar"
    schema = th.PropertiesList(
        th.Property("dividend_date", th.DateTimeType),
        th.Property("ex_dividend_date", th.DateTimeType),
        th.Property("earnings_date", th.DateTimeType),
        th.Property("ticker", th.StringType),
        th.Property("earnings_high", th.NumberType),
        th.Property("earnings_low", th.NumberType),
        th.Property("earnings_average", th.NumberType),
        th.Property("revenue_high", th.NumberType),
        th.Property("revenue_low", th.NumberType),
        th.Property("revenue_average", th.NumberType),
    ).to_dict()


class CashFlowStream(FinancialStream):
    name = "cash_flow"
    method_name = "get_cash_flow"
    schema = CASH_FLOW_SCHEMA


class DividendsStream(FinancialStream):
    name = "dividends"
    method_name = "get_dividends"
    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("timestamp_tz_aware", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("dividends", th.NumberType),
    ).to_dict()


class EarningsDatesStream(FinancialStream):
    name = "earnings_dates"
    method_name = "get_earnings_dates"
    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("timestamp_tz_aware", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("eps_estimate", th.NumberType),
        th.Property("reported_eps", th.NumberType),
        th.Property("pct_surprise", th.NumberType),
    ).to_dict()


class EarningsEstimateStream(FinancialStream):
    name = "earnings_estimate"
    method_name = "get_earnings_estimate"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType),
        th.Property("period", th.StringType),
        th.Property("avg", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("year_ago_eps", th.NumberType),
        th.Property("number_of_analysts", th.NumberType),
        th.Property("growth", th.NumberType),
    ).to_dict()


class EarningsHistoryStream(FinancialStream):
    name = "earnings_history"
    method_name = "get_earnings_history"
    schema = th.PropertiesList(
        th.Property("quarter", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType),
        th.Property("eps_actual", th.NumberType),
        th.Property("eps_estimate", th.NumberType),
        th.Property("eps_difference", th.NumberType),
        th.Property("surprise_percent", th.NumberType),
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
    ).to_dict()


class EarningsEpsRevisionsStream(FinancialStream):
    name = "eps_revisions"
    method_name = "get_eps_revisions"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType, required=True),
        th.Property("period", th.StringType),
        th.Property("up_last_7_days", th.NumberType),
        th.Property("down_last_7_days", th.NumberType),
        th.Property("up_last_30_days", th.NumberType),
        th.Property("down_last_30_days", th.NumberType),
    ).to_dict()


class EpsTrendStream(FinancialStream):
    name = "eps_trend"
    method_name = "get_eps_trend"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType, required=True),
        th.Property("period", th.StringType),
        th.Property("current", th.NumberType),
        th.Property("days_ago_7", th.NumberType),
        th.Property("days_ago_30", th.NumberType),
        th.Property("days_ago_60", th.NumberType),
        th.Property("days_ago_90", th.NumberType),
    ).to_dict()


class GrowthEstimatesStream(FinancialStream):
    name = "growth_estimates"
    method_name = "get_growth_estimates"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType, required=True),
        th.Property("period", th.StringType),
        th.Property("stock_trend", th.NumberType),
        th.Property("index_trend", th.NumberType),
    ).to_dict()


class FastInfoStream(FinancialStream):
    name = "fast_info"
    method_name = "get_fast_info"
    schema = th.PropertiesList(
        th.Property("currency", th.StringType),
        th.Property("day_high", th.NumberType),
        th.Property("day_low", th.NumberType),
        th.Property("exchange", th.StringType),
        th.Property("fifty_day_average", th.NumberType),
        th.Property("last_price", th.NumberType),
        th.Property("last_volume", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("open", th.NumberType),
        th.Property("previous_close", th.NumberType),
        th.Property("quote_type", th.StringType),
        th.Property("regular_market_previous_close", th.NumberType),
        th.Property("shares", th.NumberType),
        th.Property("ten_day_average_volume", th.NumberType),
        th.Property("three_month_average_volume", th.NumberType),
        th.Property("extracted_timezone", th.StringType),
        th.Property("two_hundred_day_average", th.NumberType),
        th.Property("year_change", th.NumberType),
        th.Property("year_high", th.NumberType),
        th.Property("year_low", th.NumberType),
        th.Property("ticker", th.StringType),
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("timestamp_tz_aware", th.StringType),
    ).to_dict()


class FinancialsStream(FinancialStream):
    name = "financials"
    method_name = "get_financials"
    schema = FINANCIALS_SCHEMA


class HistoryMetadataStream(FinancialStream):
    name = "history_metadata"
    method_name = "get_history_metadata"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("exchange_name", th.StringType),
        th.Property("instrument_type", th.StringType),
        th.Property("first_trade_date", th.NumberType),
        th.Property("regular_market_time", th.NumberType),
        th.Property("gmtoffset", th.NumberType),
        th.Property("exchange_timezone_name", th.StringType),
        th.Property("regular_market_price", th.NumberType),
        th.Property("chart_previous_close", th.NumberType),
        th.Property("previous_close", th.NumberType),
        th.Property("scale", th.NumberType),
        th.Property("price_hint", th.NumberType),
        th.Property("current_trading_period", th.NumberType),
        th.Property("trading_periods", th.NumberType),
        th.Property("data_granularity", th.StringType),
        th.Property("range", th.StringType),
        th.Property("valid_ranges", th.ArrayType(th.StringType)),
        th.Property("current_trading_period_pre_timezone", th.StringType),
        th.Property("current_trading_period_pre_start", th.NumberType),
        th.Property("current_trading_period_pre_end", th.NumberType),
        th.Property("current_trading_period_pre_gmtoffset", th.NumberType),
        th.Property("current_trading_period_regular_timezone", th.StringType),
        th.Property("current_trading_period_regular_start", th.NumberType),
        th.Property("current_trading_period_regular_end", th.NumberType),
        th.Property("current_trading_period_regular_gmtoffset", th.NumberType),
        th.Property("current_trading_period_post_timezone", th.StringType),
        th.Property("current_trading_period_post_start", th.NumberType),
        th.Property("current_trading_period_post_end", th.NumberType),
        th.Property("current_trading_period_post_gmtoffset", th.NumberType),
        th.Property("trading_period_pre_start", th.DateTimeType),
        th.Property("trading_period_pre_end", th.DateTimeType),
        th.Property("trading_period_start", th.DateTimeType),
        th.Property("trading_period_end", th.DateTimeType),
        th.Property("trading_period_post_start", th.DateTimeType),
        th.Property("trading_period_post_end", th.DateTimeType),
        th.Property("full_exchange_name", th.StringType),
        th.Property("has_pre_post_market_data", th.BooleanType),
        th.Property("fifty_two_week_high", th.NumberType),
        th.Property("fifty_two_week_low", th.NumberType),
        th.Property("regular_market_day_high", th.NumberType),
        th.Property("regular_market_day_low", th.NumberType),
        th.Property("regular_market_volume", th.NumberType),
        th.Property("long_name", th.StringType),
        th.Property("short_name", th.StringType),
    ).to_dict()


class InfoStream(FinancialStream):
    name = "info"
    method_name = "get_info"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType),
        th.Property("ticker", th.StringType),
        th.Property("address1", th.StringType),
        th.Property("address2", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zip", th.StringType),
        th.Property("country", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("website", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("industry_key", th.StringType),
        th.Property("industry_disp", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("sector_key", th.StringType),
        th.Property("sector_disp", th.StringType),
        th.Property("long_business_summary", th.StringType),
        th.Property("full_time_employees", th.NumberType),
        th.Property("company_officers", th.StringType),
        th.Property("audit_risk", th.NumberType),
        th.Property("board_risk", th.NumberType),
        th.Property("compensation_risk", th.NumberType),
        th.Property("share_holder_rights_risk", th.NumberType),
        th.Property("overall_risk", th.NumberType),
        th.Property("governance_epoch_date", th.NumberType),
        th.Property("compensation_as_of_epoch_date", th.NumberType),
        th.Property("ir_website", th.StringType),
        th.Property("executive_team", th.StringType),
        th.Property("max_age", th.NumberType),
        th.Property("price_hint", th.NumberType),
        th.Property("previous_close", th.NumberType),
        th.Property("open", th.NumberType),
        th.Property("day_low", th.NumberType),
        th.Property("day_high", th.NumberType),
        th.Property("regular_market_previous_close", th.NumberType),
        th.Property("regular_market_open", th.NumberType),
        th.Property("regular_market_day_low", th.NumberType),
        th.Property("regular_market_day_high", th.NumberType),
        th.Property("dividend_rate", th.NumberType),
        th.Property("dividend_yield", th.NumberType),
        th.Property("ex_dividend_date", th.NumberType),
        th.Property("payout_ratio", th.NumberType),
        th.Property("five_year_avg_dividend_yield", th.NumberType),
        th.Property("beta", th.NumberType),
        th.Property("trailing_pe", th.NumberType),
        th.Property("forward_pe", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("regular_market_volume", th.NumberType),
        th.Property("average_volume", th.NumberType),
        th.Property("average_volume10days", th.NumberType),
        th.Property("average_daily_volume10_day", th.NumberType),
        th.Property("bid", th.NumberType),
        th.Property("ask", th.NumberType),
        th.Property("bid_size", th.NumberType),
        th.Property("ask_size", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("fifty_two_week_low", th.NumberType),
        th.Property("fifty_two_week_high", th.NumberType),
        th.Property("price_to_sales_trailing12_months", th.NumberType),
        th.Property("fifty_day_average", th.NumberType),
        th.Property("two_hundred_day_average", th.NumberType),
        th.Property("trailing_annual_dividend_rate", th.NumberType),
        th.Property("trailing_annual_dividend_yield", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("tradeable", th.BooleanType),
        th.Property("enterprise_value", th.NumberType),
        th.Property("profit_margins", th.NumberType),
        th.Property("float_shares", th.NumberType),
        th.Property("shares_outstanding", th.NumberType),
        th.Property("shares_short", th.NumberType),
        th.Property("shares_short_prior_month", th.NumberType),
        th.Property("shares_short_previous_month_date", th.NumberType),
        th.Property("date_short_interest", th.NumberType),
        th.Property("shares_percent_shares_out", th.NumberType),
        th.Property("held_percent_insiders", th.NumberType),
        th.Property("held_percent_institutions", th.NumberType),
        th.Property("short_ratio", th.NumberType),
        th.Property("short_percent_of_float", th.NumberType),
        th.Property("implied_shares_outstanding", th.NumberType),
        th.Property("book_value", th.NumberType),
        th.Property("price_to_book", th.NumberType),
        th.Property("last_fiscal_year_end", th.NumberType),
        th.Property("next_fiscal_year_end", th.NumberType),
        th.Property("most_recent_quarter", th.NumberType),
        th.Property("earnings_quarterly_growth", th.NumberType),
        th.Property("net_income_to_common", th.NumberType),
        th.Property("trailing_eps", th.NumberType),
        th.Property("forward_eps", th.NumberType),
        th.Property("last_split_factor", th.StringType),
        th.Property("last_split_date", th.NumberType),
        th.Property("enterprise_to_revenue", th.NumberType),
        th.Property("enterprise_to_ebitda", th.NumberType),
        th.Property("change_52wk", th.NumberType),
        th.Property("sand_p52_week_change", th.NumberType),
        th.Property("last_dividend_value", th.NumberType),
        th.Property("last_dividend_date", th.NumberType),
        th.Property("quote_type", th.StringType),
        th.Property("current_price", th.NumberType),
        th.Property("target_high_price", th.NumberType),
        th.Property("target_low_price", th.NumberType),
        th.Property("target_mean_price", th.NumberType),
        th.Property("target_median_price", th.NumberType),
        th.Property("recommendation_mean", th.NumberType),
        th.Property("recommendation_key", th.StringType),
        th.Property("number_of_analyst_opinions", th.NumberType),
        th.Property("total_cash", th.NumberType),
        th.Property("total_cash_per_share", th.NumberType),
        th.Property("ebitda", th.NumberType),
        th.Property("total_debt", th.NumberType),
        th.Property("quick_ratio", th.NumberType),
        th.Property("current_ratio", th.NumberType),
        th.Property("total_revenue", th.NumberType),
        th.Property("debt_to_equity", th.NumberType),
        th.Property("revenue_per_share", th.NumberType),
        th.Property("return_on_assets", th.NumberType),
        th.Property("return_on_equity", th.NumberType),
        th.Property("gross_profits", th.NumberType),
        th.Property("free_cashflow", th.NumberType),
        th.Property("operating_cashflow", th.NumberType),
        th.Property("earnings_growth", th.NumberType),
        th.Property("revenue_growth", th.NumberType),
        th.Property("gross_margins", th.NumberType),
        th.Property("ebitda_margins", th.NumberType),
        th.Property("operating_margins", th.NumberType),
        th.Property("financial_currency", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("language", th.StringType),
        th.Property("region", th.StringType),
        th.Property("type_disp", th.StringType),
        th.Property("quote_source_name", th.StringType),
        th.Property("triggerable", th.BooleanType),
        th.Property("custom_price_alert_confidence", th.StringType),
        th.Property("corporate_actions", th.StringType),
        th.Property("post_market_time", th.StringType),
        th.Property("regular_market_time", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("message_board_id", th.StringType),
        th.Property("exchange_timezone_name", th.StringType),
        th.Property("exchange_timezone_short_name", th.StringType),
        th.Property("gmt_off_set_milliseconds", th.NumberType),
        th.Property("market", th.StringType),
        th.Property("esg_populated", th.BooleanType),
        th.Property("regular_market_change_percent", th.NumberType),
        th.Property("regular_market_price", th.NumberType),
        th.Property("dividend_date", th.NumberType),
        th.Property("earnings_timestamp", th.NumberType),
        th.Property("earnings_timestamp_start", th.NumberType),
        th.Property("earnings_timestamp_end", th.NumberType),
        th.Property("earnings_call_timestamp_start", th.NumberType),
        th.Property("earnings_call_timestamp_end", th.NumberType),
        th.Property("is_earnings_date_estimate", th.BooleanType),
        th.Property("eps_trailing_twelve_months", th.NumberType),
        th.Property("eps_forward", th.NumberType),
        th.Property("eps_current_year", th.NumberType),
        th.Property("price_eps_current_year", th.NumberType),
        th.Property("fifty_day_average_change", th.NumberType),
        th.Property("fifty_day_average_change_percent", th.NumberType),
        th.Property("two_hundred_day_average_change", th.NumberType),
        th.Property("two_hundred_day_average_change_percent", th.NumberType),
        th.Property("source_interval", th.NumberType),
        th.Property("exchange_data_delayed_by", th.NumberType),
        th.Property("average_analyst_rating", th.StringType),
        th.Property("crypto_tradeable", th.BooleanType),
        th.Property("has_pre_post_market_data", th.BooleanType),
        th.Property("first_trade_date_milliseconds", th.NumberType),
        th.Property("post_market_change_percent", th.NumberType),
        th.Property("post_market_price", th.NumberType),
        th.Property("post_market_change", th.NumberType),
        th.Property("regular_market_change", th.NumberType),
        th.Property("regular_market_day_range", th.StringType),
        th.Property("full_exchange_name", th.StringType),
        th.Property("average_daily_volume3_month", th.NumberType),
        th.Property("fifty_two_week_low_change", th.NumberType),
        th.Property("fifty_two_week_low_change_percent", th.NumberType),
        th.Property("fifty_two_week_range", th.StringType),
        th.Property("fifty_two_week_high_change", th.NumberType),
        th.Property("fifty_two_week_high_change_percent", th.NumberType),
        th.Property("fifty_two_week_change_percent", th.NumberType),
        th.Property("short_name", th.StringType),
        th.Property("long_name", th.StringType),
        th.Property("market_state", th.BooleanType),
        th.Property("display_name", th.StringType),
        th.Property("trailing_peg_ratio", th.NumberType),
        th.Property("ipo_expected_date", th.DateTimeType),
        th.Property("prev_name", th.StringType),
        th.Property("name_change_date", th.DateTimeType),
        th.Property("fax", th.StringType),
        th.Property("uuid", th.StringType),
        th.Property("underlying_symbol", th.StringType),
    ).to_dict()


class IncomeStmtStream(FinancialStream):
    name = "income_stmt"
    method_name = "get_income_stmt"
    schema = INCOME_STMT_SCHEMA


class InsiderPurchasesStream(FinancialStream):
    name = "insider_purchases"
    method_name = "get_insider_purchases"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType),
        th.Property("insider_purchases_last_6m", th.StringType),
        th.Property("shares", th.NumberType),
        th.Property("trans", th.NumberType),
        th.Property("insider_purchases_last", th.NumberType),
    ).to_dict()


class InsiderRosterHoldersStream(FinancialStream):
    name = "insider_roster_holders"
    method_name = "get_insider_roster_holders"
    schema = th.PropertiesList(
        th.Property("latest_transaction_date", th.DateTimeType),
        th.Property("ticker", th.StringType),
        th.Property("name", th.StringType),
        th.Property("position", th.StringType),
        th.Property("url", th.StringType),
        th.Property("most_recent_transaction", th.StringType),
        th.Property("shares_owned_indirectly", th.NumberType),
        th.Property("position_indirect_date", th.NumberType),
        th.Property("shares_owned_directly", th.NumberType),
        th.Property("position_direct_date", th.DateTimeType),
        th.Property("position_summary", th.NumberType),
        th.Property("position_summary_date", th.StringType),
    ).to_dict()


class InsiderTransactionsStream(FinancialStream):
    name = "insider_transactions"
    method_name = "get_insider_transactions"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("start_date", th.DateTimeType),
        th.Property("shares", th.NumberType),
        th.Property("url", th.StringType),
        th.Property("text", th.StringType),
        th.Property("insider", th.StringType),
        th.Property("position", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("ownership", th.StringType),
        th.Property("value", th.NumberType),
    ).to_dict()


class InstitutionalHoldersStream(FinancialStream):
    name = "institutional_holders"
    method_name = "get_institutional_holders"
    schema = th.PropertiesList(
        th.Property("date_reported", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType),
        th.Property("holder", th.StringType),
        th.Property("pct_held", th.NumberType),
        th.Property("shares", th.NumberType),
        th.Property("value", th.NumberType),
        th.Property("pct_change", th.NumberType),
    ).to_dict()


class IsInStream(FinancialStream):
    name = "isin"
    method_name = "get_isin"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType, required=True),
        th.Property("value", th.StringType),
    ).to_dict()


class MajorHoldersStream(FinancialStream):
    name = "major_holders"
    method_name = "get_major_holders"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType),
        th.Property("breakdown", th.StringType),
        th.Property("value", th.NumberType),
    ).to_dict()


class MutualFundHoldersStream(FinancialStream):
    name = "mutualfund_holders"
    method_name = "get_mutualfund_holders"
    schema = th.PropertiesList(
        th.Property("date_reported", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType),
        th.Property("holder", th.StringType),
        th.Property("pct_held", th.NumberType),
        th.Property("shares", th.NumberType),
        th.Property("value", th.NumberType),
        th.Property("pct_change", th.NumberType),
    ).to_dict()


class NewsStream(FinancialStream):
    name = "news"
    method_name = "get_news"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType),
        th.Property("id", th.StringType),
        th.Property("content", th.StringType),
    ).to_dict()


class RecommendationsStream(FinancialStream):
    name = "recommendations"
    method_name = "get_recommendations"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("timestamp_extracted", th.DateTimeType),
        th.Property("period", th.StringType),
        th.Property("strong_buy", th.NumberType),
        th.Property("buy", th.NumberType),
        th.Property("hold", th.NumberType),
        th.Property("sell", th.NumberType),
        th.Property("strong_sell", th.NumberType),
    ).to_dict()


class RecommendationsSummaryStream(FinancialStream):
    name = "recommendations_summary"
    method_name = "get_recommendations_summary"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("timestamp_extracted", th.DateTimeType),
        th.Property("period", th.StringType),
        th.Property("strong_buy", th.NumberType),
        th.Property("buy", th.NumberType),
        th.Property("hold", th.NumberType),
        th.Property("sell", th.NumberType),
        th.Property("strong_sell", th.NumberType),
    ).to_dict()


class RevenueEstimateStream(FinancialStream):
    name = "revenue_estimate"
    method_name = "get_revenue_estimate"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("period", th.StringType),
        th.Property("avg", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("number_of_analysts", th.NumberType),
        th.Property("year_ago_revenue", th.NumberType),
        th.Property("growth", th.NumberType),
    ).to_dict()


class SecFilingsStream(FinancialStream):
    name = "sec_filings"
    method_name = "get_sec_filings"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("epoch_date", th.NumberType, required=True),
        th.Property("type", th.StringType),
        th.Property("title", th.StringType),
        th.Property("edgar_url", th.StringType),
        th.Property("exhibits", th.StringType),
        th.Property("max_age", th.NumberType),
        th.Property("timestamp_extracted", th.DateTimeType),
    ).to_dict()


class SharesFullStream(FinancialStream):
    name = "shares_full"
    method_name = "get_shares_full"
    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("timestamp_tz_aware", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("amount", th.NumberType),
    ).to_dict()


class SplitsStream(FinancialStream):
    name = "splits"
    method_name = "get_splits"
    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("timestamp_tz_aware", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("stock_splits", th.NumberType),
    ).to_dict()


class SustainabilityStream(FinancialStream):
    name = "sustainability"
    method_name = "get_sustainability"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType),
        th.Property("max_age", th.NumberType),
        th.Property("total_esg", th.NumberType),
        th.Property("environment_score", th.NumberType),
        th.Property("social_score", th.NumberType),
        th.Property("governance_score", th.NumberType),
        th.Property("rating_year", th.NumberType),
        th.Property("rating_month", th.NumberType),
        th.Property("highest_controversy", th.NumberType),
        th.Property("peer_count", th.NumberType),
        th.Property("esg_performance", th.StringType),
        th.Property("peer_group", th.StringType),
        th.Property("related_controversy", th.StringType),
        th.Property("peer_esg_score_performance", th.StringType),
        th.Property("peer_governance_performance", th.StringType),
        th.Property("peer_social_performance", th.StringType),
        th.Property("peer_environment_performance", th.StringType),
        th.Property("peer_highest_controversy_performance", th.StringType),
        th.Property("percentile", th.NumberType),
        th.Property("environment_percentile", th.NumberType),
        th.Property("social_percentile", th.NumberType),
        th.Property("governance_percentile", th.NumberType),
        th.Property("adult", th.BooleanType),
        th.Property("alcoholic", th.BooleanType),
        th.Property("animal_testing", th.BooleanType),
        th.Property("catholic", th.BooleanType),
        th.Property("controversial_weapons", th.BooleanType),
        th.Property("small_arms", th.BooleanType),
        th.Property("fur_leather", th.BooleanType),
        th.Property("gambling", th.BooleanType),
        th.Property("gmo", th.BooleanType),
        th.Property("military_contract", th.BooleanType),
        th.Property("nuclear", th.BooleanType),
        th.Property("pesticides", th.BooleanType),
        th.Property("palm_oil", th.BooleanType),
        th.Property("coal", th.BooleanType),
        th.Property("tobacco", th.BooleanType),
    ).to_dict()


class OptionChainStream(FinancialStream):
    name = "option_chain"
    method_name = "option_chain"
    schema = th.PropertiesList(
        th.Property("last_trade_date", th.DateTimeType, required=True),
        th.Property("last_trade_date_tz_aware", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("ticker", th.StringType),
        th.Property("option_type", th.StringType),
        th.Property("contract_symbol", th.StringType),
        th.Property("strike", th.NumberType),
        th.Property("last_price", th.NumberType),
        th.Property("bid", th.NumberType),
        th.Property("ask", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("percent_change", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("open_interest", th.NumberType),
        th.Property("implied_volatility", th.NumberType),
        th.Property("in_the_money", th.BooleanType),
        th.Property("contract_size", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("metadata", th.StringType),
        th.Property("timestamp_extracted", th.DateTimeType),
    ).to_dict()


class OptionsStream(FinancialStream):
    name = "options"
    method_name = "options"
    schema = th.PropertiesList(
        th.Property("timestamp_extracted", th.DateTimeType, required=True),
        th.Property("ticker", th.StringType),
        th.Property("expiration_date", th.DateTimeType),
    ).to_dict()


class QuarterlyBalanceSheetStream(FinancialStream):
    name = "quarterly_balance_sheet"
    method_name = "quarterly_balance_sheet"
    schema = BALANCE_SHEET_SCHEMA


class QuarterlyCashFlowStream(FinancialStream):
    name = "quarterly_cash_flow"
    method_name = "quarterly_cash_flow"
    schema = CASH_FLOW_SCHEMA


class QuarterlyFinancialsStream(FinancialStream):
    name = "quarterly_financials"
    method_name = "quarterly_financials"
    schema = FINANCIALS_SCHEMA


class QuarterlyIncomeStmtStream(FinancialStream):
    name = "quarterly_income_stmt"
    method_name = "quarterly_income_stmt"
    schema = INCOME_STMT_SCHEMA


class TtmCashFlowStream(FinancialStream):
    name = "ttm_cash_flow"
    method_name = "ttm_cash_flow"
    schema = CASH_FLOW_SCHEMA


class TtmFinancialsStream(FinancialStream):
    name = "ttm_financials"
    method_name = "ttm_financials"
    schema = FINANCIALS_SCHEMA


class TtmIncomeStmtStream(FinancialStream):
    name = "ttm_income_stmt"
    method_name = "ttm_income_stmt"
    schema = INCOME_STMT_SCHEMA


class UpgradesDowngradesStream(FinancialStream):
    name = "upgrades_downgrades"
    method_name = "get_upgrades_downgrades"
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType),
        th.Property("grade_date", th.DateTimeType),
        th.Property("firm", th.StringType),
        th.Property("to_grade", th.StringType),
        th.Property("from_grade", th.StringType),
        th.Property("action", th.StringType),
        th.Property("price_target_action", th.StringType),
        th.Property("current_price_target", th.NumberType),
    ).to_dict()
