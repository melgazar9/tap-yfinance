from singer_sdk import typing as th

def get_schema(schema_category):
    if schema_category in ['stock_prices', 'futures_prices', 'forex_prices', 'crypto_prices']:
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
            th.Property("replication_key", th.StringType)
        ).to_dict()

    elif schema_category in ['stock_prices_wide', 'futures_prices_wide', 'forex_prices_wide', 'crypto_prices_wide']:
        schema = th.PropertiesList(  # potentially a dynamic number of columns
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("data", th.AnyType, required=True)
        ).to_dict()

    elif schema_category == 'stock_tickers':
        schema = th.PropertiesList(
            th.Property("yahoo_ticker", th.StringType, required=True),
            th.Property("google_ticker", th.StringType),
            th.Property("bloomberg_ticker", th.StringType),
            th.Property("numerai_ticker", th.StringType),
            th.Property("yahoo_ticker_old", th.StringType),
            th.Property("yahoo_valid_pts", th.BooleanType),
            th.Property("yahoo_valid_numerai", th.BooleanType)
        ).to_dict()

    elif schema_category == 'futures_tickers':
        schema = th.PropertiesList(
            th.Property("yahoo_ticker", th.StringType, required=True),
            th.Property("name", th.StringType),
            th.Property("last_price", th.NumberType),
            th.Property("market_time", th.StringType),
            th.Property("change", th.NumberType),
            th.Property("pct_change", th.StringType),
            th.Property("volume", th.StringType),
            th.Property("open_interest", th.StringType)
        ).to_dict()

    elif schema_category == 'forex_tickers':
        schema = th.PropertiesList(
            th.Property("yahoo_ticker", th.StringType, required=True),
            th.Property("name", th.StringType),
            th.Property("bloomberg_ticker", th.StringType),
            th.Property("last_price", th.StringType),
            th.Property("change", th.StringType),
            th.Property("pct_change", th.StringType)
        ).to_dict()

    elif schema_category == 'crypto_tickers':
        schema = th.PropertiesList(
            th.Property("yahoo_ticker", th.StringType, required=True),
            th.Property("yahoo_name", th.StringType),
            th.Property("price_intraday", th.NumberType),
            th.Property("change", th.NumberType),
            th.Property("pct_change", th.StringType),
            th.Property("market_cap", th.StringType),
            th.Property("volume_in_currency_since_0_00_utc", th.StringType),
            th.Property("volume_in_currency_24_hr", th.StringType),
            th.Property("total_volume_all_currencies_24_hr", th.StringType),
            th.Property("circulating_supply", th.StringType)
        ).to_dict()

    ### financials ###

    elif schema_category == 'get_actions':
        schema = th.PropertiesList(
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("timestamp_tz_aware", th.StringType),
            th.Property("timezone", th.StringType),
            th.Property("ticker", th.StringType),
            th.Property("dividends", th.NumberType),
            th.Property("stock_splits", th.NumberType)
        ).to_dict()

    elif schema_category == 'get_balance_sheet':
        schema = th.PropertiesList(
            th.Property('date', th.DateTimeType),
            th.Property('ticker', th.StringType),
            th.Property('treasury_shares_number', th.NumberType),
            th.Property('ordinary_shares_number', th.NumberType),
            th.Property('share_issued', th.NumberType),
            th.Property('net_debt', th.NumberType),
            th.Property('total_debt', th.NumberType),
            th.Property('tangible_book_value', th.NumberType),
            th.Property('invested_capital', th.NumberType),
            th.Property('working_capital', th.NumberType),
            th.Property('net_tangible_assets', th.NumberType),
            th.Property('common_stock_equity', th.NumberType),
            th.Property('total_capitalization', th.NumberType),
            th.Property('total_equity_gross_minority_interest', th.NumberType),
            th.Property('stockholders_equity', th.NumberType),
            th.Property('gains_losses_not_affecting_retained_earnings', th.NumberType),
            th.Property('other_equity_adjustments', th.NumberType),
            th.Property('retained_earnings', th.NumberType),
            th.Property('capital_stock', th.NumberType),
            th.Property('common_stock', th.NumberType),
            th.Property('total_liabilities_net_minority_interest', th.NumberType),
            th.Property('total_non_current_liabilities_net_minority_interest', th.NumberType),
            th.Property('other_non_current_liabilities', th.NumberType),
            th.Property('tradeand_other_payables_non_current', th.NumberType),
            th.Property('long_term_debt_and_capital_lease_obligation', th.NumberType),
            th.Property('long_term_debt', th.NumberType),
            th.Property('current_liabilities', th.NumberType),
            th.Property('other_current_liabilities', th.NumberType),
            th.Property('current_deferred_liabilities', th.NumberType),
            th.Property('current_deferred_revenue', th.NumberType),
            th.Property('current_debt_and_capital_lease_obligation', th.NumberType),
            th.Property('current_debt', th.NumberType),
            th.Property('other_current_borrowings', th.NumberType),
            th.Property('commercial_paper', th.NumberType),
            th.Property('payables_and_accrued_expenses', th.NumberType),
            th.Property('payables', th.NumberType),
            th.Property('accounts_payable', th.NumberType),
            th.Property('total_assets', th.NumberType),
            th.Property('total_non_current_assets', th.NumberType),
            th.Property('other_non_current_assets', th.NumberType),
            th.Property('investments_and_advances', th.NumberType),
            th.Property('other_investments', th.NumberType),
            th.Property('investmentin_financial_assets', th.NumberType),
            th.Property('available_for_sale_securities', th.NumberType),
            th.Property('net_p_p_e', th.NumberType),
            th.Property('accumulated_depreciation', th.NumberType),
            th.Property('gross_ppe', th.NumberType),
            th.Property('leases', th.NumberType),
            th.Property('machinery_furniture_equipment', th.NumberType),
            th.Property('land_and_improvements', th.NumberType),
            th.Property('properties', th.NumberType),
            th.Property('current_assets', th.NumberType),
            th.Property('other_current_assets', th.NumberType),
            th.Property('inventory', th.NumberType),
            th.Property('receivables', th.NumberType),
            th.Property('other_receivables', th.NumberType),
            th.Property('accounts_receivable', th.NumberType),
            th.Property('cash_cash_equivalents_and_short_term_investments', th.NumberType),
            th.Property('other_short_term_investments', th.NumberType),
            th.Property('cash_and_cash_equivalents', th.NumberType),
            th.Property('cash_equivalents', th.NumberType),
            th.Property('cash_financial', th.NumberType)
        ).to_dict()

    elif schema_category == 'get_cash_flow':
        schema = th.PropertiesList(
            th.Property("date", th.DateTimeType, required=True),
            th.Property("ticker", th.StringType),
            th.Property("beginning_cash_position", th.NumberType),
            th.Property("capital_expenditure", th.NumberType),
            th.Property("cash_dividends_paid", th.NumberType),
            th.Property("cash_flow_from_continuing_financing_activities", th.NumberType),
            th.Property("cash_flow_from_continuing_investing_activities", th.NumberType),
            th.Property("cash_flow_from_continuing_operating_activities", th.NumberType),
            th.Property("change_in_account_payable", th.NumberType),
            th.Property("change_in_inventory", th.NumberType),
            th.Property("change_in_other_current_assets", th.NumberType),
            th.Property("change_in_other_current_liabilities", th.NumberType),
            th.Property("change_in_other_working_capital", th.NumberType),
            th.Property("change_in_payable", th.NumberType),
            th.Property("change_in_payables_and_accrued_expense", th.NumberType),
            th.Property("change_in_receivables", th.NumberType),
            th.Property("change_in_working_capital", th.NumberType),
            th.Property("changes_in_account_receivables", th.NumberType),
            th.Property("changes_in_cash", th.NumberType),
            th.Property("common_stock_dividend_paid", th.NumberType),
            th.Property("common_stock_issuance", th.NumberType),
            th.Property("common_stock_payments", th.NumberType),
            th.Property("deferred_income_tax", th.NumberType),
            th.Property("deferred_tax", th.NumberType),
            th.Property("depreciation_amortization_depletion", th.NumberType),
            th.Property("depreciation_and_amortization", th.NumberType),
            th.Property("end_cash_position", th.NumberType),
            th.Property("financing_cash_flow", th.NumberType),
            th.Property("free_cash_flow", th.NumberType),
            th.Property("income_tax_paid_supplemental_data", th.NumberType),
            th.Property("interest_paid_supplemental_data", th.NumberType),
            th.Property("investing_cash_flow", th.NumberType),
            th.Property("issuance_of_capital_stock", th.NumberType),
            th.Property("issuance_of_debt", th.NumberType),
            th.Property("long_term_debt_issuance", th.NumberType),
            th.Property("long_term_debt_payments", th.NumberType),
            th.Property("net_business_purchase_and_sale", th.NumberType),
            th.Property("net_common_stock_issuance", th.NumberType),
            th.Property("net_income_from_continuing_operations", th.NumberType),
            th.Property("net_investment_purchase_and_sale", th.NumberType),
            th.Property("net_issuance_payments_of_debt", th.NumberType),
            th.Property("net_long_term_debt_issuance", th.NumberType),
            th.Property("net_other_financing_charges", th.NumberType),
            th.Property("net_other_investing_changes", th.NumberType),
            th.Property("net_ppe_purchase_and_sale", th.NumberType),
            th.Property("net_short_term_debt_issuance", th.NumberType),
            th.Property("operating_cash_flow", th.NumberType),
            th.Property("other_non_cash_items", th.NumberType),
            th.Property("purchase_of_business", th.NumberType),
            th.Property("purchase_of_investment", th.NumberType),
            th.Property("purchase_of_ppe", th.NumberType),
            th.Property("repayment_of_debt", th.NumberType),
            th.Property("repurchase_of_capital_stock", th.NumberType),
            th.Property("sale_of_investment", th.NumberType),
            th.Property("short_term_debt_issuance", th.NumberType),
            th.Property("short_term_debt_payments", th.NumberType),
            th.Property("stock_based_compensation", th.NumberType)
        ).to_dict()

    elif schema_category == 'get_dividends':
        schema = th.PropertiesList(
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("timestamp_tz_aware", th.StringType),
            th.Property("timezone", th.StringType),
            th.Property("ticker", th.StringType),
            th.Property("dividends", th.NumberType)
        ).to_dict()

    elif schema_category == 'get_earnings_dates':
        schema = th.PropertiesList(
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("timestamp_tz_aware", th.StringType),
            th.Property("timezone", th.StringType),
            th.Property("ticker", th.StringType),
            th.Property("eps_estimate", th.NumberType),
            th.Property("reported_eps", th.NumberType),
            th.Property("pct_surprise", th.NumberType)
        ).to_dict()

    else:
        raise NotImplementedError(f'Specified schema_category {schema_category} is not supported.')

    return schema