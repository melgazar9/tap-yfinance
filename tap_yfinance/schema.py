from singer_sdk import typing as th

INCOME_STMT_SCHEMA = th.PropertiesList(
    th.Property("date", th.DateTimeType, required=True),
    th.Property("ticker", th.StringType),
    th.Property("basic_average_shares", th.NumberType),
    th.Property("basic_eps", th.NumberType),
    th.Property("cost_of_revenue", th.NumberType),
    th.Property("diluted_average_shares", th.NumberType),
    th.Property("diluted_eps", th.NumberType),
    th.Property("diluted_ni_availto_com_stockholders", th.NumberType),
    th.Property("ebit", th.NumberType),
    th.Property("ebitda", th.NumberType),
    th.Property("gross_profit", th.NumberType),
    th.Property("interest_expense", th.NumberType),
    th.Property("interest_expense_non_operating", th.NumberType),
    th.Property("interest_income_non_operating", th.NumberType),
    th.Property("net_income", th.NumberType),
    th.Property("net_income_common_stockholders", th.NumberType),
    th.Property("net_income_continuous_operations", th.NumberType),
    th.Property("net_income_from_continuing_and_discontinued_operation", th.NumberType),
    th.Property("net_income_from_continuing_operation_net_minority_interest", th.NumberType),
    th.Property("net_income_including_noncontrolling_interests", th.NumberType),
    th.Property("net_interest_income", th.NumberType),
    th.Property("net_non_operating_interest_income_expense", th.NumberType),
    th.Property("normalized_ebitda", th.NumberType),
    th.Property("normalized_income", th.NumberType),
    th.Property("operating_expense", th.NumberType),
    th.Property("operating_income", th.NumberType),
    th.Property("operating_revenue", th.NumberType),
    th.Property("other_income_expense", th.NumberType),
    th.Property("other_non_operating_income_expenses", th.NumberType),
    th.Property("pretax_income", th.NumberType),
    th.Property("reconciled_cost_of_revenue", th.NumberType),
    th.Property("reconciled_depreciation", th.NumberType),
    th.Property("research_and_development", th.NumberType),
    th.Property("selling_general_and_administration", th.NumberType),
    th.Property("tax_effect_of_unusual_items", th.NumberType),
    th.Property("tax_provision", th.NumberType),
    th.Property("tax_rate_for_calcs", th.NumberType),
    th.Property("total_expenses", th.NumberType),
    th.Property("total_operating_income_as_reported", th.NumberType),
    th.Property("total_revenue", th.NumberType),
    th.Property('interest_income', th.NumberType),
    th.Property('special_income_charges', th.NumberType),
    th.Property('depreciation_and_amortization_in_income_statement', th.NumberType),
    th.Property('depreciation_amortization_depletion_income_statement', th.NumberType),
    th.Property('selling_and_marketing_expense', th.NumberType),
    th.Property('gain_on_sale_of_ppe', th.NumberType),
    th.Property('general_and_administrative_expense', th.NumberType),
    th.Property('other_gand_a', th.NumberType),
    th.Property('write_off', th.NumberType),
    th.Property('amortization_of_intangibles_income_statement', th.NumberType),
    th.Property('other_operating_expenses', th.NumberType),
    th.Property('total_unusual_items_excluding_goodwill', th.NumberType),
    th.Property('amortization', th.NumberType),
    th.Property('total_unusual_items', th.NumberType),
    th.Property('minority_interests', th.NumberType),
    th.Property('gain_on_sale_of_security', th.NumberType),
    th.Property('otherunder_preferred_stock_dividend', th.NumberType),
    th.Property('total_other_finance_cost', th.NumberType),
    th.Property('depreciation_income_statement', th.NumberType),
    th.Property('impairment_of_capital_assets', th.NumberType),
    th.Property('other_special_charges', th.NumberType),
    th.Property('rent_and_landing_fees', th.NumberType),
    th.Property('rent_expense_supplemental', th.NumberType),
    th.Property('restructuring_and_mergern_acquisition', th.NumberType),
    th.Property('net_income_discontinuous_operations', th.NumberType),
    th.Property('gain_on_sale_of_business', th.NumberType),
    th.Property('salaries_and_wages', th.NumberType),
    th.Property('earnings_from_equity_interest_net_of_tax', th.NumberType),
    th.Property('average_dilution_earnings', th.NumberType),
    th.Property('preferred_stock_dividends', th.NumberType),
    th.Property('insurance_and_claims', th.NumberType),
    th.Property('earnings_from_equity_interest', th.NumberType),
    th.Property('other_taxes', th.NumberType),
    th.Property('net_income_extraordinary', th.NumberType),
    th.Property('provision_for_doubtful_accounts', th.NumberType),
    th.Property('securities_amortization', th.NumberType),
    th.Property('excise_taxes', th.NumberType),
    th.Property('net_income_from_tax_loss_carryforward', th.NumberType),
    th.Property('depletion_income_statement', th.NumberType),
    th.Property('depreciation', th.NumberType)
).to_dict()



BALANCE_SHEET_SCHEMA = th.PropertiesList(
    th.Property("date", th.DateTimeType, required=True),
    th.Property("ticker", th.StringType),
    th.Property("capital_stock", th.NumberType),
    th.Property("current_capital_lease_obligation", th.NumberType),
    th.Property("payables_and_accrued_expenses", th.NumberType),
    th.Property("leases", th.NumberType),
    th.Property("common_stock", th.NumberType),
    th.Property("other_current_borrowings", th.NumberType),
    th.Property("other_current_liabilities", th.NumberType),
    th.Property("machinery_furniture_equipment", th.NumberType),
    th.Property("working_capital", th.NumberType),
    th.Property("other_equity_adjustments", th.NumberType),
    th.Property("total_debt", th.NumberType),
    th.Property("inventory", th.NumberType),
    th.Property("gains_losses_not_affecting_retained_earnings", th.NumberType),
    th.Property("current_deferred_revenue", th.NumberType),
    th.Property("cash_financial", th.NumberType),
    th.Property("retained_earnings", th.NumberType),
    th.Property("current_debt_and_capital_lease_obligation", th.NumberType),
    th.Property("other_non_current_assets", th.NumberType),
    th.Property("capital_lease_obligations", th.NumberType),
    th.Property("total_non_current_liabilities_net_minority_interest", th.NumberType),
    th.Property("cash_equivalents", th.NumberType),
    th.Property("payables", th.NumberType),
    th.Property("prepaid_assets", th.NumberType),
    th.Property("available_for_sale_securities", th.NumberType),
    th.Property("tangible_book_value", th.NumberType),
    th.Property("gross_accounts_receivable", th.NumberType),
    th.Property("investments_and_advances", th.NumberType),
    th.Property("other_current_assets", th.NumberType),
    th.Property("other_intangible_assets", th.NumberType),
    th.Property("accounts_receivable", th.NumberType),
    th.Property("total_non_current_assets", th.NumberType),
    th.Property("net_debt", th.NumberType),
    th.Property("net_ppe", th.NumberType),
    th.Property("land_and_improvements", th.NumberType),
    th.Property("investmentin_financial_assets", th.NumberType),
    th.Property("other_receivables", th.NumberType),
    th.Property("long_term_debt_and_capital_lease_obligation", th.NumberType),
    th.Property("cash_cash_equivalents_and_short_term_investments", th.NumberType),
    th.Property("stockholders_equity", th.NumberType),
    th.Property("total_liabilities_net_minority_interest", th.NumberType),
    th.Property("ordinary_shares_number", th.NumberType),
    th.Property("other_properties", th.NumberType),
    th.Property("share_issued", th.NumberType),
    th.Property("receivables", th.NumberType),
    th.Property("cash_and_cash_equivalents", th.NumberType),
    th.Property("long_term_debt", th.NumberType),
    th.Property("commercial_paper", th.NumberType),
    th.Property("accumulated_depreciation", th.NumberType),
    th.Property("treasury_shares_number", th.NumberType),
    th.Property("current_liabilities", th.NumberType),
    th.Property("dueto_related_parties_current", th.NumberType),
    th.Property("allowance_for_doubtful_accounts_receivable", th.NumberType),
    th.Property("construction_in_progress", th.NumberType),
    th.Property("current_assets", th.NumberType),
    th.Property("buildings_and_improvements", th.NumberType),
    th.Property("goodwill", th.NumberType),
    th.Property("common_stock_equity", th.NumberType),
    th.Property("other_payable", th.NumberType),
    th.Property("properties", th.NumberType),
    th.Property("additional_paid_in_capital", th.NumberType),
    th.Property("total_assets", th.NumberType),
    th.Property("other_investments", th.NumberType),
    th.Property("total_equity_gross_minority_interest", th.NumberType),
    th.Property("pensionand_other_post_retirement_benefit_plans_current", th.NumberType),
    th.Property("long_term_capital_lease_obligation", th.NumberType),
    th.Property("invested_capital", th.NumberType),
    th.Property("total_capitalization", th.NumberType),
    th.Property("goodwill_and_other_intangible_assets", th.NumberType),
    th.Property("long_term_equity_investment", th.NumberType),
    th.Property("current_debt", th.NumberType),
    th.Property("accounts_payable", th.NumberType),
    th.Property("other_non_current_liabilities", th.NumberType),
    th.Property("total_tax_payable", th.NumberType),
    th.Property("current_accrued_expenses", th.NumberType),
    th.Property("current_deferred_liabilities", th.NumberType),
    th.Property("tradeand_other_payables_non_current", th.NumberType),
    th.Property("gross_ppe", th.NumberType),
    th.Property("other_short_term_investments", th.NumberType),
    th.Property("net_tangible_assets", th.NumberType),
    th.Property('minority_interest', th.NumberType),
    th.Property('other_equity_interest', th.NumberType),
    th.Property('non_current_pension_and_other_postretirement_benefit_plans', th.NumberType),
    th.Property('non_current_deferred_taxes_liabilities', th.NumberType),
    th.Property('current_provisions', th.NumberType),
    th.Property('non_current_prepaid_assets', th.NumberType),
    th.Property('non_current_deferred_taxes_assets', th.NumberType),
    th.Property('financial_assets', th.NumberType),
    th.Property('financial_assets_designatedas_fair_value_through_profitor_loss_total', th.NumberType),
    th.Property('investmentsin_associatesat_cost', th.NumberType),
    th.Property('hedging_assets_current', th.NumberType),
    th.Property('finished_goods', th.NumberType),
    th.Property('work_in_process', th.NumberType),
    th.Property('raw_materials', th.NumberType),
    th.Property('taxes_receivable', th.NumberType),
    th.Property('preferred_shares_number',  th.NumberType),
    th.Property('preferred_stock_equity', th.NumberType),
    th.Property('preferred_stock', th.NumberType),
    th.Property('derivative_product_liabilities', th.NumberType),
    th.Property('non_current_deferred_revenue', th.NumberType),
    th.Property('long_term_provisions', th.NumberType),
    th.Property('investments_in_other_ventures_under_equity_method', th.NumberType),
    th.Property('investment_properties', th.NumberType),
    th.Property('assets_held_for_sale_current', th.NumberType),
    th.Property('inventories_adjustments_allowances', th.NumberType),
    th.Property('other_inventories', th.NumberType),
    th.Property('fixed_assets_revaluation_reserve', th.NumberType),
    th.Property('held_to_maturity_securities', th.NumberType),
    th.Property('investmentsin_joint_venturesat_cost', th.NumberType),
    th.Property('receivables_adjustments_allowances', th.NumberType),
    th.Property('defined_pension_benefit', th.NumberType),
    th.Property('non_current_accrued_expenses', th.NumberType),
    th.Property('treasury_stock', th.NumberType),
    th.Property('investmentsin_subsidiariesat_cost', th.NumberType),
    th.Property('restricted_cash', th.NumberType),
    th.Property('current_deferred_taxes_liabilities', th.NumberType),
    th.Property('employee_benefits', th.NumberType),
    th.Property('non_current_deferred_liabilities', th.NumberType),
    th.Property('dividends_payable', th.NumberType),
    th.Property('income_tax_payable', th.NumberType),
    th.Property('non_current_deferred_assets', th.NumberType),
    th.Property('non_current_accounts_receivable', th.NumberType),
    th.Property('trading_securities', th.NumberType),
    th.Property('line_of_credit', th.NumberType),
    th.Property('interest_payable', th.NumberType),
    th.Property('non_current_note_receivables', th.NumberType),
    th.Property('loans_receivable', th.NumberType),
    th.Property('preferred_securities_outside_stock_equity', th.NumberType),
    th.Property('duefrom_related_parties_non_current', th.NumberType),
    th.Property('duefrom_related_parties_current', th.NumberType),
    th.Property('accrued_interest_receivable', th.NumberType),
    th.Property('current_deferred_assets', th.NumberType),
    th.Property('foreign_currency_translation_adjustments', th.NumberType),
    th.Property('liabilities_heldfor_sale_non_current', th.NumberType),
    th.Property('current_deferred_taxes_assets', th.NumberType),
    th.Property('minimum_pension_liabilities', th.NumberType),
    th.Property('current_notes_payable', th.NumberType),
    th.Property('notes_receivable', th.NumberType),
    th.Property('unrealized_gain_loss', th.NumberType),
    th.Property('general_partnership_capital', th.NumberType),
    th.Property('limited_partnership_capital', th.NumberType),
    th.Property('total_partnership_capital', th.NumberType),
    th.Property('dueto_related_parties_non_current', th.NumberType),
    th.Property('restricted_common_stock', th.NumberType),
    th.Property('other_capital_stock', th.NumberType)
).to_dict()


CASH_FLOW_SCHEMA = th.PropertiesList(
    th.Property("date", th.DateTimeType, required=True),
    th.Property("ticker", th.StringType),
    th.Property("beginning_cash_position", th.NumberType),
    th.Property("capital_expenditure", th.NumberType),
    th.Property("cash_flow_from_continuing_financing_activities", th.NumberType),
    th.Property("cash_flow_from_continuing_investing_activities", th.NumberType),
    th.Property("cash_flow_from_continuing_operating_activities", th.NumberType),
    th.Property("change_in_account_payable", th.NumberType),
    th.Property("change_in_inventory", th.NumberType),
    th.Property("change_in_other_current_assets", th.NumberType),
    th.Property("change_in_other_current_liabilities", th.NumberType),
    th.Property("change_in_payable", th.NumberType),
    th.Property("change_in_payables_and_accrued_expense", th.NumberType),
    th.Property("change_in_receivables", th.NumberType),
    th.Property("change_in_working_capital", th.NumberType),
    th.Property("changes_in_account_receivables", th.NumberType),
    th.Property("changes_in_cash", th.NumberType),
    th.Property("common_stock_payments", th.NumberType),
    th.Property("depreciation_amortization_depletion", th.NumberType),
    th.Property("depreciation_and_amortization", th.NumberType),
    th.Property("end_cash_position", th.NumberType),
    th.Property("financing_cash_flow", th.NumberType),
    th.Property("free_cash_flow", th.NumberType),
    th.Property("income_tax_paid_supplemental_data", th.NumberType),
    th.Property("interest_paid_supplemental_data", th.NumberType),
    th.Property("investing_cash_flow", th.NumberType),
    th.Property("issuance_of_debt", th.NumberType),
    th.Property("long_term_debt_issuance", th.NumberType),
    th.Property("long_term_debt_payments", th.NumberType),
    th.Property("net_common_stock_issuance", th.NumberType),
    th.Property("net_income_from_continuing_operations", th.NumberType),
    th.Property("net_investment_purchase_and_sale", th.NumberType),
    th.Property("net_issuance_payments_of_debt", th.NumberType),
    th.Property("net_long_term_debt_issuance", th.NumberType),
    th.Property("net_other_financing_charges", th.NumberType),
    th.Property("net_other_investing_changes", th.NumberType),
    th.Property("net_ppe_purchase_and_sale", th.NumberType),
    th.Property("operating_cash_flow", th.NumberType),
    th.Property("other_non_cash_items", th.NumberType),
    th.Property("purchase_of_investment", th.NumberType),
    th.Property("purchase_of_ppe", th.NumberType),
    th.Property("repayment_of_debt", th.NumberType),
    th.Property("repurchase_of_capital_stock", th.NumberType),
    th.Property("sale_of_investment", th.NumberType),
    th.Property("stock_based_compensation", th.NumberType),
    th.Property('purchase_of_business', th.NumberType),
    th.Property('sale_of_ppe', th.NumberType),
    th.Property('common_stock_dividend_paid', th.NumberType),
    th.Property('deferred_tax', th.NumberType),
    th.Property('change_in_other_working_capital', th.NumberType),
    th.Property('change_in_prepaid_assets', th.NumberType),
    th.Property('cash_dividends_paid', th.NumberType),
    th.Property('effect_of_exchange_rate_changes', th.NumberType),
    th.Property('short_term_debt_payments', th.NumberType),
    th.Property('deferred_income_tax', th.NumberType),
    th.Property('net_business_purchase_and_sale', th.NumberType),
    th.Property('unrealized_gain_loss_on_investment_securities', th.NumberType),
    th.Property('asset_impairment_charge', th.NumberType),
    th.Property('change_in_accrued_expense', th.NumberType),
    th.Property('amortization_cash_flow', th.NumberType),
    th.Property('depreciation', th.NumberType),
    th.Property('dividend_received_cfo', th.NumberType),
    th.Property('gain_loss_on_investment_securities', th.NumberType),
    th.Property('gain_loss_on_sale_of_ppe', th.NumberType),
    th.Property('interest_paid_cfo', th.NumberType),
    th.Property('interest_received_cfo', th.NumberType),
    th.Property('net_foreign_currency_exchange_gain_loss', th.NumberType),
    th.Property('net_intangibles_purchase_and_sale', th.NumberType),
    th.Property('net_investment_properties_purchase_and_sale', th.NumberType),
    th.Property('other_cash_adjustment_outside_changein_cash', th.NumberType),
    th.Property('pension_and_employee_benefit_expense', th.NumberType),
    th.Property('provisionand_write_offof_assets', th.NumberType),
    th.Property('purchase_of_intangibles', th.NumberType),
    th.Property('purchase_of_investment_properties', th.NumberType),
    th.Property('sale_of_intangibles', th.NumberType),
    th.Property('short_term_debt_issuance', th.NumberType),
    th.Property('taxes_refund_paid', th.NumberType),
    th.Property('classesof_cash_payments', th.NumberType),
    th.Property('cash_flowsfromusedin_operating_activities_direct', th.NumberType),
    th.Property('classesof_cash_receiptsfrom_operating_activities', th.NumberType),
    th.Property('interest_paid_cff', th.NumberType),
    th.Property('interest_received_cfi', th.NumberType),
    th.Property('other_cash_paymentsfrom_operating_activities', th.NumberType),
    th.Property('paymentson_behalfof_employees', th.NumberType),
    th.Property('paymentsto_suppliersfor_goodsand_services', th.NumberType),
    th.Property('receiptsfrom_customers', th.NumberType),
    th.Property('taxes_refund_paid_direct', th.NumberType),
    th.Property('sale_of_business', th.NumberType),
    th.Property('common_stock_issuance', th.NumberType),
    th.Property('issuance_of_capital_stock', th.NumberType),
    th.Property('sale_of_investment_properties', th.NumberType),
    th.Property('dividends_received_cfi', th.NumberType),
    th.Property('amortization_of_intangibles', th.NumberType),
    th.Property('operating_gains_losses', th.NumberType),
    th.Property('proceeds_from_stock_option_exercised', th.NumberType),
    th.Property('capital_expenditure_reported', th.NumberType),
    th.Property('interest_paid_direct', th.NumberType),
    th.Property('earnings_losses_from_equity_investments', th.NumberType),
    th.Property('gain_loss_on_sale_of_business', th.NumberType),
    th.Property('other_cash_adjustment_inside_changein_cash', th.NumberType),
    th.Property('net_preferred_stock_issuance', th.NumberType),
    th.Property('preferred_stock_issuance', th.NumberType),
    th.Property('amortization_of_securities', th.NumberType),
    th.Property('change_in_income_tax_payable', th.NumberType),
    th.Property('change_in_tax_payable', th.NumberType),
    th.Property('dividend_paid_cfo', th.NumberType),
    th.Property('preferred_stock_dividend_paid', th.NumberType),
    th.Property('cash_from_discontinued_operating_activities', th.NumberType),
    th.Property('preferred_stock_payments', th.NumberType),
    th.Property('cash_from_discontinued_investing_activities', th.NumberType),
    th.Property('change_in_interest_payable', th.NumberType),
    th.Property('cash_from_discontinued_financing_activities', th.NumberType),
    th.Property('other_cash_receiptsfrom_operating_activities', th.NumberType),
    th.Property('cash_flow_from_discontinued_operation', th.NumberType),
    th.Property('interest_received_direct', th.NumberType),
    th.Property('excess_tax_benefit_from_stock_based_compensation', th.NumberType),
    th.Property('depletion', th.NumberType),
    th.Property('change_in_dividend_payable', th.NumberType),
    th.Property('receiptsfrom_government_grants', th.NumberType),
    th.Property('dividends_received_direct', th.NumberType),
    th.Property('net_short_term_debt_issuance', th.NumberType),
    th.Property('dividends_paid_direct', th.NumberType)
).to_dict()


FINANCIALS_SCHEMA = th.PropertiesList(
    th.Property("date", th.DateTimeType, required=True),
    th.Property("ticker", th.StringType),
    th.Property("basic_average_shares", th.NumberType),
    th.Property("basic_eps", th.NumberType),
    th.Property("cost_of_revenue", th.NumberType),
    th.Property("diluted_average_shares", th.NumberType),
    th.Property("diluted_eps", th.NumberType),
    th.Property("diluted_ni_availto_com_stockholders", th.NumberType),
    th.Property("ebit", th.NumberType),
    th.Property("ebitda", th.NumberType),
    th.Property("gross_profit", th.NumberType),
    th.Property("interest_expense", th.NumberType),
    th.Property("interest_expense_non_operating", th.NumberType),
    th.Property("net_income", th.NumberType),
    th.Property("net_income_common_stockholders", th.NumberType),
    th.Property("net_income_continuous_operations", th.NumberType),
    th.Property("net_income_from_continuing_and_discontinued_operation", th.NumberType),
    th.Property("net_income_from_continuing_operation_net_minority_interest", th.NumberType),
    th.Property("net_income_including_noncontrolling_interests", th.NumberType),
    th.Property("net_non_operating_interest_income_expense", th.NumberType),
    th.Property("normalized_ebitda", th.NumberType),
    th.Property("normalized_income", th.NumberType),
    th.Property("operating_expense", th.NumberType),
    th.Property("operating_income", th.NumberType),
    th.Property("operating_revenue", th.NumberType),
    th.Property("other_income_expense", th.NumberType),
    th.Property("other_non_operating_income_expenses", th.NumberType),
    th.Property("pretax_income", th.NumberType),
    th.Property("reconciled_cost_of_revenue", th.NumberType),
    th.Property("reconciled_depreciation", th.NumberType),
    th.Property("research_and_development", th.NumberType),
    th.Property("selling_general_and_administration", th.NumberType),
    th.Property("tax_effect_of_unusual_items", th.NumberType),
    th.Property("tax_provision", th.NumberType),
    th.Property("tax_rate_for_calcs", th.NumberType),
    th.Property("total_expenses", th.NumberType),
    th.Property("total_operating_income_as_reported", th.NumberType),
    th.Property("total_revenue", th.NumberType),
    th.Property('interest_income', th.NumberType),
    th.Property('depreciation_and_amortization_in_income_statement', th.NumberType),
    th.Property('depreciation_amortization_depletion_income_statement', th.NumberType),
    th.Property('interest_income_non_operating', th.NumberType),
    th.Property('write_off', th.NumberType),
    th.Property('other_operating_expenses', th.NumberType),
    th.Property('amortization_of_intangibles_income_statement', th.NumberType),
    th.Property('amortization', th.NumberType),
    th.Property('average_dilution_earnings', th.NumberType),
    th.Property('special_income_charges', th.NumberType),
    th.Property('gain_on_sale_of_security', th.NumberType),
    th.Property('minority_interests', th.NumberType),
    th.Property('general_and_administrative_expense', th.NumberType),
    th.Property('other_gand_a', th.NumberType),
    th.Property('selling_and_marketing_expense', th.NumberType),
    th.Property('total_other_finance_cost', th.NumberType),
    th.Property('total_unusual_items', th.NumberType),
    th.Property('total_unusual_items_excluding_goodwill', th.NumberType),
    th.Property('depreciation_income_statement', th.NumberType),
    th.Property('impairment_of_capital_assets', th.NumberType),
    th.Property('other_special_charges', th.NumberType),
    th.Property('rent_and_landing_fees', th.NumberType),
    th.Property('rent_expense_supplemental', th.NumberType),
    th.Property('net_income_discontinuous_operations', th.NumberType),
    th.Property('excise_taxes', th.NumberType),
    th.Property('insurance_and_claims', th.NumberType),
    th.Property('gain_on_sale_of_business', th.NumberType),
    th.Property('otherunder_preferred_stock_dividend', th.NumberType),
    th.Property('restructuring_and_mergern_acquisition', th.NumberType),
    th.Property('provision_for_doubtful_accounts', th.NumberType),
    th.Property('salaries_and_wages', th.NumberType),
    th.Property('earnings_from_equity_interest', th.NumberType),
    th.Property('gain_on_sale_of_ppe', th.NumberType),
    th.Property('other_taxes', th.NumberType),
    th.Property('preferred_stock_dividends', th.NumberType),
    th.Property('earnings_from_equity_interest_net_of_tax', th.NumberType),
    th.Property('securities_amortization', th.NumberType),
    th.Property('net_income_extraordinary', th.NumberType),
    th.Property('net_income_from_tax_loss_carryforward', th.NumberType),
    th.Property('depletion_income_statement', th.NumberType),
    th.Property('depreciation', th.NumberType),
    th.Property('net_interest_income', th.NumberType)
).to_dict()


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
            th.Property("data", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]}), required=True)
        ).to_dict()

    elif schema_category == 'stock_tickers':
        schema = th.PropertiesList(
            th.Property("ticker", th.StringType, required=True),
            th.Property("google_ticker", th.StringType),
            th.Property("bloomberg_ticker", th.StringType),
            th.Property("numerai_ticker", th.StringType),
            th.Property("yahoo_ticker_old", th.StringType),
            th.Property("yahoo_valid_pts", th.BooleanType),
            th.Property("yahoo_valid_numerai", th.BooleanType)
        ).to_dict()

    elif schema_category == 'futures_tickers':
        schema = th.PropertiesList(
            th.Property("ticker", th.StringType, required=True),
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
            th.Property("ticker", th.StringType, required=True),
            th.Property("name", th.StringType),
            th.Property("bloomberg_ticker", th.StringType),
            th.Property("last_price", th.NumberType),
            th.Property("change", th.NumberType),
            th.Property("pct_change", th.StringType)
        ).to_dict()

    elif schema_category in ('crypto_tickers', 'crypto_tickers_top_250'):
        schema = th.PropertiesList(
            th.Property("ticker", th.StringType, required=True),
            th.Property("name", th.StringType),
            th.Property("price_intraday", th.NumberType),
            th.Property("change", th.NumberType),
            th.Property("pct_change", th.AnyType()),
            th.Property("market_cap", th.AnyType()),
            th.Property("volume_in_currency_since_0_00_utc", th.AnyType()),
            th.Property("volume_in_currency_24_hr", th.AnyType()),
            th.Property("total_volume_all_currencies_24_hr", th.AnyType()),
            th.Property("circulating_supply", th.AnyType())
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
        schema = BALANCE_SHEET_SCHEMA

    elif schema_category == 'get_cash_flow':
        schema = CASH_FLOW_SCHEMA

    elif schema_category == 'get_dividends':
        schema = th.PropertiesList(
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("timestamp_tz_aware", th.StringType),
            th.Property("timezone", th.StringType),
            th.Property("ticker", th.StringType),
            th.Property("dividends", th.NumberType)
        ).to_dict()

    elif schema_category == 'get_calendar':
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
            th.Property("revenue_average", th.NumberType)
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

    elif schema_category == 'get_fast_info':
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
            th.Property("timestamp_tz_aware", th.StringType)
        ).to_dict()

    elif schema_category == 'get_financials':
        schema = FINANCIALS_SCHEMA

    elif schema_category == 'get_history_metadata':
        schema = th.PropertiesList(
            th.Property("timestamp_extracted", th.DateTimeType, required=True),
            th.Property('ticker', th.StringType),
            th.Property('timezone', th.StringType),
            th.Property('currency', th.StringType),
            th.Property('exchange_name', th.StringType),
            th.Property('instrument_type', th.StringType),
            th.Property('first_trade_date', th.NumberType),
            th.Property('regular_market_time', th.NumberType),
            th.Property('gmtoffset', th.NumberType),
            th.Property('exchange_timezone_name', th.StringType),
            th.Property('regular_market_price', th.NumberType),
            th.Property('chart_previous_close', th.NumberType),
            th.Property('previous_close', th.NumberType),
            th.Property('scale', th.NumberType),
            th.Property('price_hint', th.NumberType),
            th.Property('current_trading_period', th.NumberType),
            th.Property('trading_periods', th.NumberType),
            th.Property('data_granularity', th.StringType),
            th.Property('range', th.StringType),
            th.Property('valid_ranges', th.ArrayType(th.StringType)),
            th.Property('current_trading_period_pre_timezone', th.StringType),
            th.Property('current_trading_period_pre_start', th.NumberType),
            th.Property('current_trading_period_pre_end', th.NumberType),
            th.Property('current_trading_period_pre_gmtoffset', th.NumberType),
            th.Property('current_trading_period_regular_timezone', th.StringType),
            th.Property('current_trading_period_regular_start', th.NumberType),
            th.Property('current_trading_period_regular_end', th.NumberType),
            th.Property('current_trading_period_regular_gmtoffset', th.NumberType),
            th.Property('current_trading_period_post_timezone', th.StringType),
            th.Property('current_trading_period_post_start', th.NumberType),
            th.Property('current_trading_period_post_end', th.NumberType),
            th.Property('current_trading_period_post_gmtoffset', th.NumberType),
            th.Property('trading_period_pre_start', th.DateTimeType),
            th.Property('trading_period_pre_end', th.DateTimeType),
            th.Property('trading_period_start', th.DateTimeType),
            th.Property('trading_period_end', th.DateTimeType),
            th.Property('trading_period_post_start', th.DateTimeType),
            th.Property('trading_period_post_end', th.DateTimeType)
        ).to_dict()

    elif schema_category == 'get_income_stmt':
        schema = INCOME_STMT_SCHEMA

    elif schema_category == 'get_insider_purchases':
        schema = th.PropertiesList(
            th.Property("timestamp_extracted", th.DateTimeType, required=True),
            th.Property("ticker", th.StringType),
            th.Property("insider_purchases_last_6m", th.StringType),
            th.Property("shares", th.NumberType),
            th.Property("trans", th.NumberType)
        ).to_dict()

    elif schema_category == 'get_insider_roster_holders':
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
            th.Property("position_direct_date", th.DateTimeType)
        ).to_dict()

    elif schema_category == 'get_insider_transactions':
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
            th.Property("value", th.NumberType)
        ).to_dict()

    elif schema_category == 'get_institutional_holders':
        schema = th.PropertiesList(
            th.Property("date_reported", th.DateTimeType, required=True),
            th.Property("ticker", th.StringType),
            th.Property("holder", th.StringType),
            th.Property("pct_out", th.NumberType),
            th.Property("shares", th.NumberType),
            th.Property("value", th.NumberType)
        ).to_dict()

    elif schema_category == 'get_major_holders':
        schema = th.PropertiesList(
            th.Property("timestamp_extracted", th.DateTimeType, required=True),
            th.Property("ticker", th.StringType),
            th.Property("breakdown", th.StringType),
            th.Property("value", th.NumberType),
        ).to_dict()

    elif schema_category == 'get_mutualfund_holders':
        schema = th.PropertiesList(
            th.Property("date_reported", th.DateTimeType, required=True),
            th.Property("ticker", th.StringType),
            th.Property("holder", th.StringType),
            th.Property("pct_held", th.NumberType),
            th.Property("shares", th.NumberType),
            th.Property("value", th.NumberType)
        ).to_dict()

    elif schema_category == 'get_news':
        # TODO: Low priority, but need to fix nested json parsing --- thumbnail is always returning an empty json
        schema = th.PropertiesList(
            th.Property("timestamp_extracted", th.DateTimeType, required=True),
            th.Property("ticker", th.StringType),
            th.Property("link", th.StringType),
            th.Property("provider_publish_time", th.DateTimeType),
            th.Property("publisher", th.StringType),
            th.Property("related_tickers", th.ArrayType(th.StringType)),
            th.Property("thumbnail", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
            th.Property("title", th.StringType),
            th.Property("type", th.StringType),
            th.Property("uuid", th.StringType)
        ).to_dict()

    elif schema_category == 'get_shares_full':
        schema = th.PropertiesList(
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("timestamp_tz_aware", th.StringType),
            th.Property("timezone", th.StringType),
            th.Property("ticker", th.StringType),
            th.Property("amount", th.NumberType)
        ).to_dict()

    elif schema_category == 'get_splits':
        schema = th.PropertiesList(
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("timestamp_tz_aware", th.StringType),
            th.Property("timezone", th.StringType),
            th.Property("ticker", th.StringType),
            th.Property("stock_splits", th.NumberType)
        ).to_dict()

    elif schema_category == 'get_upgrades_downgrades':
        schema = th.PropertiesList(
            th.Property("ticker", th.StringType),
            th.Property("grade_date", th.DateTimeType),
            th.Property("firm", th.StringType),
            th.Property("to_grade", th.StringType),
            th.Property("from_grade", th.StringType),
            th.Property("action", th.StringType)
        ).to_dict()

    elif schema_category == 'option_chain':
        schema = th.PropertiesList(
            th.Property("last_trade_date", th.DateTimeType, required=True),
            th.Property("last_trade_date_tz_aware", th.StringType),
            th.Property("timezone", th.StringType),
            th.Property("timestamp_extracted", th.DateTimeType),
            th.Property("ticker", th.StringType),
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
            th.Property("metadata", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]}))
        ).to_dict()

    elif schema_category == 'options':
        schema = th.PropertiesList(
            th.Property("timestamp_extracted", th.DateTimeType, required=True),
            th.Property("ticker", th.StringType),
            th.Property("expiration_date", th.DateTimeType)
        ).to_dict()

    elif schema_category == 'quarterly_balance_sheet':
        schema = BALANCE_SHEET_SCHEMA

    elif schema_category == 'quarterly_cash_flow':
        schema = CASH_FLOW_SCHEMA

    elif schema_category == 'quarterly_financials':
        schema = FINANCIALS_SCHEMA

    elif schema_category == 'quarterly_income_stmt':
        schema = INCOME_STMT_SCHEMA

    else:
        raise ValueError(f'Specified schema_category {schema_category} is not supported.')

    return schema