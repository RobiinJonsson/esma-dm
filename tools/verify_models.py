"""Verify model changes after refactoring."""
import sys
sys.path.insert(0, 'c:/Users/robin/Projects/esma-dm')

from esma_dm.models import (
    Instrument, DebtInstrument, EquityInstrument, DerivativeInstrument,
    OptionAttributes, FutureAttributes, InstrumentMapper,
    EquityTransparencyRecord, NonEquityTransparencyRecord,
)
print('All imports OK')

# Verify OptionAttributes new fields
o = OptionAttributes(
    option_type='CALL',
    option_style='EURO',
    strike_price_amount=100.0,
    strike_price_sign='PLUS',
)
print(f'OptionAttributes OK: {o.option_type} {o.option_style} strike={o.strike_price_amount}')

# Verify DebtInstrument: no fixed_interest_rate, has fixed_rate
debt_fields = list(DebtInstrument.__dataclass_fields__.keys())
assert 'fixed_interest_rate' not in debt_fields, 'fixed_interest_rate still present!'
assert 'fixed_rate' in debt_fields, 'fixed_rate missing!'
print('DebtInstrument fields OK')

# Verify DerivativeInstrument new fields
deriv_fields = list(DerivativeInstrument.__dataclass_fields__.keys())
for expected in [
    'underlying_isin', 'underlying_lei', 'underlying_basket_isin',
    'underlying_basket_lei', 'underlying_index_isin', 'underlying_index_name',
    'underlying_index_reference_index', 'underlying_index_term_unit',
    'underlying_index_term_value', 'delivery_type', 'fx_type',
    'other_notional_currency', 'interest_rate_reference_name',
    'interest_rate_reference_index', 'first_leg_rate_fixed',
    'other_leg_rate_fixed', 'base_product', 'sub_product', 'further_sub_product',
]:
    assert expected in deriv_fields, f'{expected} missing from DerivativeInstrument!'
print('DerivativeInstrument new fields verified OK')

# Verify DEBT_FIELD_MAP is merged correctly (no duplicate)
assert 'DebtInstrmAttrbts_DebtSnrty' in InstrumentMapper.DEBT_FIELD_MAP, 'DebtSnrty missing'
assert InstrumentMapper.DEBT_FIELD_MAP['DebtInstrmAttrbts_IntrstRate_Fxd'] == 'fixed_rate'
assert 'DebtInstrmAttrbts_IntrstRate_Fltg_BsisPtSprd' in InstrumentMapper.DEBT_FIELD_MAP
assert 'DebtInstrmAttrbts_SnrtyTp' not in InstrumentMapper.DEBT_FIELD_MAP, 'wrong field name still present!'
print('DEBT_FIELD_MAP verified OK')

# Verify EQUITY_FIELD_MAP cleaned up
assert 'FinInstrmGnlAttrbts_DvddPmtFrqcy' not in InstrumentMapper.EQUITY_FIELD_MAP, 'phantom field still there!'
assert 'DerivInstrmAttrbts_UndrlygInstrm_Sngl_ISIN' in InstrumentMapper.EQUITY_FIELD_MAP
print('EQUITY_FIELD_MAP verified OK')

# Verify DERIVATIVE_FIELD_MAP correct column names
assert 'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_FX_FxTp' in InstrumentMapper.DERIVATIVE_FIELD_MAP
assert 'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Intrst_IntrstRate_RefRate_Nm' in InstrumentMapper.DERIVATIVE_FIELD_MAP
assert 'DerivInstrmAttrbts_StrkPric_Pric_MntryVal_Amt' in InstrumentMapper.DERIVATIVE_FIELD_MAP
assert 'DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_ISIN' in InstrumentMapper.DERIVATIVE_FIELD_MAP
assert 'DerivInstrmAttrbts_AsstClssSpcfc_Cmmdty_Pdct_Nrgy_Oil_BasePdct' not in InstrumentMapper.DERIVATIVE_FIELD_MAP, 'old wrong key still there!'
print('DERIVATIVE_FIELD_MAP verified OK')

# Verify transparency models
et = EquityTransparencyRecord(isin='GB00B1YW4409', liquid_market=True, methodology='SINT')
ne = NonEquityTransparencyRecord(isin='GB00B1YW4409', liquid_market=False, criterion_name_1='NETT')
print(f'EquityTransparencyRecord: isin={et.isin}, methodology={et.methodology}')
print(f'NonEquityTransparencyRecord: isin={ne.isin}, criterion={ne.criterion_name_1}')

print('\nAll checks passed.')
