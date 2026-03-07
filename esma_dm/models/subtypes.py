"""
Subtype output models for parsing FIRDS data from database.

These classes provide typed access to subtype-specific fields stored in the
attributes JSONB column. They map actual FIRDS field names to readable Python
properties based on CFI code subtypes.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, Optional
from .base import Instrument
from .derivative import DerivativeInstrument
from .equity import EquityInstrument
from .debt import DebtInstrument
from .swap import SwapInstrument
from .futures import FutureInstrument
from .non_standard import NonStandardDerivativeInstrument
from .forward import ForwardInstrument
from .entitlement import EntitlementInstrument


@dataclass
class EquitySwap(SwapInstrument):
    """
    Equity Swap (CFI: SE*) - 1.1M instruments.
    
    Total Return Swaps and equity-linked swaps with interest rate legs.
    Maps 28 FIRDS fields from attributes JSON.
    """
    
    # Parsed from attributes JSON
    underlying_isin: Optional[str] = None
    underlying_index_isin: Optional[str] = None
    underlying_index_name: Optional[str] = None
    underlying_index_term_value: Optional[str] = None
    underlying_index_term_unit: Optional[str] = None
    underlying_basket_isin: Optional[str] = None
    
    # Interest rate leg
    interest_rate_reference_name: Optional[str] = None
    interest_rate_term_value: Optional[str] = None
    interest_rate_term_unit: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EquitySwap':
        """Parse from database row including attributes JSON."""
        attrs = data.pop('attributes', {}) or {}
        
        # Map FIRDS field names to properties
        instance = cls(**data)
        instance.underlying_isin = attrs.get('Undrlying_Sngl_ISIN')
        instance.underlying_index_isin = attrs.get('Undrlying_Sngl_Indx_ISIN')
        instance.underlying_index_name = attrs.get('Undrlying_Sngl_Indx_Nm_RefRate_Nm')
        instance.underlying_index_term_value = attrs.get('Undrlying_Sngl_Indx_Nm_Term_Val')
        instance.underlying_index_term_unit = attrs.get('Undrlying_Sngl_Indx_Nm_Term_Unit')
        instance.underlying_basket_isin = attrs.get('Undrlying_Bskt_ISIN')
        instance.interest_rate_reference_name = attrs.get('Intrst_IntrstRate_RefRate_Nm')
        instance.interest_rate_term_value = attrs.get('Intrst_IntrstRate_Term_Val')
        instance.interest_rate_term_unit = attrs.get('Intrst_IntrstRate_Term_Unit')
        
        return instance


@dataclass  
class Swaption(NonStandardDerivativeInstrument):
    """
    Interest Rate Swaption (CFI: HR*) - 956K instruments.
    
    Options on interest rate swaps. Maps 38 FIRDS fields.
    """
    
    # Option attributes
    option_type: Optional[str] = None
    strike_price_amount: Optional[float] = None
    strike_price_percentage: Optional[float] = None
    strike_price_currency: Optional[str] = None
    strike_price_sign: Optional[str] = None
    exercise_style: Optional[str] = None
    
    # Underlying swap attributes  
    underlying_isin: Optional[str] = None
    underlying_basket_isin: Optional[str] = None
    underlying_basket_lei: Optional[str] = None
    underlying_index_name: Optional[str] = None
    underlying_index_term_value: Optional[str] = None
    underlying_index_term_unit: Optional[str] = None
    
    # Interest rate swap legs
    interest_rate_reference_name: Optional[str] = None
    interest_rate_reference_index: Optional[str] = None
    interest_rate_term_value: Optional[str] = None
    interest_rate_term_unit: Optional[str] = None
    first_leg_interest_rate_fixed: Optional[float] = None
    other_leg_interest_rate_fixed: Optional[float] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Swaption':
        """Parse from database row including attributes JSON."""
        attrs = data.pop('attributes', {}) or {}
        
        instance = cls(**data)
        instance.option_type = attrs.get('OptnTp')
        instance.strike_price_amount = attrs.get('StrkPric_Pric_MntryVal_Amt')
        instance.strike_price_percentage = attrs.get('StrkPric_Pric_Pctg')
        instance.strike_price_currency = attrs.get('StrkPric_NoPric_Ccy')
        instance.strike_price_sign = attrs.get('StrkPric_Pric_MntryVal_Sgn')
        instance.exercise_style = attrs.get('OptnExrcStyle')
        instance.underlying_isin = attrs.get('Undrlying_Sngl_ISIN')
        instance.underlying_basket_isin = attrs.get('Undrlying_Bskt_ISIN')
        instance.underlying_basket_lei = attrs.get('Undrlying_Bskt_LEI')
        instance.underlying_index_name = attrs.get('Undrlying_Sngl_Indx_Nm_RefRate_Nm')
        instance.underlying_index_term_value = attrs.get('Undrlying_Sngl_Indx_Nm_Term_Val')
        instance.underlying_index_term_unit = attrs.get('Undrlying_Sngl_Indx_Nm_Term_Unit')
        instance.interest_rate_reference_name = attrs.get('Intrst_IntrstRate_RefRate_Nm')
        instance.interest_rate_reference_index = attrs.get('Intrst_IntrstRate_RefRate_Indx')
        instance.interest_rate_term_value = attrs.get('Intrst_IntrstRate_Term_Val')
        instance.interest_rate_term_unit = attrs.get('Intrst_IntrstRate_Term_Unit')
        instance.first_leg_interest_rate_fixed = attrs.get('Intrst_FrstLegIntrstRate_Fxd')
        instance.other_leg_interest_rate_fixed = attrs.get('Intrst_OthrLegIntrstRate_Fxd')
        
        return instance


@dataclass
class EquityOption(NonStandardDerivativeInstrument):
    """
    OTC Equity Option (CFI: HE*) - 560K instruments.
    
    Non-standardized equity options. Maps 33 FIRDS fields.
    """
    
    # Option attributes
    option_type: Optional[str] = None
    strike_price_amount: Optional[float] = None
    strike_price_percentage: Optional[float] = None
    strike_price_basis_points: Optional[float] = None
    strike_price_currency: Optional[str] = None
    strike_price_sign: Optional[str] = None
    exercise_style: Optional[str] = None
    
    # Underlying
    underlying_isin: Optional[str] = None
    underlying_basket_isin: Optional[str] = None
    underlying_index_isin: Optional[str] = None
    underlying_index_name: Optional[str] = None
    underlying_index_term_value: Optional[str] = None
    underlying_index_term_unit: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EquityOption':
        """Parse from database row including attributes JSON."""
        attrs = data.pop('attributes', {}) or {}
        
        instance = cls(**data)
        instance.option_type = attrs.get('OptnTp')
        instance.strike_price_amount = attrs.get('StrkPric_Pric_MntryVal_Amt')
        instance.strike_price_percentage = attrs.get('StrkPric_Pric_Pctg')
        instance.strike_price_basis_points = attrs.get('StrkPric_Pric_BsisPts')
        instance.strike_price_currency = attrs.get('StrkPric_NoPric_Ccy')
        instance.strike_price_sign = attrs.get('StrkPric_Pric_MntryVal_Sgn')
        instance.exercise_style = attrs.get('OptnExrcStyle')
        instance.underlying_isin = attrs.get('Undrlying_Sngl_ISIN')
        instance.underlying_basket_isin = attrs.get('Undrlying_Bskt_ISIN')
        instance.underlying_index_isin = attrs.get('Undrlying_Sngl_Indx_ISIN')
        instance.underlying_index_name = attrs.get('Undrlying_Sngl_Indx_Nm_RefRate_Nm')
        instance.underlying_index_term_value = attrs.get('Undrlying_Sngl_Indx_Nm_Term_Val')
        instance.underlying_index_term_unit = attrs.get('Undrlying_Sngl_Indx_Nm_Term_Unit')
        
        return instance


@dataclass
class MiniFuture(EntitlementInstrument):
    """
    Mini-Future Certificate / Leverage Certificate (CFI: RF*) - 4.5M instruments.
    
    Leveraged products tracking underlying assets. Maps 50 FIRDS fields.
    """
    
    # Option-like attributes
    option_type: Optional[str] = None
    strike_price_amount: Optional[float] = None
    strike_price_sign: Optional[str] = None
    strike_price_pending: Optional[str] = None
    exercise_style: Optional[str] = None
    
    # Underlying
    underlying_isin: Optional[str] = None
    underlying_basket_isin: Optional[str] = None
    underlying_index_isin: Optional[str] = None
    underlying_index_name: Optional[str] = None
    underlying_index_term_value: Optional[str] = None
    underlying_index_term_unit: Optional[str] = None
    
    # Commodity attributes (if applicable)
    commodity_metal_precious_base: Optional[str] = None
    commodity_metal_precious_sub: Optional[str] = None
    commodity_metal_precious_additional: Optional[str] = None
    commodity_metal_non_precious_base: Optional[str] = None
    commodity_metal_non_precious_sub: Optional[str] = None
    commodity_metal_non_precious_additional: Optional[str] = None
    commodity_agricultural_soft_base: Optional[str] = None
    commodity_agricultural_soft_sub: Optional[str] = None
    commodity_agricultural_soft_additional: Optional[str] = None
    commodity_agricultural_grain_oilseed_base: Optional[str] = None
    commodity_agricultural_grain_oilseed_sub: Optional[str] = None
    commodity_agricultural_grain_oilseed_additional: Optional[str] = None
    commodity_energy_natural_gas_base: Optional[str] = None
    commodity_energy_natural_gas_sub: Optional[str] = None
    commodity_energy_natural_gas_additional: Optional[str] = None
    commodity_energy_oil_base: Optional[str] = None
    commodity_energy_oil_sub: Optional[str] = None
    commodity_energy_oil_additional: Optional[str] = None
    
    # FX attributes (if applicable)
    fx_type: Optional[str] = None
    fx_other_currency: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MiniFuture':
        """Parse from database row including attributes JSON."""
        attrs = data.pop('attributes', {}) or {}
        
        instance = cls(**data)
        instance.option_type = attrs.get('OptnTp')
        instance.strike_price_amount = attrs.get('StrkPric_Pric_MntryVal_Amt')
        instance.strike_price_sign = attrs.get('StrkPric_Pric_MntryVal_Sgn')
        instance.strike_price_pending = attrs.get('StrkPric_NoPric_Pdg')
        instance.exercise_style = attrs.get('OptnExrcStyle')
        instance.underlying_isin = attrs.get('Undrlying_Sngl_ISIN')
        instance.underlying_basket_isin = attrs.get('Undrlying_Bskt_ISIN')
        instance.underlying_index_isin = attrs.get('Undrlying_Sngl_Indx_ISIN')
        instance.underlying_index_name = attrs.get('Undrlying_Sngl_Indx_Nm_RefRate_Nm')
        instance.underlying_index_term_value = attrs.get('Undrlying_Sngl_Indx_Nm_Term_Val')
        instance.underlying_index_term_unit = attrs.get('Undrlying_Sngl_Indx_Nm_Term_Unit')
        
        # Commodity fields
        instance.commodity_metal_precious_base = attrs.get('Cmmdty_Pdct_Metl_Prcs_BasePdct')
        instance.commodity_metal_precious_sub = attrs.get('Cmmdty_Pdct_Metl_Prcs_SubPdct')
        instance.commodity_metal_precious_additional = attrs.get('Cmmdty_Pdct_Metl_Prcs_AddtlSubPdct')
        instance.commodity_metal_non_precious_base = attrs.get('Cmmdty_Pdct_Metl_NonPrcs_BasePdct')
        instance.commodity_metal_non_precious_sub = attrs.get('Cmmdty_Pdct_Metl_NonPrcs_SubPdct')
        instance.commodity_metal_non_precious_additional = attrs.get('Cmmdty_Pdct_Metl_NonPrcs_AddtlSubPdct')
        instance.commodity_agricultural_soft_base = attrs.get('Cmmdty_Pdct_Agrcltrl_Soft_BasePdct')
        instance.commodity_agricultural_soft_sub = attrs.get('Cmmdty_Pdct_Agrcltrl_Soft_SubPdct')
        instance.commodity_agricultural_soft_additional = attrs.get('Cmmdty_Pdct_Agrcltrl_Soft_AddtlSubPdct')
        instance.commodity_agricultural_grain_oilseed_base = attrs.get('Cmmdty_Pdct_Agrcltrl_GrnOilSeed_BasePdct')
        instance.commodity_agricultural_grain_oilseed_sub = attrs.get('Cmmdty_Pdct_Agrcltrl_GrnOilSeed_SubPdct')
        instance.commodity_agricultural_grain_oilseed_additional = attrs.get('Cmmdty_Pdct_Agrcltrl_GrnOilSeed_AddtlSubPdct')
        instance.commodity_energy_natural_gas_base = attrs.get('Cmmdty_Pdct_Nrgy_NtrlGas_BasePdct')
        instance.commodity_energy_natural_gas_sub = attrs.get('Cmmdty_Pdct_Nrgy_NtrlGas_SubPdct')
        instance.commodity_energy_natural_gas_additional = attrs.get('Cmmdty_Pdct_Nrgy_NtrlGas_AddtlSubPdct')
        instance.commodity_energy_oil_base = attrs.get('Cmmdty_Pdct_Nrgy_Oil_BasePdct')
        instance.commodity_energy_oil_sub = attrs.get('Cmmdty_Pdct_Nrgy_Oil_SubPdct')
        instance.commodity_energy_oil_additional = attrs.get('Cmmdty_Pdct_Nrgy_Oil_AddtlSubPdct')
        
        # FX fields
        instance.fx_type = attrs.get('FX_FxTp')
        instance.fx_other_currency = attrs.get('FX_OthrNtnlCcy')
        
        return instance


@dataclass
class StructuredEquity(EquityInstrument):
    """
    Structured Equity Instrument (CFI: EY*) - 771K instruments.
    
    Participation certificates and structured products. Maps 28 FIRDS fields.
    """
    
    # Underlying
    underlying_isin: Optional[str] = None
    underlying_basket_isin: Optional[str] = None
    underlying_basket_lei: Optional[str] = None
    underlying_index_isin: Optional[str] = None
    underlying_index_name: Optional[str] = None
    
    # Commodity attributes (if applicable)
    commodity_energy_oil_base: Optional[str] = None
    commodity_energy_oil_sub: Optional[str] = None
    commodity_energy_oil_additional: Optional[str] = None
    commodity_metal_precious_base: Optional[str] = None
    commodity_metal_precious_sub: Optional[str] = None
    commodity_metal_precious_additional: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StructuredEquity':
        """Parse from database row including attributes JSON."""
        attrs = data.pop('attributes', {}) or {}
        
        instance = cls(**data)
        instance.underlying_isin = attrs.get('Undrlying_Sngl_ISIN')
        instance.underlying_basket_isin = attrs.get('Undrlying_Bskt_ISIN')
        instance.underlying_basket_lei = attrs.get('Undrlying_Bskt_LEI')
        instance.underlying_index_isin = attrs.get('Undrlying_Sngl_Indx_ISIN')
        instance.underlying_index_name = attrs.get('Undrlying_Sngl_Indx_Nm_RefRate_Nm')
        instance.commodity_energy_oil_base = attrs.get('Cmmdty_Pdct_Nrgy_Oil_BasePdct')
        instance.commodity_energy_oil_sub = attrs.get('Cmmdty_Pdct_Nrgy_Oil_SubPdct')
        instance.commodity_energy_oil_additional = attrs.get('Cmmdty_Pdct_Nrgy_Oil_AddtlSubPdct')
        instance.commodity_metal_precious_base = attrs.get('Cmmdty_Pdct_Metl_Prcs_BasePdct')
        instance.commodity_metal_precious_sub = attrs.get('Cmmdty_Pdct_Metl_Prcs_SubPdct')
        instance.commodity_metal_precious_additional = attrs.get('Cmmdty_Pdct_Metl_Prcs_AddtlSubPdct')
        
        return instance


@dataclass
class StructuredDebt(DebtInstrument):
    """
    Structured Debt without Capital Protection (CFI: DE*) - 940K instruments.
    
    Notes with embedded derivatives. Maps 29 FIRDS fields.
    """
    
    # Debt attributes
    interest_rate_fixed: Optional[float] = None
    interest_rate_floating_term_unit: Optional[str] = None
    interest_rate_floating_term_value: Optional[str] = None
    interest_rate_floating_basis_point_spread: Optional[float] = None
    interest_rate_floating_reference_name: Optional[str] = None
    debt_seniority: Optional[str] = None
    total_issued_nominal_amount: Optional[float] = None
    nominal_value_per_unit: Optional[float] = None
    
    # Underlying
    underlying_isin: Optional[str] = None
    underlying_basket_isin: Optional[str] = None
    underlying_index_isin: Optional[str] = None
    underlying_index_name: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StructuredDebt':
        """Parse from database row including attributes JSON."""
        attrs = data.pop('attributes', {}) or {}
        
        instance = cls(**data)
        instance.interest_rate_fixed = attrs.get('Debt_IntrstRate_Fxd')
        instance.interest_rate_floating_term_unit = attrs.get('Debt_IntrstRate_Fltg_Term_Unit')
        instance.interest_rate_floating_term_value = attrs.get('Debt_IntrstRate_Fltg_Term_Val')
        instance.interest_rate_floating_basis_point_spread = attrs.get('Debt_IntrstRate_Fltg_BsisPtSprd')
        instance.interest_rate_floating_reference_name = attrs.get('Debt_IntrstRate_Fltg_RefRate_Nm')
        instance.debt_seniority = attrs.get('Debt_DebtSnrty')
        instance.total_issued_nominal_amount = attrs.get('Debt_TtlIssdNmnlAmt')
        instance.nominal_value_per_unit = attrs.get('Debt_NmnlValPerUnit')
        instance.underlying_isin = attrs.get('Undrlying_Sngl_ISIN')
        instance.underlying_basket_isin = attrs.get('Undrlying_Bskt_ISIN')
        instance.underlying_index_isin = attrs.get('Undrlying_Sngl_Indx_ISIN')
        instance.underlying_index_name = attrs.get('Undrlying_Sngl_Indx_Nm_RefRate_Nm')
        
        return instance


@dataclass
class CommodityFuture(FutureInstrument):
    """
    Commodity Future (CFI: FC*) - 29K instruments.
    
    Futures on physical commodities. Maps 56 FIRDS fields with extensive
    commodity taxonomy.
    """
    
    # Commodity taxonomy (33 fields)
    commodity_final_price_type: Optional[str] = None
    commodity_transaction_type: Optional[str] = None
    commodity_dairy_base: Optional[str] = None
    commodity_dairy_sub: Optional[str] = None
    commodity_potato_base: Optional[str] = None
    commodity_potato_sub: Optional[str] = None
    commodity_grain_base: Optional[str] = None
    commodity_grain_sub: Optional[str] = None
    commodity_grain_additional: Optional[str] = None
    commodity_seafood_base: Optional[str] = None
    commodity_seafood_sub: Optional[str] = None
    commodity_grain_oilseed_base: Optional[str] = None
    commodity_grain_oilseed_sub: Optional[str] = None
    commodity_grain_oilseed_additional: Optional[str] = None
    commodity_energy_natural_gas_base: Optional[str] = None
    commodity_energy_natural_gas_sub: Optional[str] = None
    commodity_energy_natural_gas_additional: Optional[str] = None
    commodity_energy_electricity_base: Optional[str] = None
    commodity_energy_electricity_sub: Optional[str] = None
    commodity_energy_electricity_additional: Optional[str] = None
    commodity_other_base: Optional[str] = None
    commodity_multi_exotic_base: Optional[str] = None
    commodity_paper_recovered_base: Optional[str] = None
    commodity_paper_recovered_sub: Optional[str] = None
    commodity_paper_pulp_base: Optional[str] = None
    commodity_paper_pulp_sub: Optional[str] = None
    commodity_environmental_emissions_base: Optional[str] = None
    commodity_environmental_emissions_sub: Optional[str] = None
    commodity_environmental_emissions_additional: Optional[str] = None
    commodity_freight_dry_base: Optional[str] = None
    commodity_freight_dry_sub: Optional[str] = None
    commodity_freight_dry_additional: Optional[str] = None
    
    # Underlying
    underlying_isin: Optional[str] = None
    underlying_index_isin: Optional[str] = None
    underlying_index_name: Optional[str] = None
    underlying_index_term_value: Optional[str] = None
    underlying_index_term_unit: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CommodityFuture':
        """Parse from database row including attributes JSON."""
        attrs = data.pop('attributes', {}) or {}
        
        instance = cls(**data)
        instance.commodity_final_price_type = attrs.get('Cmmdty_FnlPricTp')
        instance.commodity_transaction_type = attrs.get('Cmmdty_TxTp')
        instance.commodity_dairy_base = attrs.get('Cmmdty_Pdct_Agrcltrl_Dairy_BasePdct')
        instance.commodity_dairy_sub = attrs.get('Cmmdty_Pdct_Agrcltrl_Dairy_SubPdct')
        instance.commodity_potato_base = attrs.get('Cmmdty_Pdct_Agrcltrl_Ptt_BasePdct')
        instance.commodity_potato_sub = attrs.get('Cmmdty_Pdct_Agrcltrl_Ptt_SubPdct')
        instance.commodity_grain_base = attrs.get('Cmmdty_Pdct_Agrcltrl_Grn_BasePdct')
        instance.commodity_grain_sub = attrs.get('Cmmdty_Pdct_Agrcltrl_Grn_SubPdct')
        instance.commodity_grain_additional = attrs.get('Cmmdty_Pdct_Agrcltrl_Grn_AddtlSubPdct')
        instance.commodity_seafood_base = attrs.get('Cmmdty_Pdct_Agrcltrl_Sfd_BasePdct')
        instance.commodity_seafood_sub = attrs.get('Cmmdty_Pdct_Agrcltrl_Sfd_SubPdct')
        instance.commodity_grain_oilseed_base = attrs.get('Cmmdty_Pdct_Agrcltrl_GrnOilSeed_BasePdct')
        instance.commodity_grain_oilseed_sub = attrs.get('Cmmdty_Pdct_Agrcltrl_GrnOilSeed_SubPdct')
        instance.commodity_grain_oilseed_additional = attrs.get('Cmmdty_Pdct_Agrcltrl_GrnOilSeed_AddtlSubPdct')
        instance.commodity_energy_natural_gas_base = attrs.get('Cmmdty_Pdct_Nrgy_NtrlGas_BasePdct')
        instance.commodity_energy_natural_gas_sub = attrs.get('Cmmdty_Pdct_Nrgy_NtrlGas_SubPdct')
        instance.commodity_energy_natural_gas_additional = attrs.get('Cmmdty_Pdct_Nrgy_NtrlGas_AddtlSubPdct')
        instance.commodity_energy_electricity_base = attrs.get('Cmmdty_Pdct_Nrgy_Elctrcty_BasePdct')
        instance.commodity_energy_electricity_sub = attrs.get('Cmmdty_Pdct_Nrgy_Elctrcty_SubPdct')
        instance.commodity_energy_electricity_additional = attrs.get('Cmmdty_Pdct_Nrgy_Elctrcty_AddtlSubPdct')
        instance.commodity_other_base = attrs.get('Cmmdty_Pdct_Othr_BasePdct')
        instance.commodity_multi_exotic_base = attrs.get('Cmmdty_Pdct_MultiCmmdtyExtc_BasePdct')
        instance.commodity_paper_recovered_base = attrs.get('Cmmdty_Pdct_Ppr_RcvrdPpr_BasePdct')
        instance.commodity_paper_recovered_sub = attrs.get('Cmmdty_Pdct_Ppr_RcvrdPpr_SubPdct')
        instance.commodity_paper_pulp_base = attrs.get('Cmmdty_Pdct_Ppr_Pulp_BasePdct')
        instance.commodity_paper_pulp_sub = attrs.get('Cmmdty_Pdct_Ppr_Pulp_SubPdct')
        instance.commodity_environmental_emissions_base = attrs.get('Cmmdty_Pdct_Envttl_Emssns_BasePdct')
        instance.commodity_environmental_emissions_sub = attrs.get('Cmmdty_Pdct_Envttl_Emssns_SubPdct')
        instance.commodity_environmental_emissions_additional = attrs.get('Cmmdty_Pdct_Envttl_Emssns_AddtlSubPdct')
        instance.commodity_freight_dry_base = attrs.get('Cmmdty_Pdct_Frght_Dry_BasePdct')
        instance.commodity_freight_dry_sub = attrs.get('Cmmdty_Pdct_Frght_Dry_SubPdct')
        instance.commodity_freight_dry_additional = attrs.get('Cmmdty_Pdct_Frght_Dry_AddtlSubPdct')
        instance.underlying_isin = attrs.get('Undrlying_Sngl_ISIN')
        instance.underlying_index_isin = attrs.get('Undrlying_Sngl_Indx_ISIN')
        instance.underlying_index_name = attrs.get('Undrlying_Sngl_Indx_Nm_RefRate_Nm')
        instance.underlying_index_term_value = attrs.get('Undrlying_Sngl_Indx_Nm_Term_Val')
        instance.underlying_index_term_unit = attrs.get('Undrlying_Sngl_Indx_Nm_Term_Unit')
        
        return instance


@dataclass
class FXForward(ForwardInstrument):
    """
    Foreign Exchange Forward (CFI: JF*) - 213K instruments.
    
    OTC FX forward contracts. Maps 22 FIRDS fields.
    """
    
    # FX attributes
    fx_type: Optional[str] = None
    other_notional_currency: Optional[str] = None
    
    # Underlying (if applicable)
    underlying_isin: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FXForward':
        """Parse from database row including attributes JSON."""
        attrs = data.pop('attributes', {}) or {}
        
        instance = cls(**data)
        instance.fx_type = attrs.get('FX_FxTp')
        instance.other_notional_currency = attrs.get('FX_OthrNtnlCcy')
        instance.underlying_isin = attrs.get('Undrlying_Sngl_ISIN')
        
        return instance


# Mapping CFI codes to output model classes
SUBTYPE_MODELS = {
    'SE': EquitySwap,
    'HR': Swaption,
    'HE': EquityOption,
    'RF': MiniFuture,
    'EY': StructuredEquity,
    'DE': StructuredDebt,
    'FC': CommodityFuture,
    'JF': FXForward,
}


def get_output_model(cfi_code: str):
    """
    Get the appropriate output model class for a CFI code.
    
    Args:
        cfi_code: 6-character CFI code
        
    Returns:
        Output model class if available, None otherwise
    """
    if not cfi_code or len(cfi_code) < 2:
        return None
    
    subtype = cfi_code[:2].upper()
    return SUBTYPE_MODELS.get(subtype)


def parse_instrument(row: Dict[str, Any]):
    """
    Parse database row into appropriate subtype output model.
    
    Args:
        row: Dictionary with instrument data including CFI code and attributes JSON
        
    Returns:
        Instance of appropriate subtype class with parsed attributes
    """
    cfi_code = row.get('cfi_code', '')
    model_class = get_output_model(cfi_code)
    
    if model_class:
        return model_class.from_dict(row)
    
    # Fall back to base model
    from .mapper import CFIMapper
    base_class = CFIMapper.get_model_class(cfi_code)
    return base_class(**{k: v for k, v in row.items() if k != 'attributes'})
