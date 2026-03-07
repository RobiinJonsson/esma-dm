"""
ESMA Reference Data Models

Normalized data models for ESMA FIRDS reference data and FITRS transparency data.
"""
from .base import Instrument, TradingVenueAttributes, TechnicalAttributes, RecordType
from .debt import DebtInstrument
from .equity import EquityInstrument
from .derivative import DerivativeInstrument, OptionAttributes, FutureAttributes
from .swap import SwapInstrument
from .futures import FutureInstrument
from .listed_option import ListedOptionInstrument
from .non_standard import NonStandardDerivativeInstrument
from .forward import ForwardInstrument
from .spot import SpotInstrument
from .strategy import StrategyInstrument
from .collective import CollectiveInvestmentInstrument
from .entitlement import EntitlementInstrument
from .financing import FinancingInstrument
from .referential import ReferentialInstrument
from .other import OtherInstrument
from .mapper import InstrumentMapper
from .transparency import EquityTransparencyRecord, NonEquityTransparencyRecord
from .subtypes import (
    EquitySwap,
    Swaption,
    EquityOption,
    MiniFuture,
    StructuredEquity,
    StructuredDebt,
    CommodityFuture,
    FXForward,
    get_output_model,
    parse_instrument,
)

__all__ = [
    # FIRDS reference data — base
    'Instrument',
    'TradingVenueAttributes',
    'TechnicalAttributes',
    'RecordType',
    # FIRDS reference data — primary asset categories
    'EquityInstrument',           # E*
    'DebtInstrument',             # D*
    'SwapInstrument',             # S*
    'FutureInstrument',           # F*
    'ListedOptionInstrument',     # O*
    'NonStandardDerivativeInstrument',  # H*
    'ForwardInstrument',          # J*
    'SpotInstrument',             # I*
    'StrategyInstrument',         # K*
    'CollectiveInvestmentInstrument',   # C*
    'EntitlementInstrument',      # R*
    'FinancingInstrument',        # L*
    'ReferentialInstrument',      # T*
    'OtherInstrument',            # M*
    # FIRDS reference data — generic derivative (backwards compat)
    'DerivativeInstrument',
    'OptionAttributes',
    'FutureAttributes',
    # Mapper
    'InstrumentMapper',
    # FITRS transparency models
    'EquityTransparencyRecord',
    'NonEquityTransparencyRecord',
    # Specific subtype output models
    'EquitySwap',
    'Swaption',
    'EquityOption',
    'MiniFuture',
    'StructuredEquity',
    'StructuredDebt',
    'CommodityFuture',
    'FXForward',
    'get_output_model',
    'parse_instrument',
]

