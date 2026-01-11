"""
ESMA Reference Data Models

Normalized data models for ESMA FIRDS reference data.
"""
from .base import Instrument, TradingVenueAttributes, TechnicalAttributes, RecordType
from .debt import DebtInstrument
from .equity import EquityInstrument
from .derivative import DerivativeInstrument, OptionAttributes, FutureAttributes
from .mapper import InstrumentMapper
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
    'Instrument',
    'TradingVenueAttributes',
    'TechnicalAttributes',
    'RecordType',
    'DebtInstrument',
    'EquityInstrument',
    'DerivativeInstrument',
    'OptionAttributes',
    'FutureAttributes',
    'InstrumentMapper',
    # Subtype output models
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
